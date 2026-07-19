{{ config(
    materialized = 'table',
    schema = 'marts'
) }}

with lims_source as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,

        subject_identifier,
        identifier,
        order_identifier,
        specimen_identifier,
        specimen_type,

        collect_date,
        effective_date_time,

        /*
         * Facility that requested the test/sample.
         */
        performer_mfl,

        /*
         * Laboratory that performed the test.
         */
        testing_lab_code,
        testing_lab_name,

        code,
        test_name,
        code_text,
        component_code,
        unit,

        conclusion,
        value_code,

        reference_range_low,
        reference_range_high,

        case_identifier,
        id_field

    from {{ ref('slv_lims_results') }}

),

lims_lab_results as (

    select
        'LIMS'::text
            as source_system,

        id
            as source_row_id,

        /*
         * Source business identifier.
         */
        coalesce(
            nullif(trim(id_field), ''),
            nullif(trim(identifier), ''),
            nullif(trim(specimen_identifier), ''),
            nullif(trim(order_identifier), ''),
            cast(id as text)
        ) as source_record_id,

        nullif(trim(subject_identifier), '')
            as subject_identifier,

        nullif(trim(identifier), '')
            as identifier,

        nullif(trim(case_identifier), '')
            as case_identifier,

        nullif(trim(order_identifier), '')
            as order_identifier,

        nullif(trim(specimen_identifier), '')
            as specimen_identifier,

        nullif(trim(specimen_type), '')
            as specimen_type,

        collect_date
            as collection_date,

        effective_date_time
            as result_datetime,

        /*
         * performer_mfl is the health facility that
         * requested the test or submitted the specimen.
         */
        nullif(trim(performer_mfl), '')
            as requesting_facility_mfl,

        nullif(trim(testing_lab_code), '')
            as testing_laboratory_code,

        nullif(trim(testing_lab_name), '')
            as testing_laboratory_name,

        /*
         * Source test attributes.
         */
        nullif(trim(code), '')
            as test_code,

        nullif(trim(test_name), '')
            as test_name,

        nullif(trim(code_text), '')
            as test_code_text,

        nullif(trim(component_code), '')
            as component_code,

        nullif(trim(unit), '')
            as result_unit,

        /*
         * Original result values retained for reconciliation.
         */
        nullif(trim(conclusion), '')
            as source_result,

        nullif(trim(value_code), '')
            as source_value_code,

        coalesce(
            nullif(trim(conclusion), ''),
            nullif(trim(value_code), '')
        ) as result_value,

        reference_range_low,
        reference_range_high,

        _ingested_at
            as ingested_at,

        _batch_id
            as batch_id,

        _source_file
            as source_file

    from lims_source

),

standardized_results as (

    select
        *,

        /*
         * Result normalization.
         */
        lower(
            trim(
                coalesce(
                    nullif(result_value, ''),
                    ''
                )
            )
        ) as result_value_normalized,

        /*
         * Requesting-facility normalization.
         */
        lower(
            nullif(
                trim(requesting_facility_mfl),
                ''
            )
        ) as requesting_facility_mfl_normalized,

        /*
         * Testing-laboratory normalization.
         */
        lower(
            nullif(
                trim(testing_laboratory_code),
                ''
            )
        ) as testing_laboratory_code_normalized,

        lower(
            nullif(
                trim(testing_laboratory_name),
                ''
            )
        ) as testing_laboratory_name_normalized,

        /*
         * Laboratory-test normalization.
         */
        lower(
            nullif(
                trim(test_code),
                ''
            )
        ) as test_code_normalized,

        lower(
            nullif(
                trim(test_name),
                ''
            )
        ) as test_name_normalized,

        lower(
            nullif(
                trim(test_code_text),
                ''
            )
        ) as test_code_text_normalized,

        lower(
            nullif(
                trim(component_code),
                ''
            )
        ) as component_code_normalized

    from lims_lab_results

),

categorized_results as (

    select
        *,

        /*
         * Canonical result category.
         */
        case
            when result_value_normalized in (
                'positive',
                'detected',
                'reactive',
                'present'
            )
                then 'POSITIVE'

            when result_value_normalized in (
                'negative',
                'not detected',
                'non-reactive',
                'non reactive',
                'absent'
            )
                then 'NEGATIVE'

            when result_value_normalized in (
                'indeterminate',
                'inconclusive',
                'invalid',
                'equivocal'
            )
                then 'INCONCLUSIVE'

            when nullif(trim(result_value), '') is null
                then 'UNKNOWN'

            else 'OTHER'
        end as result_category

    from standardized_results

),

/*
 * Requesting-facility lookup.
 */
requesting_facilities as (

    select
        lower(
            nullif(
                trim(mfl_code::text),
                ''
            )
        ) as mfl_code_normalized,

        min(facility_key)
            as facility_key

    from {{ ref('dim_facilitylist') }}

    where nullif(
        trim(mfl_code::text),
        ''
    ) is not null

    group by
        lower(
            nullif(
                trim(mfl_code::text),
                ''
            )
        )

),

/*
 * Testing-laboratory lookup by code.
 */
laboratories_by_code as (

    select
        lower(
            nullif(
                trim(laboratory_code::text),
                ''
            )
        ) as laboratory_code_normalized,

        min(laboratory_key)
            as laboratory_key

    from {{ ref('dim_laboratory') }}

    where nullif(
        trim(laboratory_code::text),
        ''
    ) is not null

    group by
        lower(
            nullif(
                trim(laboratory_code::text),
                ''
            )
        )

),

/*
 * Testing-laboratory fallback lookup by name.
 */
laboratories_by_name as (

    select
        lower(
            nullif(
                trim(laboratory_name),
                ''
            )
        ) as laboratory_name_normalized,

        min(laboratory_key)
            as laboratory_key

    from {{ ref('dim_laboratory') }}

    where nullif(
        trim(laboratory_name),
        ''
    ) is not null

    group by
        lower(
            nullif(
                trim(laboratory_name),
                ''
            )
        )

),

/*
 * Most specific laboratory-test lookup:
 * test code plus component code.
 *
 * dim_labtest uses labtest_key as its primary key.
 */
lab_tests_by_code_component as (

    select
        lower(
            nullif(
                trim(test_code),
                ''
            )
        ) as test_code_normalized,

        lower(
            nullif(
                trim(component_code),
                ''
            )
        ) as component_code_normalized,

        min(labtest_key)
            as lab_test_key

    from {{ ref('dim_labtest') }}

    where nullif(trim(test_code), '') is not null
      and nullif(trim(component_code), '') is not null

    group by
        lower(
            nullif(
                trim(test_code),
                ''
            )
        ),

        lower(
            nullif(
                trim(component_code),
                ''
            )
        )

),

/*
 * Laboratory-test lookup by test code.
 */
lab_tests_by_code as (

    select
        lower(
            nullif(
                trim(test_code),
                ''
            )
        ) as test_code_normalized,

        min(labtest_key)
            as lab_test_key

    from {{ ref('dim_labtest') }}

    where nullif(trim(test_code), '') is not null

    group by
        lower(
            nullif(
                trim(test_code),
                ''
            )
        )

),

/*
 * Laboratory-test fallback lookup by test name.
 */
lab_tests_by_name as (

    select
        lower(
            nullif(
                trim(test_name),
                ''
            )
        ) as test_name_normalized,

        min(labtest_key)
            as lab_test_key

    from {{ ref('dim_labtest') }}

    where nullif(trim(test_name), '') is not null

    group by
        lower(
            nullif(
                trim(test_name),
                ''
            )
        )

),

/*
 * Final laboratory-test fallback using code_text.
 *
 * dim_labtest exposes this column as code_text,
 * not test_code_text.
 */
lab_tests_by_code_text as (

    select
        lower(
            nullif(
                trim(code_text),
                ''
            )
        ) as test_code_text_normalized,

        min(labtest_key)
            as lab_test_key

    from {{ ref('dim_labtest') }}

    where nullif(trim(code_text), '') is not null

    group by
        lower(
            nullif(
                trim(code_text),
                ''
            )
        )

),

/*
 * Future laboratory-result sources must return
 * the same canonical columns.
 */
unioned_lab_results as (

    select *
    from categorized_results

),

resolved_lab_results as (

    select
        r.*,

        /*
         * Resolve the canonical laboratory test.
         *
         * Priority:
         * 1. Test code and component code
         * 2. Test code
         * 3. Test name
         * 4. Code description
         */
        coalesce(
            test_code_component.lab_test_key,
            test_code.lab_test_key,
            test_name.lab_test_key,
            test_code_text.lab_test_key
        ) as resolved_lab_test_key,

        requesting_facility.facility_key
            as resolved_requesting_facility_key,

        /*
         * Prefer laboratory-code matching,
         * then fall back to laboratory name.
         */
        coalesce(
            laboratory_code.laboratory_key,
            laboratory_name.laboratory_key
        ) as resolved_testing_laboratory_key

    from unioned_lab_results r

    left join requesting_facilities requesting_facility
        on r.requesting_facility_mfl_normalized
         = requesting_facility.mfl_code_normalized

    left join laboratories_by_code laboratory_code
        on r.testing_laboratory_code_normalized
         = laboratory_code.laboratory_code_normalized

    left join laboratories_by_name laboratory_name
        on r.testing_laboratory_name_normalized
         = laboratory_name.laboratory_name_normalized

    left join lab_tests_by_code_component test_code_component
        on r.test_code_normalized
         = test_code_component.test_code_normalized

       and r.component_code_normalized
         = test_code_component.component_code_normalized

    left join lab_tests_by_code test_code
        on r.test_code_normalized
         = test_code.test_code_normalized

    left join lab_tests_by_name test_name
        on r.test_name_normalized
         = test_name.test_name_normalized

    left join lab_tests_by_code_text test_code_text
        on r.test_code_text_normalized
         = test_code_text.test_code_text_normalized

),

final as (

    select
        {{
            dbt_utils.generate_surrogate_key([
                'r.source_system',
                'r.source_record_id'
            ])
        }} as lab_result_key,

        /*
         * Dimension keys.
         */
        r.resolved_lab_test_key
            as lab_test_key,

        r.resolved_requesting_facility_key
            as requesting_facility_key,

        r.resolved_testing_laboratory_key
            as testing_laboratory_key,

        coalesce(
            collection_date.date_key,
            -1
        ) as collection_date_key,

        coalesce(
            result_date.date_key,
            -1
        ) as result_date_key,

        /*
         * Source metadata.
         */
        r.source_system,
        r.source_row_id,
        r.source_record_id,

        /*
         * Subject and case linkage.
         */
        r.subject_identifier,
        r.identifier,
        r.case_identifier,

        /*
         * Order and specimen attributes.
         */
        r.order_identifier,
        r.specimen_identifier,
        r.specimen_type,

        /*
         * Laboratory event dates.
         */
        r.collection_date,
        r.result_datetime,

        /*
         * Requesting facility.
         */
        r.requesting_facility_mfl,

        /*
         * Testing laboratory.
         */
        r.testing_laboratory_code,
        r.testing_laboratory_name,

        /*
         * Source test attributes.
         */
        r.test_code,
        r.test_name,
        r.test_code_text,
        r.component_code,

        /*
         * Laboratory result.
         */
        r.source_result,
        r.source_value_code,
        r.result_value,
        r.result_category,
        r.result_unit,

        r.reference_range_low,
        r.reference_range_high,

        /*
         * Additive fact measure.
         */
        1::integer
            as test_count,

        /*
         * Dimension-resolution flags.
         */
        case
            when r.resolved_lab_test_key is not null
                then 1
            else 0
        end::integer as lab_test_matched_flag,

        case
            when r.resolved_requesting_facility_key is not null
                then 1
            else 0
        end::integer as requesting_facility_matched_flag,

        case
            when r.resolved_testing_laboratory_key is not null
                then 1
            else 0
        end::integer as testing_laboratory_matched_flag,

        /*
         * Audit metadata.
         */
        r.ingested_at,
        r.batch_id,
        r.source_file

    from resolved_lab_results r

    left join {{ ref('dim_date') }} collection_date
        on r.collection_date
         = collection_date.full_date

    left join {{ ref('dim_date') }} result_date
        on cast(r.result_datetime as date)
         = result_date.full_date

)

select *
from final