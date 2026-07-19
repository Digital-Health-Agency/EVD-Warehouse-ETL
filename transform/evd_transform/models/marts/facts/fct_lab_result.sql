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
        'LIMS'::text as source_system,

        id as source_row_id,

        /*
         * Retain the source business identifier where available.
         * The silver model already ensures one latest record per
         * source identifier.
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
         * requested the sample/test.
         */
        nullif(trim(performer_mfl), '')
            as requesting_facility_mfl,

        nullif(trim(testing_lab_code), '')
            as testing_laboratory_code,

        nullif(trim(testing_lab_name), '')
            as testing_laboratory_name,

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

        lower(
            trim(
                coalesce(
                    nullif(result_value, ''),
                    ''
                )
            )
        ) as result_value_normalized,

        lower(
            nullif(
                trim(requesting_facility_mfl),
                ''
            )
        ) as requesting_facility_mfl_normalized,

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
        ) as testing_laboratory_name_normalized

    from lims_lab_results

),

categorized_results as (

    select
        *,

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

    where nullif(trim(mfl_code::text), '') is not null

    group by
        lower(
            nullif(
                trim(mfl_code::text),
                ''
            )
        )

),

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

    where nullif(trim(laboratory_code::text), '') is not null

    group by
        lower(
            nullif(
                trim(laboratory_code::text),
                ''
            )
        )

),

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

    where nullif(trim(laboratory_name), '') is not null

    group by
        lower(
            nullif(
                trim(laboratory_name),
                ''
            )
        )

),

/*
Future sources must return the same canonical columns.

other_lab_results as (

    select
        ...
)
*/

unioned_lab_results as (

    select *
    from categorized_results

    /*
    union all

    select *
    from other_lab_results
    */

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'r.source_system',
            'r.source_record_id'
        ]) }} as lab_result_key,

        /*
         * Facility that requested the test.
         */
        rf.facility_key
            as requesting_facility_key,

        /*
         * Laboratory that performed the test.
         * Code matching is preferred, followed by name.
         */
        coalesce(
            laboratory_code.laboratory_key,
            laboratory_name.laboratory_key
        ) as testing_laboratory_key,

        coalesce(
            collection_date.date_key,
            -1
        ) as collection_date_key,

        coalesce(
            result_date.date_key,
            -1
        ) as result_date_key,

        r.source_system,
        r.source_row_id,
        r.source_record_id,

        r.subject_identifier,
        r.identifier,
        r.case_identifier,
        r.order_identifier,
        r.specimen_identifier,
        r.specimen_type,

        r.collection_date,
        r.result_datetime,

        r.requesting_facility_mfl,

        r.testing_laboratory_code,
        r.testing_laboratory_name,

        r.test_code,
        r.test_name,
        r.test_code_text,
        r.component_code,

        r.source_result,
        r.source_value_code,
        r.result_value,
        r.result_category,
        r.result_unit,

        r.reference_range_low,
        r.reference_range_high,

        1::integer as test_count,

        r.ingested_at,
        r.batch_id,
        r.source_file

    from unioned_lab_results r

    left join requesting_facilities rf
        on r.requesting_facility_mfl_normalized
         = rf.mfl_code_normalized

    left join laboratories_by_code laboratory_code
        on r.testing_laboratory_code_normalized
         = laboratory_code.laboratory_code_normalized

    left join laboratories_by_name laboratory_name
        on r.testing_laboratory_name_normalized
         = laboratory_name.laboratory_name_normalized

    left join {{ ref('dim_date') }} collection_date
        on r.collection_date = collection_date.full_date

    left join {{ ref('dim_date') }} result_date
        on cast(r.result_datetime as date) = result_date.full_date

)

select *
from final