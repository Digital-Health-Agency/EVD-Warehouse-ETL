{{ config(
    materialized = 'table'
) }}

with source as (

    select
        nullif(trim(subject_identifier), '') as subject_identifier,
        nullif(trim(identifier_number), '') as identifier_number,
        nullif(trim(order_identifier), '') as order_identifier,
        nullif(trim(specimen_id), '') as specimen_id,
        nullif(trim(specimen_type), '') as specimen_type,

        collection_date,
        result_datetime,

        nullif(trim(facility_mfl::text), '') as facility_mfl,

        nullif(trim(loinc_code), '') as loinc_code,
        nullif(trim(test_name), '') as test_name,
        nullif(trim(code_text), '') as code_text,
        nullif(trim(component_code), '') as component_code,
        nullif(trim(unit), '') as unit,

        nullif(trim(result), '') as result,
        nullif(trim(value_code), '') as value_code,

        reference_range_low,
        reference_range_high,

        'lims'::text as source_system,
        _raw_hash::text as source_record_id,

        _batch_id::text as batch_id,
        _source_file::text as source_file,
        _raw_hash::text as raw_hash

    from {{ ref('slv_lims_results') }}

),

deduplicated_source as (

    select
        subject_identifier,
        identifier_number,
        order_identifier,
        specimen_id,
        specimen_type,

        collection_date,
        result_datetime,
        facility_mfl,

        loinc_code,
        test_name,
        code_text,
        component_code,
        unit,

        result,
        value_code,

        reference_range_low,
        reference_range_high,

        source_system,
        source_record_id,

        batch_id,
        source_file,
        raw_hash

    from (

        select
            *,
            row_number() over (
                partition by
                    source_system,
                    source_record_id
                order by
                    result_datetime desc nulls last,
                    batch_id desc nulls last
            ) as row_number

        from source

    ) ranked

    where row_number = 1

),

facility_by_mfl as (

    select
        nullif(trim(mfl_code::text), '') as mfl_code,
        min(facility_key) as facility_key

    from {{ ref('dim_facilitylist') }}

    where nullif(trim(mfl_code::text), '') is not null

    group by
        nullif(trim(mfl_code::text), '')

),

final as (

    select
        {{
            dbt_utils.generate_surrogate_key([
                's.source_system',
                's.source_record_id'
            ])
        }} as lab_result_key,

        f.facility_key,

        collection_date.date_key as collection_date_key,
        result_date.date_key as result_date_key,

        s.source_system,
        s.source_record_id,

        s.subject_identifier,
        s.identifier_number,
        s.order_identifier,
        s.specimen_id,
        s.specimen_type,

        s.loinc_code,
        s.test_name,
        s.code_text,
        s.component_code,
        s.unit,

        s.result,
        s.value_code,

        case
            when lower(
                trim(
                    coalesce(
                        nullif(s.result, ''),
                        nullif(s.value_code, '')
                    )
                )
            ) in (
                'positive',
                'detected',
                'reactive',
                'present'
            )
            then 'Positive'

            when lower(
                trim(
                    coalesce(
                        nullif(s.result, ''),
                        nullif(s.value_code, '')
                    )
                )
            ) in (
                'negative',
                'not detected',
                'non-reactive',
                'non reactive',
                'absent'
            )
            then 'Negative'

            when lower(
                trim(
                    coalesce(
                        nullif(s.result, ''),
                        nullif(s.value_code, '')
                    )
                )
            ) in (
                'indeterminate',
                'inconclusive',
                'invalid',
                'equivocal'
            )
            then 'Inconclusive'

            when coalesce(
                nullif(s.result, ''),
                nullif(s.value_code, '')
            ) is null
            then 'Unknown'

            else 'Other'
        end as result_category,

        s.reference_range_low,
        s.reference_range_high,

        s.result_datetime,

        s.batch_id,
        s.source_file,
        s.raw_hash

    from deduplicated_source s

    left join facility_by_mfl f
        on s.facility_mfl = f.mfl_code

    left join {{ ref('dim_date') }} collection_date
        on s.collection_date = collection_date.full_date

    left join {{ ref('dim_date') }} result_date
        on s.result_datetime::date = result_date.full_date

)

select
    lab_result_key,

    facility_key,

    collection_date_key,
    result_date_key,

    source_system,
    source_record_id,

    subject_identifier,
    identifier_number,
    order_identifier,
    specimen_id,
    specimen_type,

    loinc_code,
    test_name,
    code_text,
    component_code,
    unit,

    result,
    value_code,
    result_category,

    reference_range_low,
    reference_range_high,

    result_datetime,

    1 as test_count,

    case
        when result_category = 'Positive' then 1
        else 0
    end as positive_test_count,

    case
        when result_category = 'Negative' then 1
        else 0
    end as negative_test_count,

    case
        when result_category = 'Inconclusive' then 1
        else 0
    end as inconclusive_test_count,

    case
        when result_category = 'Unknown' then 1
        else 0
    end as unknown_test_count,

    case
        when result_category = 'Other' then 1
        else 0
    end as other_test_count,

    batch_id,
    source_file,
    raw_hash

from final