

with src as (

    select *
    from {{ source('bronze', 'lims_raw') }}
    WHERE component_code = '86518-8'
),

cleaned as (

    select
        nullif(subject_identifier, '') as subject_identifier,
        nullif(identifier, '') as identifier_number,
        nullif(order_identifier, '') as order_identifier,
        nullif(specimen_identifier, '') as specimen_id,
        nullif(specimen_type, '') as specimen_type,

        nullif(collect_date, '')::date as collection_date,
        nullif(effective_date_time, '')::timestamp as result_datetime,

        nullif(performer_mfl, '') as facility_mfl,

        nullif(code, '') as loinc_code,
        nullif(test_name, '') as test_name,
        nullif(code_text, '') as code_text,
        nullif(component_code, '') as component_code,
        nullif(unit, '') as unit,

        nullif(conclusion, '') as result,
        nullif(value_code, '') as value_code,

        nullif(reference_range_low, '') as reference_range_low,
        nullif(reference_range_high, '') as reference_range_high,

        _batch_id,
        _source_file,
        _raw_hash

    from src

)

select *
from cleaned