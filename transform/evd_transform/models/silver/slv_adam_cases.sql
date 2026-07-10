
with src as (

    select *
    from {{ source('bronze', 'adam_cases_raw') }}

),

cleaned as (

    select
        nullif(id_field, '') as system_id,
        nullif(name, '') as names,
        upper(nullif(sex, '')) as sex,

        nullif(date_of_birth, '')::date as date_of_birth,
        nullif(nationality, '') as nationality,
        nullif(identifier, '') as identifier_number,

        nullif(type, '') as record_type,
        nullif(initial_classification, '') as case_classification,
        nullif(outcome, '') as outcome,

        case
            when lower(samples_collected) in ('yes', 'true', '1') then true
            when lower(samples_collected) in ('no', 'false', '0') then false
            else null
        end as samples_collected,

        nullif(specimen_id, '') as specimen_id,
        nullif(final_laboratory_results, '') as final_laboratory_results,

        nullif(reporting_county, '') as reporting_county,
        nullif(reporting_subcounty, '') as reporting_subcounty,
        nullif(health_facility, '') as health_facility,

        nullif(date_of_investigation, '')::date as investigation_date,
        nullif(created_timestamp, '')::timestamp as created_at,

          case
        when trim(latitude) ~ '^-?[0-9]+(\.[0-9]+)?$'
        then trim(latitude)::numeric
        else null
        end as latitude,

        _batch_id,
        _source_file,
        _raw_hash

    from src

)

select *
from cleaned