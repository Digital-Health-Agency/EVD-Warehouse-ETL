with src as (

    select *
    from {{ source('bronze', 'adam_travellers_raw') }}

),

cleaned as (

    select
        nullif(id_field, '') as system_id,
        nullif(name, '') as names,
        upper(nullif(sex, '')) as sex,

        case
    when trim(date_of_birth) ~ '^[1-9][0-9]{3}-[0-9]{2}-[0-9]{2}$'
    then date_of_birth::date
    else null
end as date_of_birth,
        nullif(nationality, '') as nationality,
        nullif(identifier, '') as identifier_number,

        nullif(classification, '') as suspected_classification,

        case
            when lower(screened) in ('yes', 'true', '1') then true
            when lower(screened) in ('no', 'false', '0') then false
            else null
        end as screened,

        nullif(point_of_entry, '') as point_of_entry,
        nullif(created_timestamp, '')::timestamp as created_at,

        case
        when trim(latitude) ~ '^-?[0-9]+(\.[0-9]+)?$'
        then trim(latitude)::numeric
        else null
        end as latitude,

        _batch_id,
        _source_file,
        _raw_hash,
        _ingested_at

    from src

)

select *
from cleaned