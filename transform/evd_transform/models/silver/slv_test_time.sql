{{ config(
    materialized = 'table',
    schema = 'silver'
) }}

with source_data as (

    select
        id,
        _ingested_at,
        id_field,
        identifier,
        created_timestamp

    from {{ source('bronze', 'adam_travellers_raw') }}

),

timestamp_test as (

    select
        id,
        _ingested_at,
        id_field,
        identifier,

        created_timestamp
            as raw_created_timestamp,

        trim(created_timestamp)
            as trimmed_created_timestamp,

        length(created_timestamp)
            as raw_character_length,

        length(trim(created_timestamp))
            as trimmed_character_length,

        case
            when created_timestamp is null
                then 'SOURCE_NULL'

            when trim(created_timestamp) = ''
                then 'SOURCE_EMPTY'

            when trim(created_timestamp)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?([+-][0-9]{2}(:?[0-9]{2})?|Z)?$'
                then 'ISO_TIMESTAMP_MATCH'

            else 'NO_PATTERN_MATCH'
        end as timestamp_pattern_status,

        case
            when nullif(trim(created_timestamp), '') is null
                then null

            else trim(created_timestamp)::timestamptz
        end as parsed_created_timestamp,

        case
            when nullif(trim(created_timestamp), '') is null
                then null

            else (
                trim(created_timestamp)::timestamptz
                at time zone 'Africa/Nairobi'
            )::date
        end as parsed_screening_date

    from source_data

)

select *
from timestamp_test