

with source_data as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,
        _processed,

        id_field,
        name,
        sex,
        date_of_birth,
        nationality,
        identifier,
        classification,
        screened,
        point_of_entry,
        created_timestamp,
        latitude,
        longitude

    from {{ source('bronze', 'adam_travellers_raw') }}

),

cleaned as (

    select
        id,
        _ingested_at,
        nullif(trim(_source), '') as _source,
        _batch_id,
        nullif(trim(_source_file), '') as _source_file,
        coalesce(_processed, false) as _processed,

        nullif(trim(id_field), '') as id_field,
        nullif(trim(name), '') as name,
        nullif(trim(sex), '') as sex,

        case
            when nullif(trim(date_of_birth), '') is null then null

            when trim(date_of_birth)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                 and substring(trim(date_of_birth), 1, 4)::integer
                     between 1 and 9999
                 and to_char(
                     to_date(trim(date_of_birth), 'YYYY-MM-DD'),
                     'YYYY-MM-DD'
                 ) = trim(date_of_birth)
                then to_date(
                    trim(date_of_birth),
                    'YYYY-MM-DD'
                )

            when trim(date_of_birth)
                    ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                 and right(trim(date_of_birth), 4)::integer
                     between 1 and 9999
                 and to_char(
                     to_date(trim(date_of_birth), 'DD/MM/YYYY'),
                     'DD/MM/YYYY'
                 ) = trim(date_of_birth)
                then to_date(
                    trim(date_of_birth),
                    'DD/MM/YYYY'
                )

            else null
        end as date_of_birth,

        nullif(trim(nationality), '') as nationality,
        nullif(trim(identifier), '') as identifier,
        nullif(trim(classification), '') as classification,

        case
            when lower(trim(screened)) in (
                'yes',
                'true',
                '1',
                'y'
            ) then true

            when lower(trim(screened)) in (
                'no',
                'false',
                '0',
                'n'
            ) then false

            else null
        end as screened,

        nullif(trim(point_of_entry), '') as point_of_entry,

        case
            when nullif(trim(created_timestamp), '') is null then null

            when trim(created_timestamp)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                 and substring(
                     trim(created_timestamp),
                     1,
                     4
                 )::integer between 1 and 9999
                 and to_char(
                     to_date(
                         trim(created_timestamp),
                         'YYYY-MM-DD'
                     ),
                     'YYYY-MM-DD'
                 ) = trim(created_timestamp)
                then to_date(
                    trim(created_timestamp),
                    'YYYY-MM-DD'
                )::timestamp

            when trim(created_timestamp)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$'
                 and substring(
                     trim(created_timestamp),
                     1,
                     4
                 )::integer between 1 and 9999
                then to_timestamp(
                    replace(
                        trim(created_timestamp),
                        'T',
                        ' '
                    ),
                    'YYYY-MM-DD HH24:MI:SS'
                )

            when trim(created_timestamp)
                    ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                 and right(
                     trim(created_timestamp),
                     4
                 )::integer between 1 and 9999
                 and to_char(
                     to_date(
                         trim(created_timestamp),
                         'DD/MM/YYYY'
                     ),
                     'DD/MM/YYYY'
                 ) = trim(created_timestamp)
                then to_date(
                    trim(created_timestamp),
                    'DD/MM/YYYY'
                )::timestamp

            else null
        end as created_timestamp,

        case
            when nullif(trim(latitude), '') is null then null

            when trim(latitude)
                    ~ '^-?[0-9]+(\.[0-9]+)?$'
                 and trim(latitude)::double precision
                     between -90 and 90
                then trim(latitude)::double precision

            else null
        end as latitude,

        case
            when nullif(trim(longitude), '') is null then null

            when trim(longitude)
                    ~ '^-?[0-9]+(\.[0-9]+)?$'
                 and trim(longitude)::double precision
                     between -180 and 180
                then trim(longitude)::double precision

            else null
        end as longitude

    from source_data

),

ranked as (

    select
        *,

        row_number() over (
            partition by id_field
            order by
                _ingested_at desc nulls last,
                id desc
        ) as row_number

    from cleaned

),

deduplicated as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,
        _processed,

        id_field,
        name,
        sex,
        date_of_birth,
        nationality,
        identifier,
        classification,
        screened,
        point_of_entry,
        created_timestamp,
        latitude,
        longitude

    from ranked

    where row_number = 1

)

select *
from deduplicated