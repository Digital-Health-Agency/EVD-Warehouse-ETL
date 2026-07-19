
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
        type,
        initial_classification,
        outcome,
        samples_collected,
        specimen_id,
        final_laboratory_results,
        reporting_county,
        reporting_subcounty,
        health_facility,
        date_of_investigation,
        created_timestamp,
        latitude,
        longitude,
        vhf_disease,
        date_of_death,
        final_classification,
        checked_by

    from {{ source('bronze', 'adam_cases_raw') }}

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

        case
            when lower(trim(sex)) in ('male', 'm') then 'Male'
            when lower(trim(sex)) in ('female', 'f') then 'Female'
            when nullif(trim(sex), '') is null then null
            else initcap(trim(sex))
        end as sex,

        /*
         * Safely convert date of birth.
         * Invalid dates and dates with year 0000 become null.
         */
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
        nullif(trim(type), '') as type,
        nullif(trim(initial_classification), '') as initial_classification,
        nullif(trim(outcome), '') as outcome,
        nullif(trim(samples_collected), '') as samples_collected,
        nullif(trim(specimen_id), '') as specimen_id,

        nullif(
            trim(final_laboratory_results),
            ''
        ) as final_laboratory_results,

        nullif(trim(reporting_county), '') as reporting_county,

        nullif(
            trim(reporting_subcounty),
            ''
        ) as reporting_subcounty,

        nullif(trim(health_facility), '') as health_facility,

        /*
         * Safely convert date of investigation.
         */
        case
            when nullif(trim(date_of_investigation), '') is null then null

            when trim(date_of_investigation)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                 and substring(
                     trim(date_of_investigation),
                     1,
                     4
                 )::integer between 1 and 9999
                 and to_char(
                     to_date(
                         trim(date_of_investigation),
                         'YYYY-MM-DD'
                     ),
                     'YYYY-MM-DD'
                 ) = trim(date_of_investigation)
                then to_date(
                    trim(date_of_investigation),
                    'YYYY-MM-DD'
                )

            when trim(date_of_investigation)
                    ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                 and right(
                     trim(date_of_investigation),
                     4
                 )::integer between 1 and 9999
                 and to_char(
                     to_date(
                         trim(date_of_investigation),
                         'DD/MM/YYYY'
                     ),
                     'DD/MM/YYYY'
                 ) = trim(date_of_investigation)
                then to_date(
                    trim(date_of_investigation),
                    'DD/MM/YYYY'
                )

            else null
        end as date_of_investigation,

        /*
         * Safely convert created timestamp.
         */
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
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+$'
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
                    'YYYY-MM-DD HH24:MI:SS.US'
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

        /*
         * Latitude must be numeric and between -90 and 90.
         */
        case
            when nullif(trim(latitude), '') is null then null

            when trim(latitude) ~ '^-?[0-9]+(\.[0-9]+)?$'
                 and trim(latitude)::double precision
                     between -90 and 90
                then trim(latitude)::double precision

            else null
        end as latitude,

        /*
         * Longitude must be numeric and between -180 and 180.
         */
        case
            when nullif(trim(longitude), '') is null then null

            when trim(longitude) ~ '^-?[0-9]+(\.[0-9]+)?$'
                 and trim(longitude)::double precision
                     between -180 and 180
                then trim(longitude)::double precision

            else null
        end as longitude,

        nullif(trim(vhf_disease), '') as vhf_disease,

        /*
         * Safely convert date of death.
         */
        case
            when nullif(trim(date_of_death), '') is null then null

            when trim(date_of_death)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                 and substring(trim(date_of_death), 1, 4)::integer
                     between 1 and 9999
                 and to_char(
                     to_date(trim(date_of_death), 'YYYY-MM-DD'),
                     'YYYY-MM-DD'
                 ) = trim(date_of_death)
                then to_date(
                    trim(date_of_death),
                    'YYYY-MM-DD'
                )

            when trim(date_of_death)
                    ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                 and right(trim(date_of_death), 4)::integer
                     between 1 and 9999
                 and to_char(
                     to_date(trim(date_of_death), 'DD/MM/YYYY'),
                     'DD/MM/YYYY'
                 ) = trim(date_of_death)
                then to_date(
                    trim(date_of_death),
                    'DD/MM/YYYY'
                )

            else null
        end as date_of_death,

        nullif(trim(final_classification), '') as final_classification,
        nullif(trim(checked_by), '') as checked_by

    from source_data

),

ranked as (

    select
        *,

        row_number() over (
            partition by coalesce(
                id_field,
                '__bronze_id_' || id::text
            )
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
        type,
        initial_classification,
        outcome,
        samples_collected,
        specimen_id,
        final_laboratory_results,
        reporting_county,
        reporting_subcounty,
        health_facility,
        date_of_investigation,
        created_timestamp,
        latitude,
        longitude,
        vhf_disease,
        date_of_death,
        final_classification,
        checked_by

    from ranked

    where row_number = 1

)

select *
from deduplicated