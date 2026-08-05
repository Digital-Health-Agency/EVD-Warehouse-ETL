{{ config(
    materialized = 'table',
    schema = 'silver',
    alias = 'slv_mdharura'
) }}

with source_data as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,
        _processed,

        id_field,
        signal,
        community_unit,
        subcounty,
        county,
        created_at,
        unit_name,
        unit_type,

        signal_verified,
        signal_verified_true,
        signal_investigated,

        signal_verification_date,
        signal_investigation_date

    from {{ source('bronze', 'mdharura_raw') }}

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
        nullif(trim(signal), '') as signal,

        nullif(trim(community_unit), '') as community_unit,
        nullif(trim(subcounty), '') as subcounty,
        nullif(trim(county), '') as county,

        nullif(trim(unit_name), '') as unit_name,
        nullif(trim(unit_type), '') as unit_type,

        /*
         * When the signal was created in mDharura.
         */
        case
            when nullif(trim(created_at), '') is null then null

            when trim(created_at)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                 and substring(trim(created_at), 1, 4)::integer
                     between 1 and 9999
                 and to_char(
                     to_date(trim(created_at), 'YYYY-MM-DD'),
                     'YYYY-MM-DD'
                 ) = trim(created_at)
                then to_date(
                    trim(created_at),
                    'YYYY-MM-DD'
                )::timestamp

            when trim(created_at)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$'
                 and substring(trim(created_at), 1, 4)::integer
                     between 1 and 9999
                then to_timestamp(
                    replace(trim(created_at), 'T', ' '),
                    'YYYY-MM-DD HH24:MI:SS'
                )

            when trim(created_at)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+$'
                 and substring(trim(created_at), 1, 4)::integer
                     between 1 and 9999
                then to_timestamp(
                    replace(trim(created_at), 'T', ' '),
                    'YYYY-MM-DD HH24:MI:SS.US'
                )

            when trim(created_at)
                    ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                 and right(trim(created_at), 4)::integer
                     between 1 and 9999
                 and to_char(
                     to_date(trim(created_at), 'DD/MM/YYYY'),
                     'DD/MM/YYYY'
                 ) = trim(created_at)
                then to_date(
                    trim(created_at),
                    'DD/MM/YYYY'
                )::timestamp

            else null
        end as signal_created_at,

        /*
         * Verification completed flag.
         */
        signal_verified,

        /*
         * Signal confirmed as a true signal/event.
         */
        signal_verified_true,

        /*
         * Investigation completed flag.
         */
        signal_investigated,

        /*
         * When verification was completed.
         */
        case
            when nullif(trim(signal_verification_date), '') is null then null

            when trim(signal_verification_date)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                 and substring(
                     trim(signal_verification_date),
                     1,
                     4
                 )::integer between 1 and 9999
                 and to_char(
                     to_date(
                         trim(signal_verification_date),
                         'YYYY-MM-DD'
                     ),
                     'YYYY-MM-DD'
                 ) = trim(signal_verification_date)
                then to_date(
                    trim(signal_verification_date),
                    'YYYY-MM-DD'
                )::timestamp

            when trim(signal_verification_date)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$'
                 and substring(
                     trim(signal_verification_date),
                     1,
                     4
                 )::integer between 1 and 9999
                then to_timestamp(
                    replace(
                        trim(signal_verification_date),
                        'T',
                        ' '
                    ),
                    'YYYY-MM-DD HH24:MI:SS'
                )

            when trim(signal_verification_date)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+$'
                 and substring(
                     trim(signal_verification_date),
                     1,
                     4
                 )::integer between 1 and 9999
                then to_timestamp(
                    replace(
                        trim(signal_verification_date),
                        'T',
                        ' '
                    ),
                    'YYYY-MM-DD HH24:MI:SS.US'
                )

            when trim(signal_verification_date)
                    ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                 and right(
                     trim(signal_verification_date),
                     4
                 )::integer between 1 and 9999
                 and to_char(
                     to_date(
                         trim(signal_verification_date),
                         'DD/MM/YYYY'
                     ),
                     'DD/MM/YYYY'
                 ) = trim(signal_verification_date)
                then to_date(
                    trim(signal_verification_date),
                    'DD/MM/YYYY'
                )::timestamp

            else null
        end as signal_verification_at,

        /*
         * When investigation was completed.
         */
        case
            when nullif(trim(signal_investigation_date), '') is null then null

            when trim(signal_investigation_date)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                 and substring(
                     trim(signal_investigation_date),
                     1,
                     4
                 )::integer between 1 and 9999
                 and to_char(
                     to_date(
                         trim(signal_investigation_date),
                         'YYYY-MM-DD'
                     ),
                     'YYYY-MM-DD'
                 ) = trim(signal_investigation_date)
                then to_date(
                    trim(signal_investigation_date),
                    'YYYY-MM-DD'
                )::timestamp

            when trim(signal_investigation_date)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$'
                 and substring(
                     trim(signal_investigation_date),
                     1,
                     4
                 )::integer between 1 and 9999
                then to_timestamp(
                    replace(
                        trim(signal_investigation_date),
                        'T',
                        ' '
                    ),
                    'YYYY-MM-DD HH24:MI:SS'
                )

            when trim(signal_investigation_date)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+$'
                 and substring(
                     trim(signal_investigation_date),
                     1,
                     4
                 )::integer between 1 and 9999
                then to_timestamp(
                    replace(
                        trim(signal_investigation_date),
                        'T',
                        ' '
                    ),
                    'YYYY-MM-DD HH24:MI:SS.US'
                )

            when trim(signal_investigation_date)
                    ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                 and right(
                     trim(signal_investigation_date),
                     4
                 )::integer between 1 and 9999
                 and to_char(
                     to_date(
                         trim(signal_investigation_date),
                         'DD/MM/YYYY'
                     ),
                     'DD/MM/YYYY'
                 ) = trim(signal_investigation_date)
                then to_date(
                    trim(signal_investigation_date),
                    'DD/MM/YYYY'
                )::timestamp

            else null
        end as signal_investigation_at

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

final as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,
        _processed,

        id_field,
        signal,
        community_unit,
        subcounty,
        county,
        unit_name,
        unit_type,

        signal_created_at,

        signal_verified,
        signal_verified_true,
        signal_verification_at,

        signal_investigated,
        signal_investigation_at

    from ranked

    where row_number = 1

)

select *
from final