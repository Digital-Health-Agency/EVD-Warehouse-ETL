with source_data as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,
        _processed,

        subject_identifier,
        identifier,
        order_identifier,
        specimen_identifier,
        specimen_type,
        collect_date,
        performer_mfl,
        effective_date_time,
        code,
        test_name,
        code_text,
        component_code,
        unit,
        conclusion,
        value_code,
        reference_range_low,
        reference_range_high,
        case_identifier

    from {{ source('bronze', 'lims_raw') }}

),

cleaned as (

    select
        id,
        _ingested_at,
        nullif(trim(_source), '') as _source,
        _batch_id,
        nullif(trim(_source_file), '') as _source_file,
        coalesce(_processed, false) as _processed,

        nullif(trim(subject_identifier), '') as subject_identifier,
        nullif(trim(identifier), '') as identifier,
        nullif(trim(order_identifier), '') as order_identifier,
        nullif(trim(specimen_identifier), '') as specimen_identifier,
        nullif(trim(specimen_type), '') as specimen_type,

        case
            when nullif(trim(collect_date), '') is null then null

            when trim(collect_date)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                 and substring(trim(collect_date), 1, 4)::integer
                     between 1 and 9999
                 and to_char(
                     to_date(trim(collect_date), 'YYYY-MM-DD'),
                     'YYYY-MM-DD'
                 ) = trim(collect_date)
                then to_date(
                    trim(collect_date),
                    'YYYY-MM-DD'
                )

            when trim(collect_date)
                    ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                 and right(trim(collect_date), 4)::integer
                     between 1 and 9999
                 and to_char(
                     to_date(trim(collect_date), 'DD/MM/YYYY'),
                     'DD/MM/YYYY'
                 ) = trim(collect_date)
                then to_date(
                    trim(collect_date),
                    'DD/MM/YYYY'
                )

            else null
        end as collect_date,

        nullif(trim(performer_mfl), '') as performer_mfl,

        case
            when nullif(trim(effective_date_time), '') is null then null

            when trim(effective_date_time)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                 and substring(
                     trim(effective_date_time),
                     1,
                     4
                 )::integer between 1 and 9999
                 and to_char(
                     to_date(
                         trim(effective_date_time),
                         'YYYY-MM-DD'
                     ),
                     'YYYY-MM-DD'
                 ) = trim(effective_date_time)
                then to_date(
                    trim(effective_date_time),
                    'YYYY-MM-DD'
                )::timestamp

            when trim(effective_date_time)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$'
                 and substring(
                     trim(effective_date_time),
                     1,
                     4
                 )::integer between 1 and 9999
                then to_timestamp(
                    replace(
                        trim(effective_date_time),
                        'T',
                        ' '
                    ),
                    'YYYY-MM-DD HH24:MI:SS'
                )

            when trim(effective_date_time)
                    ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+$'
                 and substring(
                     trim(effective_date_time),
                     1,
                     4
                 )::integer between 1 and 9999
                then to_timestamp(
                    replace(
                        trim(effective_date_time),
                        'T',
                        ' '
                    ),
                    'YYYY-MM-DD HH24:MI:SS.US'
                )

            else null
        end as effective_date_time,

        nullif(trim(code), '') as code,
        nullif(trim(test_name), '') as test_name,
        nullif(trim(code_text), '') as code_text,
        nullif(trim(component_code), '') as component_code,
        nullif(trim(unit), '') as unit,
        nullif(trim(conclusion), '') as conclusion,
        nullif(trim(value_code), '') as value_code,

        case
            when nullif(trim(reference_range_low), '') is null then null
            when trim(reference_range_low)
                    ~ '^-?[0-9]+(\.[0-9]+)?$'
                then trim(reference_range_low)::numeric
            else null
        end as reference_range_low,

        case
            when nullif(trim(reference_range_high), '') is null then null
            when trim(reference_range_high)
                    ~ '^-?[0-9]+(\.[0-9]+)?$'
                then trim(reference_range_high)::numeric
            else null
        end as reference_range_high,

        nullif(trim(case_identifier), '') as case_identifier

    from source_data

),

ranked as (

    select
        *,

        row_number() over (
            partition by coalesce(
                identifier,
                specimen_identifier,
                order_identifier,
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

        subject_identifier,
        identifier,
        order_identifier,
        specimen_identifier,
        specimen_type,
        collect_date,
        performer_mfl,
        effective_date_time,
        code,
        test_name,
        code_text,
        component_code,
        unit,
        conclusion,
        value_code,
        reference_range_low,
        reference_range_high,
        case_identifier

    from ranked

    where row_number = 1

)

select *
from deduplicated