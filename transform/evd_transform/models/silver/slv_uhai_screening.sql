{{ config(
    materialized = 'table',
    schema = 'silver',
    alias = 'slv_uhai_screening'
) }}

with source_data as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,
        _processed,

        system_id,
        names,
        sex,
        date_of_birth,
        nationality,
        identifier_type,
        identifier,

        suspected,
        screening,
        confirmed,
        died,
        recovered,
        tested,
        result,

        point_of_entry,

        reporting_county,
        reporting_sub_county,
        ward,
        facility_fid,
        community_health_unit_chu,

        reporting_date,
        reporting_time,
        created_at
    from {{ source('bronze', 'uhai_raw') }}

),

cleaned as (

    select
        id,
        _ingested_at,
        nullif(trim(_source), '') as _source,
        _batch_id,
        nullif(trim(_source_file), '') as _source_file,
        coalesce(_processed, false) as _processed,

        nullif(trim(system_id), '') as system_id,
        nullif(trim(names), '') as names,

        case
            when lower(trim(sex)) in ('male', 'm') then 'Male'
            when lower(trim(sex)) in ('female', 'f') then 'Female'
            when nullif(trim(sex), '') is null then null
            else initcap(trim(sex))
        end as sex,

        case
            when nullif(trim(date_of_birth), '') is null then null
            when trim(date_of_birth)
                ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(trim(date_of_birth), 1, 10)::date
            else null
        end as date_of_birth,

        nullif(trim(nationality), '') as nationality,
        nullif(trim(identifier_type), '') as identifier_type,
        nullif(trim(identifier), '') as identifier,

        case
            when lower(trim(screening)) in ('true','1','yes','y')
                then true
            when lower(trim(screening)) in ('false','0','no','n')
                then false
            else null
        end as screened_flag,

        case
            when lower(trim(suspected)) in ('true','1','yes','y')
                then true
            when lower(trim(suspected)) in ('false','0','no','n')
                then false
            else null
        end as suspected_flag,

        case
            when lower(trim(confirmed)) in ('true','1','yes','y')
                then true
            when lower(trim(confirmed)) in ('false','0','no','n')
                then false
            else null
        end as confirmed_flag,

        case
            when lower(trim(died)) in ('true','1','yes','y')
                then true
            when lower(trim(died)) in ('false','0','no','n')
                then false
            else null
        end as died_flag,

        case
            when lower(trim(recovered)) in ('true','1','yes','y')
                then true
            when lower(trim(recovered)) in ('false','0','no','n')
                then false
            else null
        end as recovered_flag,

        case
            when lower(trim(tested)) in ('true','1','yes','y')
                then true
            when lower(trim(tested)) in ('false','0','no','n')
                then false
            else null
        end as tested_flag,

        nullif(trim(result), '') as result,

        nullif(trim(point_of_entry), '') as point_of_entry,

        initcap(nullif(trim(reporting_county), '')) as county,
        initcap(nullif(trim(reporting_sub_county), '')) as subcounty,
        initcap(nullif(trim(ward), '')) as ward,

        nullif(trim(facility_fid), '') as facility_code,
        nullif(trim(community_health_unit_chu), '') as community_health_unit,

        case
            when nullif(trim(reporting_date), '') is null then null
            when trim(reporting_date)
                ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(trim(reporting_date), 1, 10)::date
            else null
        end as screening_date,

        nullif(trim(reporting_time), '') as reporting_time,

        case
            when nullif(trim(created_at), '') is null then null
            when trim(created_at)
                ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?$'
                then replace(trim(created_at), 'T', ' ')::timestamp
            else null
        end as created_at

    from source_data

),

ranked as (

    select
        *,

        row_number() over (
            partition by coalesce(
                system_id,
                identifier,
                '__bronze_id_' || id::text
            )
            order by
                _ingested_at desc nulls last,
                id desc
        ) as row_number

    from cleaned

)

select
    id,
    _ingested_at,
    _source,
    _batch_id,
    _source_file,
    _processed,

    system_id,
    names,
    sex,
    date_of_birth,
    nationality,
    identifier_type,
    identifier,

    screened_flag,
    suspected_flag,
    confirmed_flag,
    died_flag,
    recovered_flag,
    tested_flag,
    result,

    point_of_entry,

    county,
    subcounty,
    ward,
    facility_code,
    community_health_unit,

    screening_date,
    reporting_time,
    created_at

from ranked
where row_number = 1