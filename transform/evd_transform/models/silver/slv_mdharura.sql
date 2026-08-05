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
        ------------------------------------------------------------------
        -- Technical metadata
        ------------------------------------------------------------------

        id,
        _ingested_at,
        nullif(trim(_source), '') as _source,
        _batch_id,
        nullif(trim(_source_file), '') as _source_file,
        coalesce(_processed, false) as _processed,

        ------------------------------------------------------------------
        -- Signal identifiers
        ------------------------------------------------------------------

        nullif(trim(id_field), '') as id_field,
        nullif(trim(signal), '') as signal,

        ------------------------------------------------------------------
        -- Location attributes
        ------------------------------------------------------------------

        nullif(trim(community_unit), '') as community_unit,
        nullif(trim(subcounty), '') as subcounty,
        nullif(trim(county), '') as county,
        nullif(trim(unit_name), '') as unit_name,
        nullif(trim(unit_type), '') as unit_type,

        ------------------------------------------------------------------
        -- Signal creation date
        ------------------------------------------------------------------

        case
            when nullif(trim(created_at), '') is null then null
            else trim(created_at)::timestamp::date
        end as signal_created_date,

        ------------------------------------------------------------------
        -- Signal status flags
        ------------------------------------------------------------------

        signal_verified,
        signal_verified_true,
        signal_investigated,

        ------------------------------------------------------------------
        -- Verification date
        ------------------------------------------------------------------

        case
            when nullif(trim(signal_verification_date), '') is null
                then null
            else trim(signal_verification_date)::timestamp::date
        end as signal_verification_date,

        ------------------------------------------------------------------
        -- Investigation date
        ------------------------------------------------------------------

        case
            when nullif(trim(signal_investigation_date), '') is null
                then null
            else trim(signal_investigation_date)::timestamp::date
        end as signal_investigation_date

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
        ------------------------------------------------------------------
        -- Technical metadata
        ------------------------------------------------------------------

        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,
        _processed,

        ------------------------------------------------------------------
        -- Business fields
        ------------------------------------------------------------------

        id_field,
        signal,

        community_unit,
        subcounty,
        county,
        unit_name,
        unit_type,

        signal_created_date,

        signal_verified,
        signal_verified_true,
        signal_verification_date,

        signal_investigated,
        signal_investigation_date

    from ranked

    where row_number = 1

)

select *
from final