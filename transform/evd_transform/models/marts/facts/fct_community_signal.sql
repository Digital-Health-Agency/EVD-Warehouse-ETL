{{ config(
    materialized = 'table',
    schema = 'marts',
    alias = 'fct_community_signal'
) }}

with mdharura_signals as (

    select
        id as source_row_id,
        id_field as source_signal_id,

        'MDHARURA' as source_system,

        signal,

        initcap(nullif(trim(county), '')) as county,
        initcap(nullif(trim(subcounty), '')) as subcounty,
        initcap(nullif(trim(community_unit), '')) as community_unit,
        initcap(nullif(trim(unit_name), '')) as unit_name,
        initcap(nullif(trim(unit_type), '')) as unit_type,

        signal_created_date,

        signal_verified,
        signal_verified_true,
        signal_verification_date,

        signal_investigated,
        signal_investigation_date,

        _source,
        _ingested_at

    from {{ ref('slv_mdharura') }}

),

all_community_signals as (

    select *
    from mdharura_signals

    /*
    Future community signal source:

    union all

    select
        id as source_row_id,
        signal_id as source_signal_id,
        'ECHIS' as source_system,
        signal,
        county,
        subcounty,
        community_unit,
        unit_name,
        unit_type,
    
    */

),

final as (

    select
        ------------------------------------------------------------------
        -- Community signal key
        ------------------------------------------------------------------

        {{ dbt_utils.generate_surrogate_key([
            's.source_system',
            "coalesce(s.source_signal_id, s.source_row_id::text)"
        ]) }} as community_signal_key,

        ------------------------------------------------------------------
        -- Source identifiers
        ------------------------------------------------------------------

        s.source_system,
        s.source_signal_id,
        s.source_row_id,

        ------------------------------------------------------------------
        -- Dimension keys
        ------------------------------------------------------------------

        location.location_key,

        created_date.date_key as created_date_key,
        verification_date.date_key as verification_date_key,
        investigation_date.date_key as investigation_date_key,

        epiweek.epi_week_key,

        ------------------------------------------------------------------
        -- Signal details
        ------------------------------------------------------------------

        s.signal,

        s.signal_created_date,

        s.signal_verified,
        s.signal_verified_true,
        s.signal_verification_date,

        s.signal_investigated,
        s.signal_investigation_date,

        ------------------------------------------------------------------
        -- Date-level turnaround measures
        ------------------------------------------------------------------

        case
            when s.signal_created_date is not null
             and s.signal_verification_date is not null
             and s.signal_verification_date >= s.signal_created_date
                then (
                    s.signal_verification_date
                    - s.signal_created_date
                )
            else null
        end as verification_time_days,

        case
            when s.signal_created_date is not null
             and s.signal_investigation_date is not null
             and s.signal_investigation_date >= s.signal_created_date
                then (
                    s.signal_investigation_date
                    - s.signal_created_date
                )
            else null
        end as investigation_time_days,

        ------------------------------------------------------------------
        -- Measures
        ------------------------------------------------------------------

        1 as signals_reported,

        case
            when s.signal_verified is true then 1
            else 0
        end as signals_verified,

        case
            when s.signal_verified_true is true then 1
            else 0
        end as signals_verified_true,

        case
            when s.signal_investigated is true then 1
            else 0
        end as signals_investigated,

        ------------------------------------------------------------------
        -- Lineage
        ------------------------------------------------------------------

        s._source,
        s._ingested_at

    from all_community_signals s

    ----------------------------------------------------------------------
    -- Location lookup
    ----------------------------------------------------------------------

    left join {{ ref('dim_location') }} location
        on coalesce(s.county, 'UNKNOWN')
            = coalesce(location.county, 'UNKNOWN')

       and coalesce(s.subcounty, 'UNKNOWN')
            = coalesce(location.subcounty, 'UNKNOWN')

       and coalesce(s.community_unit, 'UNKNOWN')
            = coalesce(location.community_unit, 'UNKNOWN')

       and coalesce(s.unit_name, 'UNKNOWN')
            = coalesce(location.unit_name, 'UNKNOWN')

       and coalesce(s.unit_type, 'UNKNOWN')
            = coalesce(location.unit_type, 'UNKNOWN')

    ----------------------------------------------------------------------
    -- Date lookups
    ----------------------------------------------------------------------

    left join {{ ref('dim_date') }} created_date
        on s.signal_created_date = created_date.full_date

    left join {{ ref('dim_date') }} verification_date
        on s.signal_verification_date = verification_date.full_date

    left join {{ ref('dim_date') }} investigation_date
        on s.signal_investigation_date = investigation_date.full_date

    ----------------------------------------------------------------------
    -- Epidemiological week based on signal creation date
    ----------------------------------------------------------------------

    left join {{ ref('dim_epiweek') }} epiweek
        on s.signal_created_date
        between epiweek.start_of_week and epiweek.end_of_week

)

select *
from final