
with community_signals as (

    select *
    from {{ ref('fct_community_signal') }}

),

final as (

    select
        ------------------------------------------------------------------
        -- Signal identifiers
        ------------------------------------------------------------------

        f.community_signal_key,
        f.source_system,
        f.source_signal_id,

        ------------------------------------------------------------------
        -- Signal details
        ------------------------------------------------------------------

        f.signal as signal_description,

        f.signal_created_at as created_at,
        f.signal_verification_at as verification_at,
        f.signal_investigation_at as investigation_at,

        f.signal_verified,
        f.signal_verified_true,
        f.signal_investigated,

        ------------------------------------------------------------------
        -- Calendar date
        ------------------------------------------------------------------

        f.created_date_key,
        created_date.full_date as created_date,

        f.verification_date_key,
        verification_date.full_date as verification_date,

        f.investigation_date_key,
        investigation_date.full_date as investigation_date,

        ------------------------------------------------------------------
        -- Epidemiological week
        ------------------------------------------------------------------

        f.epi_week_key,
        epi.week_number,
        epi.epi_year,
        epi.epi_week_label,
        epi.start_of_week,
        epi.end_of_week,

        ------------------------------------------------------------------
        -- Location
        ------------------------------------------------------------------

        f.location_key,
        location.county,
        location.subcounty,
        location.community_unit,
        location.unit_name,
        location.unit_type,

        ------------------------------------------------------------------
        -- Timeliness
        ------------------------------------------------------------------

        f.verification_time_hours,
        f.investigation_time_hours,

        ------------------------------------------------------------------
        -- Indicators
        ------------------------------------------------------------------

        f.signals_reported,
        f.signals_verified,
        f.signals_verified_true,
        f.signals_investigated,

        ------------------------------------------------------------------
        -- Lineage
        ------------------------------------------------------------------

        f._source,
        f._ingested_at

    from community_signals f

    left join {{ ref('dim_location') }} location
        on f.location_key = location.location_key

    left join {{ ref('dim_date') }} created_date
        on f.created_date_key = created_date.date_key

    left join {{ ref('dim_date') }} verification_date
        on f.verification_date_key = verification_date.date_key

    left join {{ ref('dim_date') }} investigation_date
        on f.investigation_date_key = investigation_date.date_key

    left join {{ ref('dim_epiweek') }} epi
        on f.epi_week_key = epi.epi_week_key

)

select *
from final