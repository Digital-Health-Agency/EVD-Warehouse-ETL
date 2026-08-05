

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
        f.source_row_id,

        ------------------------------------------------------------------
        -- Signal details
        ------------------------------------------------------------------

        f.signal as signal_description,

        ------------------------------------------------------------------
        -- Signal lifecycle dates
        ------------------------------------------------------------------

        f.signal_created_date,

        f.signal_verified,
        f.signal_verified_true,
        f.signal_verification_date,

        f.signal_investigated,
        f.signal_investigation_date,

        ------------------------------------------------------------------
        -- Calendar dimension mapping
        ------------------------------------------------------------------

        f.created_date_key,
        created_date.full_date as created_date,

        f.verification_date_key,
        verification_date.full_date as verified_date,

        f.investigation_date_key,
        investigation_date.full_date as investigated_date,

        ------------------------------------------------------------------
        -- Epidemiological week
        ------------------------------------------------------------------

        f.epi_week_key,
        epiweek.week_number,
        epiweek.epi_year,
        epiweek.epi_week_label,
        epiweek.start_of_week,
        epiweek.end_of_week,

        ------------------------------------------------------------------
        -- Location mapping
        ------------------------------------------------------------------

        f.location_key,
        location.county,
        location.subcounty,
        location.community_unit,
        location.unit_name,
        location.unit_type,

        ------------------------------------------------------------------
        -- Turnaround measures
        ------------------------------------------------------------------

        f.verification_time_days,
        f.investigation_time_days,

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

    left join {{ ref('dim_epiweek') }} epiweek
        on f.epi_week_key = epiweek.epi_week_key

)

select *
from final