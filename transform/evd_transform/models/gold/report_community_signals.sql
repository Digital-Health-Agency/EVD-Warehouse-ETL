
with community_signals as (

    select *
    from {{ ref('fct_community_signal') }}

),

locations as (

    select *
    from {{ ref('dim_location') }}

),

dates as (

    select *
    from {{ ref('dim_date') }}

),

epiweeks as (

    select *
    from {{ ref('dim_epiweek') }}

)

select
    --------------------------------------------------------------------
    -- Signal identifiers
    --------------------------------------------------------------------

    f.community_signal_key,
    f.source_system,
    f.source_signal_id,
    f.source_row_id,

    --------------------------------------------------------------------
    -- Signal details
    --------------------------------------------------------------------

    f.signal_description,
    f.signal_reported_at,

    f.signal_verified,
    f.signal_verified_true,
    f.signal_verification_date,

    f.signal_investigated,
    f.signal_investigation_date,

    --------------------------------------------------------------------
    -- Date mapping
    --------------------------------------------------------------------

    f.reported_date_key,
    d.full_date as reported_date,

    f.verification_date_key,
    f.investigation_date_key,

    --------------------------------------------------------------------
    -- Epidemiological week
    --------------------------------------------------------------------

    f.epi_week_key,
    ew.week_number,
    ew.epi_year,
    ew.epi_week_label,
    ew.start_of_week,
    ew.end_of_week,

    --------------------------------------------------------------------
    -- Location mapping
    --------------------------------------------------------------------

    f.location_key,
    l.county,
    l.subcounty,
    l.community_unit,
    l.unit_name,
    l.unit_type,

    --------------------------------------------------------------------
    -- Timeliness
    --------------------------------------------------------------------

    f.verification_time_hours,
    f.investigation_time_hours,

    --------------------------------------------------------------------
    -- Indicators
    --------------------------------------------------------------------

    f.signal_count as signals_reported,
    f.verified_signal_count as signals_verified,

    --------------------------------------------------------------------
    -- Lineage
    --------------------------------------------------------------------

    f._source,
    f._ingested_at

from community_signals f

left join locations l
    on f.location_key = l.location_key

left join dates d
    on f.reported_date_key = d.date_key

left join epiweeks ew
    on f.epi_week_key = ew.epi_week_key