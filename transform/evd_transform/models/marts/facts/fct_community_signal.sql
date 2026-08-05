
with signals as (

    select *
    from {{ ref('slv_mdharura') }}

),

prepared as (

    select

        id,
        id_field,

        'MDHARURA' as source_system,

        signal,

        county,
        subcounty,
        community_unit,
        unit_name,
        unit_type,

        signal_created_at,
        signal_verification_at,
        signal_investigation_at,

        signal_verified,
        signal_verified_true,
        signal_investigated,

        _source,
        _ingested_at

    from signals

),

final as (

    select

        ------------------------------------------------------------------
        -- Primary Key
        ------------------------------------------------------------------

        {{ dbt_utils.generate_surrogate_key([
            "'MDHARURA'",
            'id_field'
        ]) }} as community_signal_key,

        ------------------------------------------------------------------
        -- Business Keys
        ------------------------------------------------------------------

        'MDHARURA' as source_system,

        id_field as source_signal_id,

        ------------------------------------------------------------------
        -- Location
        ------------------------------------------------------------------

        l.location_key,

        ------------------------------------------------------------------
        -- Date Keys
        ------------------------------------------------------------------

        created_date.date_key       as created_date_key,
        verification_date.date_key  as verification_date_key,
        investigation_date.date_key as investigation_date_key,

        epi.epi_week_key,

        ------------------------------------------------------------------
        -- Signal Information
        ------------------------------------------------------------------

        signal,

        signal_created_at,
        signal_verification_at,
        signal_investigation_at,

        signal_verified,
        signal_verified_true,
        signal_investigated,

        ------------------------------------------------------------------
        -- Timeliness
        ------------------------------------------------------------------

        case
            when signal_created_at is not null
             and signal_verification_at is not null
            then round(
                extract(
                    epoch from (
                        signal_verification_at
                        - signal_created_at
                    )
                ) / 3600.0,
                2
            )
        end as verification_time_hours,

        case
            when signal_created_at is not null
             and signal_investigation_at is not null
            then round(
                extract(
                    epoch from (
                        signal_investigation_at
                        - signal_created_at
                    )
                ) / 3600.0,
                2
            )
        end as investigation_time_hours,

        ------------------------------------------------------------------
        -- Measures
        ------------------------------------------------------------------

        1 as signals_reported,

        case
            when signal_verified then 1
            else 0
        end as signals_verified,

        case
            when signal_verified_true then 1
            else 0
        end as signals_verified_true,

        case
            when signal_investigated then 1
            else 0
        end as signals_investigated,

        ------------------------------------------------------------------
        -- Lineage
        ------------------------------------------------------------------

        _source,
        _ingested_at

    from prepared s

    ----------------------------------------------------------------------
    -- Location Dimension
    ----------------------------------------------------------------------

    left join {{ ref('dim_location') }} l
        on s.county = l.county
       and s.subcounty = l.subcounty
       and coalesce(s.community_unit,'') = coalesce(l.community_unit,'')
       and coalesce(s.unit_name,'') = coalesce(l.unit_name,'')
       and coalesce(s.unit_type,'') = coalesce(l.unit_type,'')

    ----------------------------------------------------------------------
    -- Calendar Date
    ----------------------------------------------------------------------

    left join {{ ref('dim_date') }} created_date
        on s.signal_created_at::date = created_date.full_date

    left join {{ ref('dim_date') }} verification_date
        on s.signal_verification_at::date = verification_date.full_date

    left join {{ ref('dim_date') }} investigation_date
        on s.signal_investigation_at::date = investigation_date.full_date

    ----------------------------------------------------------------------
    -- Epidemiological Week
    ----------------------------------------------------------------------

    left join {{ ref('dim_epiweek') }} epi
        on s.signal_created_at::date
        between epi.start_of_week and epi.end_of_week

)

select *
from final