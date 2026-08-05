with mdharura_signals as (

    select
        id as source_row_id,
        id_field as source_signal_id,

        'MDHARURA' as source_system,

        signal as signal_description,

        nullif(trim(community_unit), '') as community_unit,
        nullif(trim(unit_name), '') as unit_name,
        nullif(trim(unit_type), '') as unit_type,
        nullif(trim(county), '') as county,
        nullif(trim(subcounty), '') as subcounty,

        created_at as signal_reported_at,

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

),

prepared as (

    select
        *,

        initcap(county) as standardized_county,
        initcap(subcounty) as standardized_subcounty,
        initcap(community_unit) as standardized_community_unit,
        initcap(unit_name) as standardized_unit_name,
        initcap(unit_type) as standardized_unit_type,

        signal_reported_at::date as signal_reported_date,
        signal_verification_date::date as signal_verification_date_only,
        signal_investigation_date::date as signal_investigation_date_only

    from all_community_signals

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            's.source_system',
            "coalesce(s.source_signal_id, s.source_row_id::text)"
        ]) }} as community_signal_key,

        s.source_system,
        s.source_signal_id,
        s.source_row_id,

        l.location_key,

        reported_date.date_key as reported_date_key,
        verification_date.date_key as verification_date_key,
        investigation_date.date_key as investigation_date_key,
        epiweek.epi_week_key,

        s.signal_description,

        s.signal_reported_at,
        s.signal_reported_date,

        s.signal_verified,
        s.signal_verified_true,
        s.signal_verification_date,

        s.signal_investigated,
        s.signal_investigation_date,

        case
            when s.signal_reported_at is not null
             and s.signal_verification_date is not null
             and s.signal_verification_date >= s.signal_reported_at
            then round(
                (
                    extract(
                        epoch from (
                            s.signal_verification_date
                            - s.signal_reported_at
                        )
                    ) / 3600.0
                )::numeric,
                2
            )
            else null
        end as verification_time_hours,

        case
            when s.signal_reported_at is not null
             and s.signal_investigation_date is not null
             and s.signal_investigation_date >= s.signal_reported_at
            then round(
                (
                    extract(
                        epoch from (
                            s.signal_investigation_date
                            - s.signal_reported_at
                        )
                    ) / 3600.0
                )::numeric,
                2
            )
            else null
        end as investigation_time_hours,

        case
            when s.signal_verified is true
              or s.signal_verified_true is true
            then 1
            else 0
        end as verified_signal_count,

        case
            when s.signal_investigated is true
            then 1
            else 0
        end as investigated_signal_count,

        case
            when not coalesce(s.signal_verified, false)
             and not coalesce(s.signal_verified_true, false)
            then 1
            else 0
        end as unverified_signal_count,

        case
            when not coalesce(s.signal_investigated, false)
            then 1
            else 0
        end as uninvestigated_signal_count,

        1 as signal_count,

        s._source,
        s._ingested_at

    from prepared s

    left join {{ ref('dim_location') }} l
        on coalesce(s.standardized_county, 'UNKNOWN')
            = coalesce(l.county, 'UNKNOWN')
       and coalesce(s.standardized_subcounty, 'UNKNOWN')
            = coalesce(l.subcounty, 'UNKNOWN')
       and coalesce(s.standardized_community_unit, 'UNKNOWN')
            = coalesce(l.community_unit, 'UNKNOWN')
       and coalesce(s.standardized_unit_name, 'UNKNOWN')
            = coalesce(l.unit_name, 'UNKNOWN')
       and coalesce(s.standardized_unit_type, 'UNKNOWN')
            = coalesce(l.unit_type, 'UNKNOWN')

    left join {{ ref('dim_date') }} reported_date
        on s.signal_reported_date = reported_date.full_date

    left join {{ ref('dim_date') }} verification_date
        on s.signal_verification_date_only = verification_date.full_date

    left join {{ ref('dim_date') }} investigation_date
        on s.signal_investigation_date_only = investigation_date.full_date

    left join {{ ref('dim_epiweek') }} epiweek
        on s.signal_reported_date
        between epiweek.start_of_week and epiweek.end_of_week

)

select *
from final