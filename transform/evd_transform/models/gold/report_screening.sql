{{ config(
    materialized = 'table',
    schema = 'gold',
    alias = 'report_screening'
) }}

with screening_source as (

    select
        f.*,

        /*
         * Age in whole years at the screening date: from the date of birth
         * where the source has one (ADaM, Taifa Care), else the age the
         * source reported (UHAI). Ages outside 0-110 are treated as
         * unknown: some UHAI records hold a birth year (e.g. 1985) or a
         * mistyped value in the age field.
         */
        case
            when f.source_person_date_of_birth is not null
             and f.screening_date is not null
             and f.source_person_date_of_birth <= f.screening_date
             and extract(
                    year from age(
                        f.screening_date,
                        f.source_person_date_of_birth
                    )
                 ) <= 110
                then extract(
                    year from age(
                        f.screening_date,
                        f.source_person_date_of_birth
                    )
                )::integer

            when f.source_person_date_of_birth is null
             and f.source_person_age_years between 0 and 110
                then f.source_person_age_years

            else null
        end as age_at_screening

    from {{ ref('fct_screening') }} f

),

screening_enriched as (

    select
        ------------------------------------------------------------------
        -- Retain all fact columns
        ------------------------------------------------------------------

        s.*,

        ------------------------------------------------------------------
        -- Calendar hierarchy
        ------------------------------------------------------------------

        d.full_date
            as reporting_date,

        d.year
            as reporting_year,

        d.quarter
            as reporting_quarter,

        d.month
            as reporting_month_number,

        d.month_name
            as reporting_month_name,

        ------------------------------------------------------------------
        -- Epidemiological week hierarchy
        ------------------------------------------------------------------

        e.epi_week_key
            as reporting_epi_week_key,

        e.epi_year
            as reporting_epi_year,

        e.week_number
            as reporting_epi_week,

        e.epi_week_label
            as reporting_epi_week_label,

        e.start_of_week
            as reporting_epi_week_start_date,

        e.end_of_week
            as reporting_epi_week_end_date,

        e.start_week_day_name
            as reporting_epi_week_start_day_name,

        e.end_week_day_name
            as reporting_epi_week_end_day_name,

        e.current_epi_week_flag
            as reporting_current_epi_week_flag,

        e.current_epi_year_flag
            as reporting_current_epi_year_flag,

        ------------------------------------------------------------------
        -- Traveller screening location
        ------------------------------------------------------------------

        case
            when s.surveillance_pathway = 'TRAVELLER'
            then coalesce(
                p.point_of_entry,
                s.point_of_entry,
                'Unknown'
            )

            else null
        end as reporting_point_of_entry,

        ------------------------------------------------------------------
        -- Facility screening location
        ------------------------------------------------------------------

        case
            when s.surveillance_pathway = 'FACILITY'
            then coalesce(
                f.mfl_code,
                s.facility_code
            )

            else null
        end as reporting_mfl_code,

        case
            when s.surveillance_pathway = 'FACILITY'
            then coalesce(
                f.facility_name,
                'Unknown'
            )

            else null
        end as reporting_facility_name,

        case
            when s.surveillance_pathway = 'FACILITY'
            then f.province

            else null
        end as reporting_province,

        case
            when s.surveillance_pathway = 'FACILITY'
            then f.county

            else null
        end as reporting_county,

        case
            when s.surveillance_pathway = 'FACILITY'
            then f.subcounty

            else null
        end as reporting_subcounty,

        case
            when s.surveillance_pathway = 'FACILITY'
            then f.ward

            else null
        end as reporting_ward,

        ------------------------------------------------------------------
        -- Unified reporting location
        ------------------------------------------------------------------

        case
            when s.surveillance_pathway = 'TRAVELLER'
            then coalesce(
                p.point_of_entry,
                s.point_of_entry,
                'Unknown'
            )

            when s.surveillance_pathway = 'FACILITY'
            then coalesce(
                f.facility_name,
                s.facility_code,
                'Unknown'
            )

            else 'Unknown'
        end as reporting_location,

        case
            when s.surveillance_pathway = 'TRAVELLER'
                then 'POINT OF ENTRY'

            when s.surveillance_pathway = 'FACILITY'
                then 'HEALTH FACILITY'

            else coalesce(
                s.surveillance_pathway,
                'UNKNOWN'
            )
        end as reporting_location_type,

        ------------------------------------------------------------------
        -- Canonical reporting category
        ------------------------------------------------------------------

        case
            when nullif(trim(s.screening_outcome), '') is not null
                then upper(trim(s.screening_outcome))

            when coalesce(s.suspected_flag, 0) = 1
                then 'SUSPECTED'

            when coalesce(s.probable_flag, 0) = 1
                then 'PROBABLE'

            when coalesce(s.flagged_flag, 0) = 1
                then 'FLAGGED'

            when coalesce(s.normal_flag, 0) = 1
                then 'NORMAL'

            else 'UNKNOWN'
        end as reporting_screening_category,

        ------------------------------------------------------------------
        -- Reporting age group (same bands as report_contact_registration)
        ------------------------------------------------------------------

        case
            when s.age_at_screening is null
                then 'UNKNOWN'

            when s.age_at_screening < 1
                then '<1'

            when s.age_at_screening between 1 and 4
                then '1-4'

            when s.age_at_screening between 5 and 9
                then '5-9'

            when s.age_at_screening between 10 and 14
                then '10-14'

            when s.age_at_screening between 15 and 19
                then '15-19'

            when s.age_at_screening between 20 and 24
                then '20-24'

            when s.age_at_screening between 25 and 34
                then '25-34'

            when s.age_at_screening between 35 and 44
                then '35-44'

            when s.age_at_screening between 45 and 54
                then '45-54'

            when s.age_at_screening between 55 and 64
                then '55-64'

            when s.age_at_screening >= 65
                then '65+'

            else 'UNKNOWN'
        end as reporting_age_group,

        ------------------------------------------------------------------
        -- Reporting measures
        ------------------------------------------------------------------

        coalesce(
            s.screening_count,
            1
        )::integer as total_screening_count,

        case
            when coalesce(s.normal_flag, 0) = 1
                then coalesce(s.screening_count, 1)
            else 0
        end::integer as normal_screening_count,

        case
            when coalesce(s.flagged_flag, 0) = 1
                then coalesce(s.screening_count, 1)
            else 0
        end::integer as flagged_screening_count,

        case
            when coalesce(s.suspected_flag, 0) = 1
                then coalesce(s.screening_count, 1)
            else 0
        end::integer as suspected_screening_count,

        case
            when coalesce(s.probable_flag, 0) = 1
                then coalesce(s.screening_count, 1)
            else 0
        end::integer as probable_screening_count,

        case
            when upper(
                trim(
                    coalesce(
                        s.screening_outcome,
                        ''
                    )
                )
            ) = 'NOT RISK'
                then coalesce(s.screening_count, 1)

            else 0
        end::integer as not_risk_screening_count,

        case
            when coalesce(s.normal_flag, 0) = 0
             and coalesce(s.flagged_flag, 0) = 0
             and coalesce(s.suspected_flag, 0) = 0
             and coalesce(s.probable_flag, 0) = 0
             and upper(
                    trim(
                        coalesce(
                            s.screening_outcome,
                            ''
                        )
                    )
                 ) <> 'NOT RISK'
                then coalesce(s.screening_count, 1)

            else 0
        end::integer as unknown_screening_count

    from screening_source s

    ----------------------------------------------------------------------
    -- Calendar date
    ----------------------------------------------------------------------

    left join {{ ref('dim_date') }} d
        on s.screening_date_key = d.date_key

    ----------------------------------------------------------------------
    -- Epidemiological week
    ----------------------------------------------------------------------

    left join {{ ref('dim_epiweek') }} e
        on d.full_date between
            e.start_of_week
            and e.end_of_week

    ----------------------------------------------------------------------
    -- Point of entry
    ----------------------------------------------------------------------

    left join {{ ref('dim_point_of_entry') }} p
        on s.point_of_entry_key = p.point_of_entry_key

    ----------------------------------------------------------------------
    -- Facility/MFL
    ----------------------------------------------------------------------

    left join {{ ref('dim_facilitylist') }} f
        on s.facility_key = f.facility_key

)

select *
from screening_enriched