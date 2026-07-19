{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

with screening_source as (

    select
        *

    from {{ ref('fct_screening') }}

),

screening_enriched as (

    select
        /*
         * Retain every column from fct_screening.
         */
        s.*,

        /*
         * Calendar hierarchy from dim_date.
         */
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

        /*
         * Epidemiological hierarchy from dim_epiweek.
         */
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

        /*
         * Point-of-entry attributes.
         *
         * These are populated for traveller screenings.
         * Other pathways may have no point of entry.
         */
        coalesce(
            p.point_of_entry,
            s.point_of_entry,
            'Unknown'
        ) as reporting_point_of_entry,

  
        /*
         * Canonical reporting category.
         *
         * The core classification logic is already handled
         * in fct_screening.
         */
        case
            when coalesce(s.suspected_flag, 0) = 1
                then 'SUSPECTED'

            when coalesce(s.probable_flag, 0) = 1
                then 'PROBABLE'

            when coalesce(s.normal_flag, 0) = 1
                then 'NORMAL'

            when coalesce(s.flagged_flag, 0) = 1
                then 'FLAGGED'

            when nullif(trim(s.screening_outcome), '') is not null
                then upper(trim(s.screening_outcome))

            else 'UNKNOWN'
        end as reporting_screening_category,

        /*
         * Reporting measures.
         */
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
            when coalesce(s.normal_flag, 0) = 0
             and coalesce(s.flagged_flag, 0) = 0
             and coalesce(s.suspected_flag, 0) = 0
             and coalesce(s.probable_flag, 0) = 0
                then coalesce(s.screening_count, 1)
            else 0
        end::integer as unknown_screening_count

    from screening_source s

    left join {{ ref('dim_date') }} d
        on s.screening_date_key = d.date_key

    left join {{ ref('dim_epiweek') }} e
        on d.full_date between
            e.start_of_week
            and e.end_of_week

    left join {{ ref('dim_point_of_entry') }} p
        on s.point_of_entry_key = p.point_of_entry_key

)

select *
from screening_enriched