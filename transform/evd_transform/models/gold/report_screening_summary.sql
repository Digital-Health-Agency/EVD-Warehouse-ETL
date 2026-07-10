{{ config(
    materialized = 'table'
) }}

with screening_facts as (

    select
        location_key,
        facility_key,

        source_system,

        screening_date,
        screening_datetime,

        classification,
        test_result,

        screening_count,
        screened_count,
        suspected_count,
        confirmed_count,
        tested_count

    from {{ ref('fct_screening') }}

),

enriched as (

    select
        epiweek.epi_week_key,
        epiweek.epi_year,
        epiweek.week_number as epi_week,
        epiweek.epi_week_label,
        epiweek.start_of_week,
        epiweek.end_of_week,

        screenings.screening_date,

        extract(year from screenings.screening_date)::integer
            as screening_year,

        extract(month from screenings.screening_date)::integer
            as screening_month_number,

        trim(to_char(screenings.screening_date, 'Month'))
            as screening_month_name,

        location.county,
        location.subcounty,
        location.ward,
        location.point_of_entry,

        facility.mfl_code,
        facility.facility_name,

        screenings.source_system,
        screenings.classification,
        screenings.test_result,

        screenings.screening_count,
        screenings.screened_count,
        screenings.suspected_count,
        screenings.confirmed_count,
        screenings.tested_count

    from screening_facts screenings

    left join {{ ref('dim_epiweek') }} epiweek
        on screenings.screening_date between
            epiweek.start_of_week
            and epiweek.end_of_week

       and epiweek.epi_year <> -999

    left join {{ ref('dim_location') }} location
        on screenings.location_key = location.location_key

    left join {{ ref('dim_facilitylist') }} facility
        on screenings.facility_key = facility.facility_key

),

aggregated as (

    select
        screening_date,
        screening_year,
        screening_month_number,
        screening_month_name,

        epi_week_key,
        epi_year,
        epi_week,
        epi_week_label,
        start_of_week,
        end_of_week,

        county,
        subcounty,
        ward,
        point_of_entry,

        mfl_code,
        facility_name,

        source_system,
        classification,
        test_result,

        sum(coalesce(screening_count, 0))
            as total_screening_records,

        sum(coalesce(screened_count, 0))
            as total_screened,

        sum(coalesce(suspected_count, 0))
            as total_suspected,

        sum(coalesce(confirmed_count, 0))
            as total_confirmed,

        sum(coalesce(tested_count, 0))
            as total_tested

    from enriched

    group by
        screening_date,
        screening_year,
        screening_month_number,
        screening_month_name,

        epi_week_key,
        epi_year,
        epi_week,
        epi_week_label,
        start_of_week,
        end_of_week,

        county,
        subcounty,
        ward,
        point_of_entry,

        mfl_code,
        facility_name,

        source_system,
        classification,
        test_result

)

select
    screening_date,
    screening_year,
    screening_month_number,
    screening_month_name,

    epi_week_key,
    epi_year,
    epi_week,
    epi_week_label,
    start_of_week,
    end_of_week,

    county,
    subcounty,
    ward,
    point_of_entry,

    mfl_code,
    facility_name,

    source_system,
    classification,
    test_result,

    total_screening_records,
    total_screened,
    total_suspected,
    total_confirmed,
    total_tested,

    round(
        total_screened * 100.0
        / nullif(total_screening_records, 0),
        1
    ) as screening_completion_rate,

    round(
        total_suspected * 100.0
        / nullif(total_screened, 0),
        1
    ) as suspected_screening_rate,

    round(
        total_tested * 100.0
        / nullif(total_suspected, 0),
        1
    ) as testing_rate,

    round(
        total_confirmed * 100.0
        / nullif(total_tested, 0),
        1
    ) as positivity_rate,

    round(
        total_confirmed * 100.0
        / nullif(total_screened, 0),
        1
    ) as confirmed_screening_rate

from aggregated