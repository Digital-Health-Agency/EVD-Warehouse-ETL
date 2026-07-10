{{ config(
    materialized = 'table'
) }}

with case_facts as (

    select
        case_date_key,
        location_key,
        facility_key,

        source_system,
        record_type,

        case_count,
        suspected_case_count,
        probable_case_count,
        confirmed_case_count,
        tested_case_count,
        sample_collected_count,
        recovered_case_count,
        death_count

    from {{ ref('fct_cases') }}

),

dated_cases as (

    select
        cases.location_key,
        cases.facility_key,

        cases.source_system,
        cases.record_type,

        case_date.full_date as case_date,

        cases.case_count,
        cases.suspected_case_count,
        cases.probable_case_count,
        cases.confirmed_case_count,
        cases.tested_case_count,
        cases.sample_collected_count,
        cases.recovered_case_count,
        cases.death_count

    from case_facts cases

    left join {{ ref('dim_date') }} case_date
        on cases.case_date_key = case_date.date_key

),

enriched as (

    select
        epiweek.epi_week_key,
        epiweek.epi_year,
        epiweek.week_number as epi_week,
        epiweek.epi_week_label,
        epiweek.start_of_week,
        epiweek.end_of_week,

        location.county,
        location.subcounty,
        location.ward,
        location.point_of_entry,

        facility.mfl_code,
        facility.facility_name,

        cases.source_system,
        cases.record_type,

        cases.case_count,
        cases.suspected_case_count,
        cases.probable_case_count,
        cases.confirmed_case_count,
        cases.tested_case_count,
        cases.sample_collected_count,
        cases.recovered_case_count,
        cases.death_count

    from dated_cases cases

    left join {{ ref('dim_epiweek') }} epiweek
        on cases.case_date between
            epiweek.start_of_week
            and epiweek.end_of_week

       and epiweek.epi_year <> -999

    left join {{ ref('dim_location') }} location
        on cases.location_key = location.location_key

    left join {{ ref('dim_facilitylist') }} facility
        on cases.facility_key = facility.facility_key

),

aggregated as (

    select
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
        record_type,

        sum(coalesce(case_count, 0)) as total_cases,
        sum(coalesce(suspected_case_count, 0)) as suspected_cases,
        sum(coalesce(probable_case_count, 0)) as probable_cases,
        sum(coalesce(confirmed_case_count, 0)) as confirmed_cases,
        sum(coalesce(tested_case_count, 0)) as tested_cases,
        sum(coalesce(sample_collected_count, 0)) as samples_collected,
        sum(coalesce(recovered_case_count, 0)) as recovered_cases,
        sum(coalesce(death_count, 0)) as deaths

    from enriched

    group by
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
        record_type

)

select
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
    record_type,

    total_cases,
    suspected_cases,
    probable_cases,
    confirmed_cases,
    tested_cases,
    samples_collected,
    recovered_cases,
    deaths,

    round(
        suspected_cases * 100.0
        / nullif(total_cases, 0),
        1
    ) as suspected_case_rate,

    round(
        probable_cases * 100.0
        / nullif(total_cases, 0),
        1
    ) as probable_case_rate,

    round(
        confirmed_cases * 100.0
        / nullif(total_cases, 0),
        1
    ) as confirmation_rate,

    round(
        tested_cases * 100.0
        / nullif(total_cases, 0),
        1
    ) as testing_rate,

    round(
        samples_collected * 100.0
        / nullif(total_cases, 0),
        1
    ) as sample_collection_rate,

    round(
        recovered_cases * 100.0
        / nullif(confirmed_cases, 0),
        1
    ) as recovery_rate,

    round(
        deaths * 100.0
        / nullif(confirmed_cases, 0),
        1
    ) as case_fatality_rate

from aggregated