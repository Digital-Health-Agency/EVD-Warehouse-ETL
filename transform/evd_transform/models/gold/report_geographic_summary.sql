{{ config(
    materialized = 'table'
) }}

with case_activity as (

    select
        case_date.full_date as activity_date,

        location.county,
        location.subcounty,
        location.ward,
        location.point_of_entry,

        facility.mfl_code,
        facility.facility_name,

        sum(coalesce(cases.case_count, 0))
            as total_cases,

        sum(coalesce(cases.confirmed_case_count, 0))
            as confirmed_cases,

        sum(coalesce(cases.tested_case_count, 0))
            as tested_cases,

        sum(coalesce(cases.death_count, 0))
            as deaths,

        0::bigint as total_screening_records,
        0::bigint as total_screened,
        0::bigint as suspected_screenings,
        0::bigint as confirmed_screenings,
        0::bigint as tested_screenings,

        0::bigint as laboratory_tests,
        0::bigint as positive_tests,
        0::bigint as negative_tests,
        0::bigint as inconclusive_tests

    from {{ ref('fct_cases') }} cases

    left join {{ ref('dim_date') }} case_date
        on cases.case_date_key = case_date.date_key

    left join {{ ref('dim_location') }} location
        on cases.location_key = location.location_key

    left join {{ ref('dim_facilitylist') }} facility
        on cases.facility_key = facility.facility_key

    group by
        case_date.full_date,

        location.county,
        location.subcounty,
        location.ward,
        location.point_of_entry,

        facility.mfl_code,
        facility.facility_name

),

screening_activity as (

    select
        screening.screening_date as activity_date,

        location.county,
        location.subcounty,
        location.ward,
        location.point_of_entry,

        facility.mfl_code,
        facility.facility_name,

        0::bigint as total_cases,
        0::bigint as confirmed_cases,
        0::bigint as tested_cases,
        0::bigint as deaths,

        sum(coalesce(screening.screening_count, 0))
            as total_screening_records,

        sum(coalesce(screening.screened_count, 0))
            as total_screened,

        sum(coalesce(screening.suspected_count, 0))
            as suspected_screenings,

        sum(coalesce(screening.confirmed_count, 0))
            as confirmed_screenings,

        sum(coalesce(screening.tested_count, 0))
            as tested_screenings,

        0::bigint as laboratory_tests,
        0::bigint as positive_tests,
        0::bigint as negative_tests,
        0::bigint as inconclusive_tests

    from {{ ref('fct_screening') }} screening

    left join {{ ref('dim_location') }} location
        on screening.location_key = location.location_key

    left join {{ ref('dim_facilitylist') }} facility
        on screening.facility_key = facility.facility_key

    group by
        screening.screening_date,

        location.county,
        location.subcounty,
        location.ward,
        location.point_of_entry,

        facility.mfl_code,
        facility.facility_name

),

laboratory_activity as (

    select
        coalesce(
            result_date.full_date,
            collection_date.full_date
        ) as activity_date,

        facility.county,
        facility.subcounty,

        null::text as ward,
        null::text as point_of_entry,

        facility.mfl_code,
        facility.facility_name,

        0::bigint as total_cases,
        0::bigint as confirmed_cases,
        0::bigint as tested_cases,
        0::bigint as deaths,

        0::bigint as total_screening_records,
        0::bigint as total_screened,
        0::bigint as suspected_screenings,
        0::bigint as confirmed_screenings,
        0::bigint as tested_screenings,

        sum(coalesce(laboratory.test_count, 0))
            as laboratory_tests,

        sum(coalesce(laboratory.positive_test_count, 0))
            as positive_tests,

        sum(coalesce(laboratory.negative_test_count, 0))
            as negative_tests,

        sum(coalesce(laboratory.inconclusive_test_count, 0))
            as inconclusive_tests

    from {{ ref('fct_lab_result') }} laboratory

    left join {{ ref('dim_date') }} result_date
        on laboratory.result_date_key = result_date.date_key

    left join {{ ref('dim_date') }} collection_date
        on laboratory.collection_date_key = collection_date.date_key

    left join {{ ref('dim_facilitylist') }} facility
        on laboratory.facility_key = facility.facility_key

    group by
        coalesce(
            result_date.full_date,
            collection_date.full_date
        ),

        facility.county,
        facility.subcounty,
        facility.mfl_code,
        facility.facility_name

),

combined_activity as (

    select * from case_activity

    union all

    select * from screening_activity

    union all

    select * from laboratory_activity

),

enriched as (

    select
        activity.activity_date,

        extract(year from activity.activity_date)::integer
            as activity_year,

        extract(month from activity.activity_date)::integer
            as activity_month_number,

        trim(to_char(activity.activity_date, 'Month'))
            as activity_month_name,

        epiweek.epi_week_key,
        epiweek.epi_year,
        epiweek.week_number as epi_week,
        epiweek.epi_week_label,
        epiweek.start_of_week,
        epiweek.end_of_week,

        activity.county,
        activity.subcounty,
        activity.ward,
        activity.point_of_entry,

        activity.mfl_code,
        activity.facility_name,

        activity.total_cases,
        activity.confirmed_cases,
        activity.tested_cases,
        activity.deaths,

        activity.total_screening_records,
        activity.total_screened,
        activity.suspected_screenings,
        activity.confirmed_screenings,
        activity.tested_screenings,

        activity.laboratory_tests,
        activity.positive_tests,
        activity.negative_tests,
        activity.inconclusive_tests

    from combined_activity activity

    left join {{ ref('dim_epiweek') }} epiweek
        on activity.activity_date between
            epiweek.start_of_week
            and epiweek.end_of_week

       and epiweek.epi_year <> -999

),

aggregated as (

    select
        activity_date,
        activity_year,
        activity_month_number,
        activity_month_name,

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

        sum(total_cases) as total_cases,
        sum(confirmed_cases) as confirmed_cases,
        sum(tested_cases) as tested_cases,
        sum(deaths) as deaths,

        sum(total_screening_records)
            as total_screening_records,

        sum(total_screened)
            as total_screened,

        sum(suspected_screenings)
            as suspected_screenings,

        sum(confirmed_screenings)
            as confirmed_screenings,

        sum(tested_screenings)
            as tested_screenings,

        sum(laboratory_tests)
            as laboratory_tests,

        sum(positive_tests)
            as positive_tests,

        sum(negative_tests)
            as negative_tests,

        sum(inconclusive_tests)
            as inconclusive_tests

    from enriched

    group by
        activity_date,
        activity_year,
        activity_month_number,
        activity_month_name,

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
        facility_name

)

select
    activity_date,
    activity_year,
    activity_month_number,
    activity_month_name,

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

    total_cases,
    confirmed_cases,
    tested_cases,
    deaths,

    total_screening_records,
    total_screened,
    suspected_screenings,
    confirmed_screenings,
    tested_screenings,

    laboratory_tests,
    positive_tests,
    negative_tests,
    inconclusive_tests,

    round(
        confirmed_cases * 100.0
        / nullif(total_cases, 0),
        1
    ) as confirmation_rate,

    round(
        tested_cases * 100.0
        / nullif(total_cases, 0),
        1
    ) as case_testing_rate,

    round(
        deaths * 100.0
        / nullif(confirmed_cases, 0),
        1
    ) as case_fatality_rate,

    round(
        suspected_screenings * 100.0
        / nullif(total_screened, 0),
        1
    ) as suspected_screening_rate,

    round(
        tested_screenings * 100.0
        / nullif(suspected_screenings, 0),
        1
    ) as screening_testing_rate,

    round(
        confirmed_screenings * 100.0
        / nullif(tested_screenings, 0),
        1
    ) as screening_positivity_rate,

    round(
        positive_tests * 100.0
        / nullif(
            positive_tests
            + negative_tests
            + inconclusive_tests,
            0
        ),
        1
    ) as laboratory_positivity_rate

from aggregated