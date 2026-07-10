
with laboratory_facts as (

    select
        facility_key,

        collection_date_key,
        result_date_key,

        source_system,

        specimen_type,
        loinc_code,
        test_name,
        code_text,
        component_code,
        unit,

        result_category,

        test_count,
        positive_test_count,
        negative_test_count,
        inconclusive_test_count,
        unknown_test_count,
        other_test_count

    from {{ ref('fct_lab_result') }}

),

dated_results as (

    select
        laboratory.facility_key,

        laboratory.source_system,

        collection_date.full_date as collection_date,
        result_date.full_date as result_date,

        laboratory.specimen_type,
        laboratory.loinc_code,
        laboratory.test_name,
        laboratory.code_text,
        laboratory.component_code,
        laboratory.unit,
        laboratory.result_category,

        laboratory.test_count,
        laboratory.positive_test_count,
        laboratory.negative_test_count,
        laboratory.inconclusive_test_count,
        laboratory.unknown_test_count,
        laboratory.other_test_count

    from laboratory_facts laboratory

    left join {{ ref('dim_date') }} collection_date
        on laboratory.collection_date_key = collection_date.date_key

    left join {{ ref('dim_date') }} result_date
        on laboratory.result_date_key = result_date.date_key

),

enriched as (

    select
        epiweek.epi_week_key,
        epiweek.epi_year,
        epiweek.week_number as epi_week,
        epiweek.epi_week_label,
        epiweek.start_of_week,
        epiweek.end_of_week,

        results.collection_date,
        results.result_date,

        extract(year from results.result_date)::integer
            as result_year,

        extract(month from results.result_date)::integer
            as result_month_number,

        trim(to_char(results.result_date, 'Month'))
            as result_month_name,

        facility.mfl_code,
        facility.facility_name,
        facility.county,
        facility.subcounty,

        results.source_system,

        results.specimen_type,
        results.loinc_code,
        results.test_name,
        results.code_text,
        results.component_code,
        results.unit,
        results.result_category,

        results.test_count,
        results.positive_test_count,
        results.negative_test_count,
        results.inconclusive_test_count,
        results.unknown_test_count,
        results.other_test_count

    from dated_results results

    left join {{ ref('dim_epiweek') }} epiweek
        on coalesce(
            results.result_date,
            results.collection_date
        ) between epiweek.start_of_week
          and epiweek.end_of_week

       and epiweek.epi_year <> -999

    left join {{ ref('dim_facilitylist') }} facility
        on results.facility_key = facility.facility_key

),

aggregated as (

    select
        epi_week_key,
        epi_year,
        epi_week,
        epi_week_label,
        start_of_week,
        end_of_week,

        result_year,
        result_month_number,
        result_month_name,

        mfl_code,
        facility_name,
        county,
        subcounty,

        source_system,

        specimen_type,
        loinc_code,
        test_name,
        code_text,
        component_code,
        unit,
        result_category,

        sum(coalesce(test_count, 0))
            as total_tests,

        sum(coalesce(positive_test_count, 0))
            as positive_tests,

        sum(coalesce(negative_test_count, 0))
            as negative_tests,

        sum(coalesce(inconclusive_test_count, 0))
            as inconclusive_tests,

        sum(coalesce(unknown_test_count, 0))
            as unknown_tests,

        sum(coalesce(other_test_count, 0))
            as other_tests

    from enriched

    group by
        epi_week_key,
        epi_year,
        epi_week,
        epi_week_label,
        start_of_week,
        end_of_week,

        result_year,
        result_month_number,
        result_month_name,

        mfl_code,
        facility_name,
        county,
        subcounty,

        source_system,

        specimen_type,
        loinc_code,
        test_name,
        code_text,
        component_code,
        unit,
        result_category

)

select
    epi_week_key,
    epi_year,
    epi_week,
    epi_week_label,

    start_of_week,
    end_of_week,

    result_year,
    result_month_number,
    result_month_name,

    county,
    subcounty,

    mfl_code,
    facility_name,

    source_system,

    specimen_type,
    loinc_code,
    test_name,
    code_text,
    component_code,
    unit,
    result_category,

    total_tests,
    positive_tests,
    negative_tests,
    inconclusive_tests,
    unknown_tests,
    other_tests,

    round(
        positive_tests * 100.0
        / nullif(
            positive_tests
            + negative_tests
            + inconclusive_tests,
            0
        ),
        1
    ) as positivity_rate,

    round(
        negative_tests * 100.0
        / nullif(total_tests, 0),
        1
    ) as negative_rate,

    round(
        inconclusive_tests * 100.0
        / nullif(total_tests, 0),
        1
    ) as inconclusive_rate,

    round(
        unknown_tests * 100.0
        / nullif(total_tests, 0),
        1
    ) as unknown_result_rate,

    round(
        (
            positive_tests
            + negative_tests
            + inconclusive_tests
        ) * 100.0
        / nullif(total_tests, 0),
        1
    ) as result_completion_rate

from aggregated