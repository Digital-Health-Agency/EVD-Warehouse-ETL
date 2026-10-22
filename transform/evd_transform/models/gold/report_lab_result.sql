{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

with lab_result_source as (

    select
        *,

        /*
         * The LIMS case (per-patient episode) this test belongs to,
         * normalised the same way the case models link to LIMS.
         */
        {{ lims_link_key(
            "coalesce(case_identifier, subject_identifier, specimen_identifier)"
        ) }} as lab_case_key

    from {{ ref('fct_lab_result') }}

),

/*
 * Test events are the grain of this mart. Laboratory reporting counts
 * tests; surveillance reporting counts people. One case can be sampled
 * more than once and one specimen can be tested more than once, so the
 * first test per case and per specimen is ranked here (within a test
 * code) to expose case- and specimen-grain counters alongside the
 * test-event counters.
 */
lab_result_ranked as (

    select
        *,

        row_number() over (
            partition by lab_case_key, test_code
            order by result_datetime nulls last, collection_date, source_row_id
        ) as lab_case_test_rank,

        case
            when result_category = 'POSITIVE'
                then row_number() over (
                    partition by lab_case_key, test_code, result_category
                    order by result_datetime nulls last, collection_date, source_row_id
                )
            else null
        end as lab_case_positive_rank,

        row_number() over (
            partition by specimen_identifier, test_code
            order by result_datetime nulls last, collection_date, source_row_id
        ) as specimen_test_rank,

        case
            when result_category = 'POSITIVE'
                then row_number() over (
                    partition by specimen_identifier, test_code, result_category
                    order by result_datetime nulls last, collection_date, source_row_id
                )
            else null
        end as specimen_positive_rank

    from lab_result_source

),

lab_result_enriched as (

    select
        /*
         * Retain all columns from fct_lab_result.
         */
        r.*,

        /*
         * Collection-date calendar hierarchy.
         */
        collection_date.full_date
            as reporting_collection_date,

        collection_date.year
            as reporting_collection_year,

        collection_date.quarter
            as reporting_collection_quarter,

        collection_date.month
            as reporting_collection_month_number,

        collection_date.month_name
            as reporting_collection_month_name,

        /*
         * Collection-date epidemiological hierarchy.
         */
        collection_epiweek.epi_week_key
            as reporting_collection_epi_week_key,

        collection_epiweek.epi_year
            as reporting_collection_epi_year,

        collection_epiweek.week_number
            as reporting_collection_epi_week,

        collection_epiweek.epi_week_label
            as reporting_collection_epi_week_label,

        collection_epiweek.start_of_week
            as reporting_collection_epi_week_start_date,

        collection_epiweek.end_of_week
            as reporting_collection_epi_week_end_date,

        /*
         * Result-date calendar hierarchy.
         */
        result_date.full_date
            as reporting_result_date,

        result_date.year
            as reporting_result_year,

        result_date.quarter
            as reporting_result_quarter,

        result_date.month
            as reporting_result_month_number,

        result_date.month_name
            as reporting_result_month_name,

        /*
         * Result-date epidemiological hierarchy.
         */
        result_epiweek.epi_week_key
            as reporting_result_epi_week_key,

        result_epiweek.epi_year
            as reporting_result_epi_year,

        result_epiweek.week_number
            as reporting_result_epi_week,

        result_epiweek.epi_week_label
            as reporting_result_epi_week_label,

        result_epiweek.start_of_week
            as reporting_result_epi_week_start_date,

        result_epiweek.end_of_week
            as reporting_result_epi_week_end_date,

        result_epiweek.current_epi_week_flag
            as reporting_current_epi_week_flag,

        result_epiweek.current_epi_year_flag
            as reporting_current_epi_year_flag,

        /*
         * Canonical requesting-facility identifiers.
         *
         * Existing column retained unchanged.
         */
        requesting_facility.mfl_code
            as reporting_requesting_facility_mfl,

        /*
         * Additional requesting-facility attributes.
         *
         * These are appended without changing any
         * existing report columns.
         */
        requesting_facility.facility_name
            as reporting_requesting_facility_name,

        requesting_facility.province
            as reporting_requesting_facility_province,

        requesting_facility.county
            as reporting_requesting_facility_county,

        requesting_facility.subcounty
            as reporting_requesting_facility_subcounty,

        requesting_facility.ward
            as reporting_requesting_facility_ward,

        /*
         * Canonical testing-laboratory identifiers.
         */
        testing_laboratory.laboratory_code
            as reporting_testing_laboratory_code,

        testing_laboratory.laboratory_name
            as reporting_testing_laboratory_name,

        /*
         * Turnaround time from specimen collection to result.
         */
        case
            when r.collection_date is not null
             and r.result_datetime is not null
             and cast(r.result_datetime as date) >= r.collection_date
                then (
                    cast(r.result_datetime as date)
                    - r.collection_date
                )::integer

            else null
        end as turnaround_time_days,

        case
            when r.collection_date is not null
             and r.result_datetime is not null
             and r.result_datetime >= r.collection_date::timestamp
                then round(
                    (
                        extract(
                            epoch from (
                                r.result_datetime
                                - r.collection_date::timestamp
                            )
                        ) / 3600
                    )::numeric,
                    2
                )

            else null
        end as turnaround_time_hours,

        /*
         * Turnaround-time reporting band.
         */
        case
            when r.collection_date is null
              or r.result_datetime is null
                then 'UNKNOWN'

            when cast(r.result_datetime as date) < r.collection_date
                then 'INVALID'

            when (
                cast(r.result_datetime as date)
                - r.collection_date
            ) = 0
                then 'SAME DAY'

            when (
                cast(r.result_datetime as date)
                - r.collection_date
            ) = 1
                then '1 DAY'

            when (
                cast(r.result_datetime as date)
                - r.collection_date
            ) between 2 and 3
                then '2-3 DAYS'

            when (
                cast(r.result_datetime as date)
                - r.collection_date
            ) between 4 and 7
                then '4-7 DAYS'

            else 'OVER 7 DAYS'
        end as turnaround_time_band,

        /*
         * General laboratory test measure.
         */
        coalesce(
            r.test_count,
            1
        )::integer as total_test_count,

        /*
         * Result-category measures.
         */
        case
            when r.result_category = 'POSITIVE'
                then coalesce(r.test_count, 1)
            else 0
        end::integer as positive_test_count,

        case
            when r.result_category = 'NEGATIVE'
                then coalesce(r.test_count, 1)
            else 0
        end::integer as negative_test_count,

        case
            when r.result_category = 'INCONCLUSIVE'
                then coalesce(r.test_count, 1)
            else 0
        end::integer as inconclusive_test_count,

        case
            when r.result_category = 'OTHER'
                then coalesce(r.test_count, 1)
            else 0
        end::integer as other_result_count,

        case
            when r.result_category = 'UNKNOWN'
              or r.result_category is null
                then coalesce(r.test_count, 1)
            else 0
        end::integer as unknown_result_count,

        /*
         * Result availability measures.
         */
        case
            when r.result_category in (
                'POSITIVE',
                'NEGATIVE',
                'INCONCLUSIVE',
                'OTHER'
            )
                then coalesce(r.test_count, 1)
            else 0
        end::integer as result_available_count,

        case
            when r.result_category = 'UNKNOWN'
              or r.result_category is null
                then coalesce(r.test_count, 1)
            else 0
        end::integer as result_not_available_count,

        /*
         * Specimen and case-linkage measures.
         */
        case
            when nullif(trim(r.specimen_identifier), '') is not null
                then coalesce(r.test_count, 1)
            else 0
        end::integer as specimen_identified_count,

        case
            when nullif(trim(r.case_identifier), '') is not null
                then coalesce(r.test_count, 1)
            else 0
        end::integer as case_linked_test_count,

        case
            when r.requesting_facility_key is not null
                then coalesce(r.test_count, 1)
            else 0
        end::integer as requesting_facility_matched_count,

        case
            when r.testing_laboratory_key is not null
                then coalesce(r.test_count, 1)
            else 0
        end::integer as testing_laboratory_matched_count,

        /*
         * Case-grain measures: 1 on the first test (or first positive
         * test) of each LIMS case for this test code, 0 otherwise.
         * sum(positive_case_count) where test_code = '86518-8' equals
         * the distinct laboratory-confirmed cases and must reconcile
         * to sum(final_confirmed_count) in report_case_investigation.
         */
        case
            when r.lab_case_key is not null
             and r.lab_case_test_rank = 1
                then 1
            else 0
        end::integer as tested_case_count,

        case
            when r.lab_case_key is not null
             and r.lab_case_positive_rank = 1
                then 1
            else 0
        end::integer as positive_case_count,

        /*
         * Specimen-grain measures: 1 on the first test (or first
         * positive test) of each specimen for this test code.
         */
        case
            when r.specimen_identifier is not null
             and r.specimen_test_rank = 1
                then 1
            else 0
        end::integer as tested_specimen_count,

        case
            when r.specimen_identifier is not null
             and r.specimen_positive_rank = 1
                then 1
            else 0
        end::integer as positive_specimen_count,

        /*
         * Repeat flags for the linelist.
         */
        case
            when r.lab_case_key is not null
             and r.lab_case_test_rank > 1
                then 1
            else 0
        end::integer as repeat_case_test_flag,

        case
            when r.specimen_identifier is not null
             and r.specimen_test_rank > 1
                then 1
            else 0
        end::integer as repeat_specimen_test_flag

    from lab_result_ranked r

    left join {{ ref('dim_date') }} collection_date
        on r.collection_date_key = collection_date.date_key

    left join {{ ref('dim_epiweek') }} collection_epiweek
        on collection_date.full_date
            between collection_epiweek.start_of_week
                and collection_epiweek.end_of_week

    left join {{ ref('dim_date') }} result_date
        on r.result_date_key = result_date.date_key

    left join {{ ref('dim_epiweek') }} result_epiweek
        on result_date.full_date
            between result_epiweek.start_of_week
                and result_epiweek.end_of_week

    left join {{ ref('dim_facilitylist') }} requesting_facility
        on r.requesting_facility_key
         = requesting_facility.facility_key

    left join {{ ref('dim_laboratory') }} testing_laboratory
        on r.testing_laboratory_key
         = testing_laboratory.laboratory_key

)

select *
from lab_result_enriched