{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

with treatment_outcomes as (

    select
        *

    from {{ ref('fct_treatment_outcome') }}

),

prepared as (

    select
        o.*,

        /*
         * For deceased cases, outcome_date is the validated date of death.
         *
         * Other outcomes currently do not have a separate outcome date,
         * so the date on which the outcome was recorded is used for
         * reporting.
         */
        coalesce(
            o.outcome_date,
            cast(o.outcome_recorded_datetime as date)
        ) as reporting_date,

        /*
         * Calculate age using the most appropriate reporting date.
         */
        case
            when o.source_person_date_of_birth is not null
             and coalesce(
                    o.outcome_date,
                    cast(o.outcome_recorded_datetime as date)
                 ) is not null
                then extract(
                    year from age(
                        coalesce(
                            o.outcome_date,
                            cast(o.outcome_recorded_datetime as date)
                        ),
                        o.source_person_date_of_birth
                    )
                )::integer

            else null
        end as age_at_outcome

    from treatment_outcomes o

),

classified as (

    select
        p.*,

        /*
         * Calendar reporting hierarchy.
         */
        extract(
            year from p.reporting_date
        )::integer as reporting_year,

        extract(
            quarter from p.reporting_date
        )::integer as reporting_quarter,

        extract(
            month from p.reporting_date
        )::integer as reporting_month,

        trim(
            to_char(
                p.reporting_date,
                'Month'
            )
        ) as reporting_month_name,

        to_char(
            p.reporting_date,
            'YYYY-MM'
        ) as reporting_year_month,

        /*
         * Epidemiological reporting hierarchy.
         */
        extract(
            isoyear from p.reporting_date
        )::integer as reporting_epi_year,

        extract(
            week from p.reporting_date
        )::integer as reporting_epi_week,

        concat(
            extract(
                isoyear from p.reporting_date
            )::integer,
            '-W',
            lpad(
                extract(
                    week from p.reporting_date
                )::integer::text,
                2,
                '0'
            )
        ) as reporting_epi_week_label,

        /*
         * Reporting age groups.
         */
        case
            when p.age_at_outcome is null
                then 'UNKNOWN'

            when p.age_at_outcome < 0
                then 'UNKNOWN'

            when p.age_at_outcome < 1
                then '<1'

            when p.age_at_outcome between 1 and 4
                then '1-4'

            when p.age_at_outcome between 5 and 9
                then '5-9'

            when p.age_at_outcome between 10 and 14
                then '10-14'

            when p.age_at_outcome between 15 and 19
                then '15-19'

            when p.age_at_outcome between 20 and 24
                then '20-24'

            when p.age_at_outcome between 25 and 34
                then '25-34'

            when p.age_at_outcome between 35 and 44
                then '35-44'

            when p.age_at_outcome between 45 and 54
                then '45-54'

            when p.age_at_outcome between 55 and 64
                then '55-64'

            when p.age_at_outcome >= 65
                then '65+'

            else 'UNKNOWN'
        end as reporting_age_group

    from prepared p

),

final as (

    select
        /*
         * Fact and dimension keys.
         */
        treatment_outcome_key,
        outcome_date_key,
        outcome_recorded_date_key,
        date_of_birth_key,

        /*
         * Source metadata.
         */
        source_system,
        source_row_id,
        source_record_id,

        /*
         * Person attributes.
         */
        source_person_name,
        source_person_identifier,
        source_person_sex,
        source_person_date_of_birth,
        source_person_nationality,

        age_at_outcome,
        reporting_age_group,

        /*
         * Disease.
         */
        disease,

        /*
         * Reporting dates.
         */
        reporting_date,
        reporting_year,
        reporting_quarter,
        reporting_month,
        reporting_month_name,
        reporting_year_month,

        reporting_epi_year,
        reporting_epi_week,
        reporting_epi_week_label,

        outcome_date,
        date_of_death,
        outcome_recorded_datetime,

        /*
         * Case classification.
         */
        source_initial_classification,
        initial_classification,

        source_final_classification,
        final_classification,

        source_final_laboratory_result,
        final_laboratory_result,

        /*
         * Treatment outcome.
         */
        source_outcome,
        treatment_outcome,
        died_flag,
        outcome_validation_status,

        /*
         * Case and specimen linkage.
         */
        specimen_identifier,

        /*
         * Geography and facility.
         */
        reporting_county,
        reporting_subcounty,
        health_facility,

        latitude,
        longitude,

        /*
         * Review information.
         */
        checked_by,

        /*
         * Base additive measure.
         */
        treatment_outcome_count
            as total_treatment_outcome_count,

        /*
         * Outcome measures.
         */
        case
            when treatment_outcome = 'ALIVE'
                then treatment_outcome_count
            else 0
        end::integer as alive_count,

        case
            when treatment_outcome = 'RECOVERED'
                then treatment_outcome_count
            else 0
        end::integer as recovered_count,

        case
            when treatment_outcome = 'DECEASED'
                then treatment_outcome_count
            else 0
        end::integer as deceased_count,

        case
            when treatment_outcome = 'ON_TREATMENT'
                then treatment_outcome_count
            else 0
        end::integer as on_treatment_count,

        case
            when treatment_outcome = 'TRANSFERRED'
                then treatment_outcome_count
            else 0
        end::integer as transferred_count,

        case
            when treatment_outcome = 'LOST_TO_FOLLOW_UP'
                then treatment_outcome_count
            else 0
        end::integer as lost_to_follow_up_count,

        case
            when treatment_outcome = 'UNKNOWN'
                then treatment_outcome_count
            else 0
        end::integer as unknown_outcome_count,

        /*
         * Case-classification measures.
         */
        case
            when final_classification = 'CONFIRMED'
                then treatment_outcome_count
            else 0
        end::integer as confirmed_case_outcome_count,

        case
            when final_classification = 'PROBABLE'
                then treatment_outcome_count
            else 0
        end::integer as probable_case_outcome_count,

        case
            when final_classification = 'SUSPECTED'
                then treatment_outcome_count
            else 0
        end::integer as suspected_case_outcome_count,

        case
            when final_classification = 'DISCARDED'
                then treatment_outcome_count
            else 0
        end::integer as discarded_case_outcome_count,

        /*
         * Laboratory-result measures.
         */
        case
            when final_laboratory_result = 'POSITIVE'
                then treatment_outcome_count
            else 0
        end::integer as positive_lab_outcome_count,

        case
            when final_laboratory_result = 'NEGATIVE'
                then treatment_outcome_count
            else 0
        end::integer as negative_lab_outcome_count,

        /*
         * Outcome validation measures.
         */
        case
            when outcome_validation_status is null
                then treatment_outcome_count
            else 0
        end::integer as valid_outcome_count,

        case
            when outcome_validation_status is not null
                then treatment_outcome_count
            else 0
        end::integer as outcome_validation_issue_count,

        case
            when outcome_validation_status
                = 'DEATH_DATE_WITHOUT_CONFIRMED_CLASSIFICATION'
                then treatment_outcome_count
            else 0
        end::integer
            as death_without_confirmed_classification_count,

        case
            when outcome_validation_status
                = 'DEATH_DATE_WITH_NEGATIVE_LAB_RESULT'
                then treatment_outcome_count
            else 0
        end::integer
            as death_with_negative_lab_result_count,

        case
            when outcome_validation_status
                = 'DEATH_DATE_WITHOUT_POSITIVE_LAB_RESULT'
                then treatment_outcome_count
            else 0
        end::integer
            as death_without_positive_lab_result_count,

        /*
         * Completeness measures.
         */
        case
            when treatment_outcome <> 'UNKNOWN'
                then treatment_outcome_count
            else 0
        end::integer as known_outcome_count,

        case
            when reporting_date is not null
                then treatment_outcome_count
            else 0
        end::integer as outcome_date_available_count,

        case
            when disease is not null
                then treatment_outcome_count
            else 0
        end::integer as disease_available_count,

        /*
         * Audit metadata.
         */
        ingested_at,
        batch_id,
        source_file

    from classified

)

select *
from final