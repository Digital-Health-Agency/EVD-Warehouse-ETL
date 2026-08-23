{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

with case_investigation_source as (

    select *
    from {{ ref('fct_case_investigation') }}

),

lab_results as (

    select
        specimen_identifier,
        case_identifier,
        subject_identifier,
        component_code,
        test_name,
        result_category,
        result_value,
        result_datetime

    from {{ ref('fct_lab_result') }}

),

lab_results_by_specimen as (

    select
        specimen_identifier,

        /*
         * Latest relevant laboratory result datetime.
         */
        max(
            case
                when trim(component_code) = '86518-8'
                    then result_datetime
                else null
            end
        ) as latest_lab_result_datetime,

        /*
         * Authoritative laboratory result for the
         * linked EVD specimen.
         *
         * Priority:
         * POSITIVE > NEGATIVE > INCONCLUSIVE > OTHER
         */
        case
            when max(
                case
                    when trim(component_code) = '86518-8'
                     and result_category = 'POSITIVE'
                        then 1
                    else 0
                end
            ) = 1
                then 'POSITIVE'

            when max(
                case
                    when trim(component_code) = '86518-8'
                     and result_category = 'NEGATIVE'
                        then 1
                    else 0
                end
            ) = 1
                then 'NEGATIVE'

            when max(
                case
                    when trim(component_code) = '86518-8'
                     and result_category = 'INCONCLUSIVE'
                        then 1
                    else 0
                end
            ) = 1
                then 'INCONCLUSIVE'

            when max(
                case
                    when trim(component_code) = '86518-8'
                     and result_category = 'OTHER'
                        then 1
                    else 0
                end
            ) = 1
                then 'OTHER'

            else null
        end as lims_result_category

    from lab_results

    where nullif(trim(specimen_identifier), '') is not null

    group by specimen_identifier

),

case_investigation_resolved as (

    select
        c.*,

        lab.lims_result_category,
        lab.latest_lab_result_datetime,

        /*
         * Final laboratory truth.
         *
         * Do NOT fall back to the ADAM source result.
         * No linked LIMS result = UNKNOWN.
         */
        coalesce(
            lab.lims_result_category,
            'UNKNOWN'
        ) as final_laboratory_result,

        /*
         * Only a linked POSITIVE LIMS result
         * confirms the case.
         */
        case
            when lab.lims_result_category = 'POSITIVE'
                then 1
            else 0
        end::integer as confirmed_case_flag,

        case
            when lab.lims_result_category = 'NEGATIVE'
                then 1
            else 0
        end::integer as negative_lab_flag,

        case
            when lab.lims_result_category is not null
                then 1
            else 0
        end::integer as lab_matched_flag,

        /*
         * Authoritative reporting classification.
         *
         * POSITIVE LIMS  -> CONFIRMED
         * NEGATIVE LIMS  -> DISCARDED
         *
         * A source CONFIRMED classification without
         * positive LIMS confirmation is NOT treated
         * as confirmed.
         */
        case
            when lab.lims_result_category = 'POSITIVE'
                then 'CONFIRMED'

            when lab.lims_result_category = 'NEGATIVE'
                then 'DISCARDED'

            when lab.lims_result_category in (
                'INCONCLUSIVE',
                'OTHER'
            )
                then case
                    when c.final_classification in (
                        'SUSPECTED',
                        'PROBABLE'
                    )
                        then c.final_classification
                    else 'UNKNOWN'
                end

            when c.final_classification = 'DISCARDED'
                then 'DISCARDED'

            when c.final_classification = 'SUSPECTED'
                then 'SUSPECTED'

            when c.final_classification = 'PROBABLE'
                then 'PROBABLE'

            /*
             * Source says CONFIRMED but there is no
             * LIMS confirmation.
             */
            when c.final_classification = 'CONFIRMED'
                then case
                    when c.initial_classification in (
                        'SUSPECTED',
                        'PROBABLE'
                    )
                        then c.initial_classification
                    else 'UNKNOWN'
                end

            when c.final_classification = 'UNKNOWN'
              or c.final_classification is null
                then case
                    when c.initial_classification in (
                        'SUSPECTED',
                        'PROBABLE',
                        'DISCARDED'
                    )
                        then c.initial_classification
                    else 'UNKNOWN'
                end

            else 'UNKNOWN'
        end as resolved_case_classification,

        /*
         * Reconcile ADAM source laboratory result
         * against the linked authoritative LIMS result.
         */
        case
            when lab.lims_result_category is null
                then 'NO LAB MATCH'

            when nullif(
                trim(c.source_final_laboratory_result),
                ''
            ) is null
                then 'NO SOURCE RESULT'

            when lab.lims_result_category = 'POSITIVE'
             and lower(trim(c.source_final_laboratory_result)) in (
                 'positive',
                 'detected',
                 'reactive',
                 'present'
             )
                then 'MATCH'

            when lab.lims_result_category = 'NEGATIVE'
             and lower(trim(c.source_final_laboratory_result)) in (
                 'negative',
                 'not detected',
                 'non-reactive',
                 'non reactive',
                 'absent'
             )
                then 'MATCH'

            when lab.lims_result_category = 'INCONCLUSIVE'
             and lower(trim(c.source_final_laboratory_result)) in (
                 'indeterminate',
                 'inconclusive',
                 'invalid',
                 'equivocal'
             )
                then 'MATCH'

            else 'MISMATCH'
        end as laboratory_result_reconciliation

    from case_investigation_source c

    left join lab_results_by_specimen lab
        on trim(c.specimen_identifier)
         = trim(lab.specimen_identifier)

),

case_investigation_enriched as (

    select
        /*
         * Retain every fact column.
         */
        c.*,

        /*
         * Investigation calendar hierarchy.
         */
        investigation_date.full_date
            as reporting_date,

        investigation_date.year
            as reporting_year,

        investigation_date.quarter
            as reporting_quarter,

        investigation_date.month
            as reporting_month_number,

        investigation_date.month_name
            as reporting_month_name,

        /*
         * Epidemiological hierarchy.
         */
        epiweek.epi_week_key
            as reporting_epi_week_key,

        epiweek.epi_year
            as reporting_epi_year,

        epiweek.week_number
            as reporting_epi_week,

        epiweek.epi_week_label
            as reporting_epi_week_label,

        epiweek.start_of_week
            as reporting_epi_week_start_date,

        epiweek.end_of_week
            as reporting_epi_week_end_date,

        epiweek.start_week_day_name
            as reporting_epi_week_start_day_name,

        epiweek.end_week_day_name
            as reporting_epi_week_end_day_name,

        epiweek.current_epi_week_flag
            as reporting_current_epi_week_flag,

        epiweek.current_epi_year_flag
            as reporting_current_epi_year_flag,

        /*
         * Date-of-birth calendar attributes.
         */
        birth_date.full_date
            as reporting_date_of_birth,

        birth_date.year
            as reporting_birth_year,

        birth_date.month
            as reporting_birth_month_number,

        birth_date.month_name
            as reporting_birth_month_name,

        /*
         * Age at investigation.
         */
        case
            when c.source_person_date_of_birth is not null
             and c.investigation_date is not null
             and c.source_person_date_of_birth <= c.investigation_date
                then extract(
                    year from age(
                        c.investigation_date,
                        c.source_person_date_of_birth
                    )
                )::integer
            else null
        end as age_at_investigation,

        /*
         * Reporting age group.
         */
        case
            when c.source_person_date_of_birth is null
              or c.investigation_date is null
                then 'UNKNOWN'

            when c.source_person_date_of_birth > c.investigation_date
                then 'INVALID'

            when extract(
                year from age(
                    c.investigation_date,
                    c.source_person_date_of_birth
                )
            ) < 5
                then '0-4'

            when extract(
                year from age(
                    c.investigation_date,
                    c.source_person_date_of_birth
                )
            ) between 5 and 14
                then '5-14'

            when extract(
                year from age(
                    c.investigation_date,
                    c.source_person_date_of_birth
                )
            ) between 15 and 24
                then '15-24'

            when extract(
                year from age(
                    c.investigation_date,
                    c.source_person_date_of_birth
                )
            ) between 25 and 34
                then '25-34'

            when extract(
                year from age(
                    c.investigation_date,
                    c.source_person_date_of_birth
                )
            ) between 35 and 44
                then '35-44'

            when extract(
                year from age(
                    c.investigation_date,
                    c.source_person_date_of_birth
                )
            ) between 45 and 54
                then '45-54'

            when extract(
                year from age(
                    c.investigation_date,
                    c.source_person_date_of_birth
                )
            ) between 55 and 64
                then '55-64'

            else '65+'
        end as reporting_age_group,

        /*
         * Existing dashboard column name retained.
         *
         * It now uses the resolved classification rather
         * than accepting an unverified source CONFIRMED.
         */
        c.resolved_case_classification
            as reporting_case_classification,

        /*
         * Investigation status aligned to the resolved
         * classification.
         */
        case
            when c.resolved_case_classification in (
                'CONFIRMED',
                'DISCARDED'
            )
                then 'CONCLUDED'
            else 'OPEN'
        end as investigation_status,

        /*
         * General investigation measure.
         */
        coalesce(
            c.investigation_count,
            1
        )::integer as total_investigation_count,

        /*
         * Initial classification measures.
         *
         * These intentionally retain the source initial
         * classifications for historical/source reporting.
         */
        case
            when c.initial_classification = 'SUSPECTED'
                then coalesce(c.investigation_count, 1)
            else 0
        end::integer as initial_suspected_count,

        case
            when c.initial_classification = 'PROBABLE'
                then coalesce(c.investigation_count, 1)
            else 0
        end::integer as initial_probable_count,

        case
            when c.initial_classification = 'CONFIRMED'
                then coalesce(c.investigation_count, 1)
            else 0
        end::integer as initial_confirmed_count,

        case
            when c.initial_classification = 'DISCARDED'
                then coalesce(c.investigation_count, 1)
            else 0
        end::integer as initial_discarded_count,

        case
            when c.initial_classification = 'UNKNOWN'
              or c.initial_classification is null
                then coalesce(c.investigation_count, 1)
            else 0
        end::integer as initial_unknown_count,

        /*
         * Final classification measures.
         *
         * These now use the authoritative resolved
         * classification.
         */
        case
            when c.resolved_case_classification = 'SUSPECTED'
                then coalesce(c.investigation_count, 1)
            else 0
        end::integer as final_suspected_count,

        case
            when c.resolved_case_classification = 'PROBABLE'
                then coalesce(c.investigation_count, 1)
            else 0
        end::integer as final_probable_count,

        case
            when c.resolved_case_classification = 'CONFIRMED'
                then coalesce(c.investigation_count, 1)
            else 0
        end::integer as final_confirmed_count,

        case
            when c.resolved_case_classification = 'DISCARDED'
                then coalesce(c.investigation_count, 1)
            else 0
        end::integer as final_discarded_count,

        case
            when c.resolved_case_classification = 'UNKNOWN'
                then coalesce(c.investigation_count, 1)
            else 0
        end::integer as final_unknown_count,

        /*
         * Investigation status measures.
         */
        case
            when c.resolved_case_classification in (
                'CONFIRMED',
                'DISCARDED'
            )
                then coalesce(c.investigation_count, 1)
            else 0
        end::integer as concluded_investigation_count,

        case
            when c.resolved_case_classification not in (
                'CONFIRMED',
                'DISCARDED'
            )
                then coalesce(c.investigation_count, 1)
            else 0
        end::integer as open_investigation_count,

        /*
         * Sample collection measures.
         */
        case
            when coalesce(c.sample_collected_flag, 0) = 1
                then coalesce(c.investigation_count, 1)
            else 0
        end::integer as sample_collected_count,

        case
            when coalesce(c.sample_collected_flag, 0) = 0
                then coalesce(c.investigation_count, 1)
            else 0
        end::integer as sample_not_collected_count

    from case_investigation_resolved c

    left join {{ ref('dim_date') }} investigation_date
        on c.investigation_date_key
         = investigation_date.date_key

    left join {{ ref('dim_epiweek') }} epiweek
        on investigation_date.full_date
        between epiweek.start_of_week
            and epiweek.end_of_week

    left join {{ ref('dim_date') }} birth_date
        on c.date_of_birth_key
         = birth_date.date_key

)

select *
from case_investigation_enriched