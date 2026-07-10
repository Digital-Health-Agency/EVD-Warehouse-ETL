{{ config(
    materialized = 'table'
) }}

with cases as (

    select *
    from {{ ref('fct_cases') }}

),

patients as (

    select *
    from {{ ref('dim_patient') }}

),

diseases as (

    select *
    from {{ ref('dim_disease') }}

),

locations as (

    select *
    from {{ ref('dim_location') }}

),

facilities as (

    select *
    from {{ ref('dim_facility') }}

),

dates as (

    select *
    from {{ ref('dim_date') }}

),

final as (

    select
        /* Case identifiers */
        c.case_key,
        c.case_id,
        c.system_id,
        c.source_system,
        c.source_record_id,

        /* Case dates */
        c.date_key,
        d.date_day as case_date,
        d.day_name,
        d.day_of_week,
        d.week_number,
        d.epi_week,
        d.epi_year,
        d.month_number,
        d.month_name,
        d.quarter_number,
        d.calendar_year,

        /* Patient details */
        c.patient_key,
        p.patient_id,
        p.identifier_number,
        p.names,
        p.sex,
        p.date_of_birth,
        p.age,
        p.age_group,
        p.nationality,

        /* Disease details */
        c.disease_key,
        dis.disease_name,
        dis.disease_code,
        dis.loinc_code,

        /* Case classification */
        c.record_type,
        c.case_classification,
        c.case_status,
        c.outcome,

        /* Clinical information */
        c.date_of_onset,
        c.date_reported,
        c.date_investigated,
        c.date_admitted,
        c.date_discharged,
        c.date_of_outcome,

        /* Laboratory information */
        c.samples_collected,
        c.specimen_id,
        c.final_laboratory_results,

        case
            when lower(c.final_laboratory_results) in (
                'positive',
                'detected',
                'confirmed'
            ) then 'Positive'

            when lower(c.final_laboratory_results) in (
                'negative',
                'not detected'
            ) then 'Negative'

            when c.final_laboratory_results is null then 'Not Tested'

            else 'Other'
        end as laboratory_result_group,

        /* Reporting facility */
        c.facility_key,
        f.facility_identifier,
        f.mfl_code,
        f.facility_name,
        f.facility_type,
        f.ownership as facility_ownership,

        /* Reporting location */
        c.location_key,
        l.county,
        l.subcounty,
        l.ward,
        l.community_health_unit,
        l.point_of_entry,
        l.latitude,
        l.longitude,

        /* Reporting timelines */
        case
            when c.date_reported is not null
                and c.date_of_onset is not null
            then c.date_reported - c.date_of_onset
        end as onset_to_report_days,

        case
            when c.date_investigated is not null
                and c.date_reported is not null
            then c.date_investigated - c.date_reported
        end as report_to_investigation_days,

        case
            when c.date_reported is not null
                and c.date_of_onset is not null
                and c.date_reported - c.date_of_onset <= 1
            then true
            else false
        end as reported_within_24_hours,

        /* Dashboard flags */
        case
            when lower(c.case_classification) = 'suspected'
            then 1 else 0
        end as suspected_case_flag,

        case
            when lower(c.case_classification) = 'probable'
            then 1 else 0
        end as probable_case_flag,

        case
            when lower(c.case_classification) = 'confirmed'
            then 1 else 0
        end as confirmed_case_flag,

        case
            when lower(c.outcome) in (
                'dead',
                'death',
                'deceased',
                'died'
            ) then 1 else 0
        end as death_flag,

        case
            when lower(c.outcome) in (
                'recovered',
                'discharged',
                'alive'
            ) then 1 else 0
        end as recovered_flag,

        case
            when c.samples_collected is true
            then 1 else 0
        end as sampled_flag,

        case
            when lower(c.final_laboratory_results) in (
                'positive',
                'detected',
                'confirmed'
            ) then 1 else 0
        end as positive_result_flag,

        case
            when lower(c.final_laboratory_results) in (
                'negative',
                'not detected'
            ) then 1 else 0
        end as negative_result_flag,

        /* Record lineage */
        c.created_at,
        c.updated_at

    from cases c

    left join dates d
        on c.date_key = d.date_key

    left join patients p
        on c.patient_key = p.patient_key

    left join diseases dis
        on c.disease_key = dis.disease_key

    left join locations l
        on c.location_key = l.location_key

    left join facilities f
        on c.facility_key = f.facility_key

)

select *
from final