/*
 * NPHIIS case records.
 *
 * One row per case. NPHIIS replaced ADAM as the sending system for
 * EVD cases; the ADAM silver model is retained for history.
 */

with source_data as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,
        _processed,

        patient_id,
        epid,
        record_type,
        classification,
        disease,
        county_rls,
        subcounty_rls,
        county,
        sub_county,
        ward,
        facility,
        patient_name,
        sex,
        age,
        birth_date,
        phone,
        occupation,
        pregnant,
        residence_county,
        residence_subcounty,
        village,
        symptoms,
        care_type,
        care_facility,
        outcome,
        patient_status,
        exposure_type,
        relationship,
        travelled,
        vaccinated,
        samples_collected,
        sample_type,
        exposure_risk,
        specimen_id,
        sample_condition,
        lab_tests,
        preliminary_result,
        final_result,
        final_lab,
        contact_id,

        date_reported,
        epi_week,
        date_confirmed,
        investigation_date,
        onset_date,
        admission_date,
        contact_date,
        collection_date,
        sent_date,
        last_followup,
        death_date,
        received_date,
        preliminary_date,
        final_date

    from {{ source('bronze', 'nphiis_cases_raw') }}

),

cleaned as (

    select
        id,
        _ingested_at,
        nullif(trim(_source), '') as _source,
        _batch_id,
        nullif(trim(_source_file), '') as _source_file,
        coalesce(_processed, false) as _processed,

        nullif(trim(patient_id), '') as patient_id,
        nullif(trim(epid), '') as epid,
        nullif(trim(record_type), '') as record_type,
        nullif(trim(classification), '') as classification,
        nullif(trim(disease), '') as disease,
        nullif(trim(county_rls), '') as county_rls,
        nullif(trim(subcounty_rls), '') as subcounty_rls,
        nullif(trim(county), '') as county,
        nullif(trim(sub_county), '') as sub_county,
        nullif(trim(ward), '') as ward,
        nullif(trim(facility), '') as facility,
        nullif(trim(patient_name), '') as patient_name,

        case
            when lower(trim(sex)) in ('male', 'm') then 'Male'
            when lower(trim(sex)) in ('female', 'f') then 'Female'
            when nullif(trim(sex), '') is null then null
            else initcap(trim(sex))
        end as sex,

        case
            when trim(age) ~ '^[0-9]+$' then trim(age)::integer
            else null
        end as age,

        {{ safe_parse_date('birth_date') }} as birth_date,

        nullif(trim(phone), '') as phone,
        nullif(trim(occupation), '') as occupation,
        nullif(trim(pregnant), '') as pregnant,
        nullif(trim(residence_county), '') as residence_county,
        nullif(trim(residence_subcounty), '') as residence_subcounty,
        nullif(trim(village), '') as village,
        nullif(trim(symptoms), '') as symptoms,
        nullif(trim(care_type), '') as care_type,
        nullif(trim(care_facility), '') as care_facility,
        nullif(trim(outcome), '') as outcome,
        nullif(trim(patient_status), '') as patient_status,
        nullif(trim(exposure_type), '') as exposure_type,
        nullif(trim(relationship), '') as relationship,
        nullif(trim(travelled), '') as travelled,
        nullif(trim(vaccinated), '') as vaccinated,
        nullif(trim(samples_collected), '') as samples_collected,
        nullif(trim(sample_type), '') as sample_type,
        nullif(trim(exposure_risk), '') as exposure_risk,
        nullif(trim(specimen_id), '') as specimen_id,

        /*
         * NPHIIS specimen ids carry stray spaces ('NVRL/ 1098/ 26')
         * and correspond to the LIMS case identifier ('NVRL/1098/26').
         */
        nullif(regexp_replace(specimen_id, '\s', '', 'g'), '')
            as specimen_identifier_normalised,

        nullif(trim(sample_condition), '') as sample_condition,
        nullif(trim(lab_tests), '') as lab_tests,
        nullif(trim(preliminary_result), '') as preliminary_result,
        nullif(trim(final_result), '') as final_result,
        nullif(trim(final_lab), '') as final_lab,
        nullif(trim(contact_id), '') as contact_id,

        {{ safe_parse_date('date_reported') }} as date_reported,
        {{ safe_parse_date('epi_week') }} as epi_week_start,
        {{ safe_parse_date('date_confirmed') }} as date_confirmed,
        {{ safe_parse_date('investigation_date') }} as investigation_date,
        {{ safe_parse_date('onset_date') }} as onset_date,
        {{ safe_parse_date('admission_date') }} as admission_date,
        {{ safe_parse_date('contact_date') }} as contact_date,
        {{ safe_parse_date('collection_date') }} as collection_date,
        {{ safe_parse_date('sent_date') }} as sent_date,
        {{ safe_parse_date('last_followup') }} as last_followup,
        {{ safe_parse_date('death_date') }} as death_date,
        {{ safe_parse_date('received_date') }} as received_date,
        {{ safe_parse_date('preliminary_date') }} as preliminary_date,
        {{ safe_parse_date('final_date') }} as final_date

    from source_data

),

ranked as (

    select
        *,

        row_number() over (
            partition by coalesce(
                patient_id,
                epid,
                '__bronze_id_' || id::text
            )
            order by
                _ingested_at desc nulls last,
                id desc
        ) as row_number

    from cleaned

),

deduplicated as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,
        _processed,

        patient_id,
        epid,
        record_type,
        classification,
        disease,
        county_rls,
        subcounty_rls,
        county,
        sub_county,
        ward,
        facility,
        patient_name,
        sex,
        age,
        birth_date,
        phone,
        occupation,
        pregnant,
        residence_county,
        residence_subcounty,
        village,
        symptoms,
        care_type,
        care_facility,
        outcome,
        patient_status,
        exposure_type,
        relationship,
        travelled,
        vaccinated,
        samples_collected,
        sample_type,
        exposure_risk,
        specimen_id,
        specimen_identifier_normalised,
        sample_condition,
        lab_tests,
        preliminary_result,
        final_result,
        final_lab,
        contact_id,

        date_reported,
        epi_week_start,
        date_confirmed,
        investigation_date,
        onset_date,
        admission_date,
        contact_date,
        collection_date,
        sent_date,
        last_followup,
        death_date,
        received_date,
        preliminary_date,
        final_date

    from ranked

    where row_number = 1

)

select *
from deduplicated
