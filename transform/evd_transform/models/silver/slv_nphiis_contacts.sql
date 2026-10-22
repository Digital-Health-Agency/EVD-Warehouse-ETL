/*
 * NPHIIS contact records.
 *
 * One row per registered contact. Contacts link to their index case
 * through source_epid, which matches slv_nphiis_cases.epid.
 *
 * Note: the contact feed's own epid column is a truncated prefix
 * (e.g. 'KEN-WAJ-WAE-2026-VHF-') and is not unique; patient_id is the
 * reliable business key.
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
        source_epid,
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
        exposure_type,
        relationship,
        travelled,
        vaccinated,
        days_followed,
        seen_recently,
        ever_symptomatic,
        monitoring_status,
        contact_id,

        date_reported,
        epi_week,
        investigation_date,
        contact_date,
        followup_ends

    from {{ source('bronze', 'nphiis_contacts_raw') }}

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
        nullif(trim(source_epid), '') as source_epid,
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
        nullif(trim(exposure_type), '') as exposure_type,
        nullif(trim(relationship), '') as relationship,
        nullif(trim(travelled), '') as travelled,
        nullif(trim(vaccinated), '') as vaccinated,

        case
            when trim(days_followed) ~ '^[0-9]+$' then trim(days_followed)::integer
            else null
        end as days_followed,

        case
            when lower(trim(seen_recently)) in ('yes', 'y', 'true', '1') then true
            when lower(trim(seen_recently)) in ('no', 'n', 'false', '0') then false
            else null
        end as seen_recently,

        case
            when lower(trim(ever_symptomatic)) in ('yes', 'y', 'true', '1') then true
            when lower(trim(ever_symptomatic)) in ('no', 'n', 'false', '0') then false
            else null
        end as ever_symptomatic,

        nullif(trim(monitoring_status), '') as monitoring_status,
        nullif(trim(contact_id), '') as contact_id,

        {{ safe_parse_date('date_reported') }} as date_reported,
        {{ safe_parse_date('epi_week') }} as epi_week_start,
        {{ safe_parse_date('investigation_date') }} as investigation_date,
        {{ safe_parse_date('contact_date') }} as contact_date,
        {{ safe_parse_date('followup_ends') }} as followup_ends

    from source_data

),

ranked as (

    select
        *,

        row_number() over (
            partition by coalesce(
                patient_id,
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
        source_epid,
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
        exposure_type,
        relationship,
        travelled,
        vaccinated,
        days_followed,
        seen_recently,
        ever_symptomatic,
        monitoring_status,
        contact_id,

        date_reported,
        epi_week_start,
        investigation_date,
        contact_date,
        followup_ends

    from ranked

    where row_number = 1

)

select *
from deduplicated
