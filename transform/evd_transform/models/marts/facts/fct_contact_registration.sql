
with adam_source as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,

        id_field,
        name,
        sex,
        date_of_birth,
        nationality,
        identifier,
        type,
        initial_classification,
        samples_collected,
        specimen_id,
        final_laboratory_results,
        reporting_county,
        reporting_subcounty,
        health_facility,
        date_of_investigation,
        created_timestamp,
        latitude,
        longitude,
        vhf_disease,
        final_classification,
        checked_by

    from {{ ref('slv_adam_cases') }}

    /*
     * ADAM stores case and contact records in the same dataset.
     * This fact only contains registered contacts.
     */
    where lower(trim(type)) = 'contact'

),

adam_contact_registrations as (

    select
        'ADAM'::text as source_system,

        id as source_row_id,

        coalesce(
            nullif(trim(id_field), ''),
            nullif(trim(identifier), ''),
            cast(id as text)
        ) as source_record_id,

        /*
         * Contact attributes are retained until a canonical
         * person or client dimension is implemented.
         */
        nullif(trim(name), '')
            as source_contact_name,

        nullif(trim(identifier), '')
            as source_contact_identifier,

        nullif(trim(sex), '')
            as source_contact_sex,

        date_of_birth
            as source_contact_date_of_birth,

        nullif(trim(nationality), '')
            as source_contact_nationality,

        /*
         * The current ADAM structure does not expose a dedicated
         * contact-registration date. The investigation date is
         * therefore used, with created timestamp as a fallback.
         */
        coalesce(
            date_of_investigation,
            cast(created_timestamp as date)
        ) as registration_date,

        created_timestamp
            as registration_datetime,

        nullif(trim(vhf_disease), '')
            as disease,

        nullif(trim(reporting_county), '')
            as reporting_county,

        nullif(trim(reporting_subcounty), '')
            as reporting_subcounty,

        nullif(trim(health_facility), '')
            as health_facility,

        /*
         * Classification fields are retained because a registered
         * contact may subsequently be assessed and classified.
         */
        nullif(trim(initial_classification), '')
            as source_initial_classification,

        case
            when lower(trim(initial_classification)) = 'suspected'
                then 'SUSPECTED'

            when lower(trim(initial_classification)) = 'probable'
                then 'PROBABLE'

            when lower(trim(initial_classification)) = 'confirmed'
                then 'CONFIRMED'

            when lower(trim(initial_classification)) in (
                'discarded',
                'not a case',
                'no case'
            )
                then 'DISCARDED'

            when nullif(trim(initial_classification), '') is null
                then 'UNKNOWN'

            else upper(trim(initial_classification))
        end as initial_classification,

        nullif(trim(final_classification), '')
            as source_final_classification,

        case
            when lower(trim(final_classification)) = 'suspected'
                then 'SUSPECTED'

            when lower(trim(final_classification)) = 'probable'
                then 'PROBABLE'

            when lower(trim(final_classification)) = 'confirmed'
                then 'CONFIRMED'

            when lower(trim(final_classification)) in (
                'discarded',
                'not a case',
                'no case',
                'negative'
            )
                then 'DISCARDED'

            when nullif(trim(final_classification), '') is null
                then 'UNKNOWN'

            else upper(trim(final_classification))
        end as final_classification,

        nullif(trim(samples_collected), '')
            as source_samples_collected,

        case
            when lower(trim(samples_collected)) in (
                'yes',
                'y',
                'true',
                '1'
            )
                then 1

            else 0
        end::integer as sample_collected_flag,

        nullif(trim(specimen_id), '')
            as specimen_identifier,

        /*
         * Retained for reconciliation only.
         * Laboratory analytics should use fct_lab_result.
         */
        nullif(trim(final_laboratory_results), '')
            as source_final_laboratory_result,

        nullif(trim(checked_by), '')
            as checked_by,

        1::integer
            as contact_registration_count,

        latitude,
        longitude,

        /*
         * Contact-tracing attributes are not captured by ADAM.
         */
        null::text
            as source_case_identifier,

        null::text
            as exposure_type,

        null::date
            as exposure_date,

        null::text
            as relationship_to_case,

        null::date
            as followup_end_date,

        null::integer
            as days_followed,

        null::text
            as monitoring_status,

        null::integer
            as seen_recently_flag,

        null::integer
            as ever_symptomatic_flag,

        _ingested_at
            as ingested_at,

        _batch_id
            as batch_id,

        _source_file
            as source_file

    from adam_source

),

nphiis_source as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,

        patient_id,
        source_epid,
        record_type,
        classification,
        disease,
        county_rls,
        subcounty_rls,
        county,
        sub_county,
        facility,
        patient_name,
        sex,
        birth_date,
        exposure_type,
        relationship,
        days_followed,
        seen_recently,
        ever_symptomatic,
        monitoring_status,
        date_reported,
        investigation_date,
        contact_date,
        followup_ends

    from {{ ref('slv_nphiis_contacts') }}

    /*
     * NPHIIS delivers contacts in their own feed, so every row here
     * is a contact. The guard protects against a future mixed feed.
     */
    where lower(trim(record_type)) = 'contact'
       or record_type is null

),

nphiis_contact_registrations as (

    select
        'NPHIIS'::text as source_system,

        id as source_row_id,

        /*
         * The contact feed's epid is a truncated prefix, so the
         * patient id is the only reliable contact identifier.
         */
        coalesce(
            patient_id,
            cast(id as text)
        ) as source_record_id,

        patient_name
            as source_contact_name,

        patient_id
            as source_contact_identifier,

        sex
            as source_contact_sex,

        birth_date
            as source_contact_date_of_birth,

        null::text
            as source_contact_nationality,

        coalesce(
            investigation_date,
            date_reported
        ) as registration_date,

        cast(
            coalesce(
                investigation_date,
                date_reported
            ) as timestamp
        ) as registration_datetime,

        disease,

        coalesce(county, county_rls)
            as reporting_county,

        coalesce(sub_county, subcounty_rls)
            as reporting_subcounty,

        facility
            as health_facility,

        /*
         * NPHIIS classifies these rows only as 'Contact'. A contact
         * has not been assessed as a case, so the canonical
         * classification is UNKNOWN rather than a literal CONTACT.
         */
        classification
            as source_initial_classification,

        'UNKNOWN'::text
            as initial_classification,

        classification
            as source_final_classification,

        'UNKNOWN'::text
            as final_classification,

        null::text
            as source_samples_collected,

        0::integer
            as sample_collected_flag,

        null::text
            as specimen_identifier,

        null::text
            as source_final_laboratory_result,

        null::text
            as checked_by,

        1::integer
            as contact_registration_count,

        null::numeric
            as latitude,

        null::numeric
            as longitude,

        /*
         * Contact-tracing attributes.
         */
        source_epid
            as source_case_identifier,

        exposure_type,

        contact_date
            as exposure_date,

        relationship
            as relationship_to_case,

        followup_ends
            as followup_end_date,

        days_followed,

        monitoring_status,

        case
            when seen_recently then 1
            when not seen_recently then 0
            else null
        end::integer as seen_recently_flag,

        case
            when ever_symptomatic then 1
            when not ever_symptomatic then 0
            else null
        end::integer as ever_symptomatic_flag,

        _ingested_at
            as ingested_at,

        _batch_id
            as batch_id,

        _source_file
            as source_file

    from nphiis_source

),

/*
 * Every source branch must return the same canonical columns
 * in the same order.
 */

unioned_contact_registrations as (

    select *
    from adam_contact_registrations

    union all

    select *
    from nphiis_contact_registrations

),

final as (

    select
        {{
            dbt_utils.generate_surrogate_key([
                'c.source_system',
                'c.source_record_id'
            ])
        }} as contact_registration_key,

        coalesce(
            registration_date.date_key,
            -1
        ) as registration_date_key,

        coalesce(
            birth_date.date_key,
            -1
        ) as contact_date_of_birth_key,

        c.source_system,
        c.source_row_id,
        c.source_record_id,

        c.source_contact_name,
        c.source_contact_identifier,
        c.source_contact_sex,
        c.source_contact_date_of_birth,
        c.source_contact_nationality,

        c.registration_date,
        c.registration_datetime,

        c.disease,

        c.reporting_county,
        c.reporting_subcounty,
        c.health_facility,

        c.source_initial_classification,
        c.initial_classification,

        c.source_final_classification,
        c.final_classification,

        c.source_samples_collected,
        c.sample_collected_flag,
        c.specimen_identifier,

        c.source_final_laboratory_result,

        c.checked_by,

        c.contact_registration_count,

        c.latitude,
        c.longitude,

        c.source_case_identifier,
        c.exposure_type,
        c.exposure_date,
        c.relationship_to_case,
        c.followup_end_date,
        c.days_followed,
        c.monitoring_status,
        c.seen_recently_flag,
        c.ever_symptomatic_flag,

        c.ingested_at,
        c.batch_id,
        c.source_file

    from unioned_contact_registrations c

    left join {{ ref('dim_date') }} registration_date
        on c.registration_date = registration_date.full_date

    left join {{ ref('dim_date') }} birth_date
        on c.source_contact_date_of_birth = birth_date.full_date

)

select *
from final