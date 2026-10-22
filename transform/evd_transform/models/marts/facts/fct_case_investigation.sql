{{ config(
    materialized = 'table',
    schema = 'marts'
) }}

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
     * The ADAM case silver contains both case and contact records.
     * Only formal case records belong in fct_case_investigation.
     */
    where lower(trim(type)) = 'case'

),

adam_case_investigations as (

    select
        'ADAM'::text as source_system,

        id as source_row_id,

        coalesce(
            nullif(trim(id_field), ''),
            nullif(trim(identifier), ''),
            cast(id as text)
        ) as source_record_id,

        /*
         * Person attributes are retained temporarily until
         * linkage to a canonical person/client dimension exists.
         */
        nullif(trim(name), '')
            as source_person_name,

        nullif(trim(identifier), '')
            as source_person_identifier,

        nullif(trim(sex), '')
            as source_person_sex,

        date_of_birth
            as source_person_date_of_birth,

        nullif(trim(nationality), '')
            as source_person_nationality,

        /*
         * Prefer the formal investigation date.
         * Use record creation date when investigation date is absent.
         */
        coalesce(
            date_of_investigation,
            cast(created_timestamp as date)
        ) as investigation_date,

        created_timestamp
            as investigation_datetime,

        nullif(trim(vhf_disease), '')
            as disease,

        nullif(trim(reporting_county), '')
            as reporting_county,

        nullif(trim(reporting_subcounty), '')
            as reporting_subcounty,

        nullif(trim(health_facility), '')
            as health_facility,

        /*
         * Original initial classification retained for
         * source reconciliation and data-quality review.
         */
        nullif(trim(initial_classification), '')
            as source_initial_classification,

        /*
         * Standardized initial epidemiological classification.
         */
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

        /*
         * Original final classification retained for
         * source reconciliation and data-quality review.
         */
        nullif(trim(final_classification), '')
            as source_final_classification,

        /*
         * Final epidemiological conclusion of the investigation.
         */
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
         * Retained for source reconciliation.
         * Laboratory reporting should use fct_lab_result.
         */
        nullif(trim(final_laboratory_results), '')
            as source_final_laboratory_result,

        nullif(trim(checked_by), '')
            as checked_by,

        1::integer
            as investigation_count,

        latitude,
        longitude,

        /*
         * Not captured by ADAM.
         */
        null::date
            as symptom_onset_date,

        null::date
            as reported_date,

        _ingested_at
            as ingested_at,

        _batch_id
            as batch_id,

        _source_file
            as source_file,

        'INVESTIGATION'::text
            as case_record_origin

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
        epid,
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
        samples_collected,
        specimen_id,
        final_result,
        date_reported,
        investigation_date,
        onset_date

    from {{ ref('slv_nphiis_cases') }}

    /*
     * NPHIIS delivers contacts in a separate feed, so every row here
     * is a case. The guard protects against a future mixed feed.
     */
    where lower(trim(record_type)) = 'case'
       or record_type is null

),

nphiis_case_investigations as (

    select
        'NPHIIS'::text as source_system,

        id as source_row_id,

        /*
         * The EPID is the formal case identifier
         * (e.g. KEN-KAJ-KAN-2026-VHF-001).
         */
        coalesce(
            epid,
            patient_id,
            cast(id as text)
        ) as source_record_id,

        patient_name
            as source_person_name,

        patient_id
            as source_person_identifier,

        sex
            as source_person_sex,

        birth_date
            as source_person_date_of_birth,

        /*
         * NPHIIS does not capture nationality.
         */
        null::text
            as source_person_nationality,

        coalesce(
            investigation_date,
            date_reported
        ) as investigation_date,

        cast(
            coalesce(
                investigation_date,
                date_reported
            ) as timestamp
        ) as investigation_datetime,

        disease,

        coalesce(county, county_rls)
            as reporting_county,

        coalesce(sub_county, subcounty_rls)
            as reporting_subcounty,

        facility
            as health_facility,

        /*
         * NPHIIS carries a single classification; it feeds both the
         * initial and the final canonical classification.
         */
        classification
            as source_initial_classification,

        case
            when lower(trim(classification)) = 'suspected'
                then 'SUSPECTED'

            when lower(trim(classification)) = 'probable'
                then 'PROBABLE'

            when lower(trim(classification)) = 'confirmed'
                then 'CONFIRMED'

            when lower(trim(classification)) in (
                'discarded',
                'not a case',
                'no case'
            )
                then 'DISCARDED'

            when nullif(trim(classification), '') is null
                then 'UNKNOWN'

            else upper(trim(classification))
        end as initial_classification,

        classification
            as source_final_classification,

        case
            when lower(trim(classification)) = 'suspected'
                then 'SUSPECTED'

            when lower(trim(classification)) = 'probable'
                then 'PROBABLE'

            when lower(trim(classification)) = 'confirmed'
                then 'CONFIRMED'

            when lower(trim(classification)) in (
                'discarded',
                'not a case',
                'no case',
                'negative'
            )
                then 'DISCARDED'

            when nullif(trim(classification), '') is null
                then 'UNKNOWN'

            else upper(trim(classification))
        end as final_classification,

        samples_collected
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

        /*
         * NPHIIS specimen ids correspond to the LIMS case identifier
         * (e.g. 'NVRL/ 1098/ 26' ~ 'NVRL/1098/26'); the gold layer
         * normalises whitespace before linking.
         */
        specimen_id
            as specimen_identifier,

        final_result
            as source_final_laboratory_result,

        null::text
            as checked_by,

        1::integer
            as investigation_count,

        null::numeric
            as latitude,

        null::numeric
            as longitude,

        onset_date
            as symptom_onset_date,

        date_reported
            as reported_date,

        _ingested_at
            as ingested_at,

        _batch_id
            as batch_id,

        _source_file
            as source_file,

        'INVESTIGATION'::text
            as case_record_origin

    from nphiis_source

),

/*
 * Every source branch must return the same canonical columns
 * in the same order.
 */

source_case_investigations as (

    select *
    from adam_case_investigations

    union all

    select *
    from nphiis_case_investigations

),

/*
 * Lab-only confirmed cases.
 *
 * A positive LIMS EVD result is a confirmed case whether or not a
 * case investigation form exists for it. Where no ADAM/NPHIIS record
 * links to the LIMS case (via the specimen identifier), one row per
 * LIMS case is emitted here with the LIMS case identifier standing in
 * for the person, so that confirmed-case counts, the case linelist
 * and the case fatality rate reconcile to positive laboratory tests.
 *
 * The LIMS case identifier ('NVRL/1098/26') is a per-patient-episode
 * key; one case can carry several specimens and tests.
 */
lims_lab_results as (

    select
        source_row_id,
        subject_identifier,
        case_identifier,
        specimen_identifier,
        component_code,
        result_category,
        result_value,
        result_datetime,
        collection_date,
        requesting_facility_mfl,
        ingested_at,
        batch_id,
        source_file,

        {{ lims_link_key(
            "coalesce(case_identifier, subject_identifier, specimen_identifier)"
        ) }} as lab_case_key

    from {{ ref('fct_lab_result') }}

    where coalesce(
        case_identifier,
        subject_identifier,
        specimen_identifier
    ) is not null

),

/*
 * Every identifier a source system might store for a LIMS case,
 * over all LIMS rows (any test, any result): ADAM stores the LIMS
 * specimen identifier, NPHIIS the LIMS case identifier, and one
 * specimen identifier can span more than one LIMS case.
 */
lims_link_map as (

    select
        {{ lims_link_key('specimen_identifier') }} as link_key,
        lab_case_key

    from lims_lab_results

    where nullif(trim(specimen_identifier), '') is not null

    union

    select
        {{ lims_link_key('case_identifier') }} as link_key,
        lab_case_key

    from lims_lab_results

    where nullif(trim(case_identifier), '') is not null

),

investigation_linked_lab_cases as (

    select distinct
        m.lab_case_key

    from source_case_investigations i

    inner join lims_link_map m
        on {{ lims_link_key('i.specimen_identifier') }} = m.link_key

    where nullif(trim(i.specimen_identifier), '') is not null

),

lims_positive_results as (

    select
        *,

        row_number() over (
            partition by lab_case_key
            order by result_datetime, source_row_id
        ) as positive_rank

    from lims_lab_results

    where trim(component_code) = '86518-8'
      and result_category = 'POSITIVE'

),

/*
 * Upper-cased to match the facility naming used by NPHIIS rows, so
 * downstream facility breakdowns do not split one facility in two.
 */
lims_facilities as (

    select
        lower(nullif(trim(mfl_code::text), ''))
            as mfl_code_normalized,

        upper(min(facility_name))
            as facility_name,

        upper(min(county))
            as county,

        upper(min(subcounty))
            as subcounty

    from {{ ref('dim_facilitylist') }}

    where nullif(trim(mfl_code::text), '') is not null

    group by lower(nullif(trim(mfl_code::text), ''))

),

lims_lab_only_case_investigations as (

    select
        'LIMS'::text as source_system,

        r.source_row_id,

        r.lab_case_key
            as source_record_id,

        /*
         * No person record exists; the LIMS case identifier stands in
         * until an investigation form linking to it arrives.
         */
        r.lab_case_key
            as source_person_name,

        r.lab_case_key
            as source_person_identifier,

        null::text
            as source_person_sex,

        null::date
            as source_person_date_of_birth,

        null::text
            as source_person_nationality,

        cast(r.result_datetime as date)
            as investigation_date,

        r.result_datetime
            as investigation_datetime,

        'Ebola'::text
            as disease,

        f.county
            as reporting_county,

        f.subcounty
            as reporting_subcounty,

        f.facility_name
            as health_facility,

        null::text
            as source_initial_classification,

        'CONFIRMED'::text
            as initial_classification,

        null::text
            as source_final_classification,

        'CONFIRMED'::text
            as final_classification,

        null::text
            as source_samples_collected,

        1::integer
            as sample_collected_flag,

        /*
         * The LIMS case identifier is the specimen identifier NPHIIS
         * would have stored; gold links it back to LIMS the same way.
         */
        r.lab_case_key
            as specimen_identifier,

        r.result_value
            as source_final_laboratory_result,

        null::text
            as checked_by,

        1::integer
            as investigation_count,

        null::numeric
            as latitude,

        null::numeric
            as longitude,

        null::date
            as symptom_onset_date,

        null::date
            as reported_date,

        r.ingested_at,
        r.batch_id,
        r.source_file,

        'LAB_ONLY'::text
            as case_record_origin

    from lims_positive_results r

    left join lims_facilities f
        on lower(nullif(trim(r.requesting_facility_mfl), ''))
         = f.mfl_code_normalized

    where r.positive_rank = 1

      and r.lab_case_key not in (
          select lab_case_key
          from investigation_linked_lab_cases
      )

),

unioned_case_investigations as (

    select *
    from source_case_investigations

    union all

    select *
    from lims_lab_only_case_investigations

),

final as (

    select
        {{
            dbt_utils.generate_surrogate_key([
                'c.source_system',
                'c.source_record_id'
            ])
        }} as case_investigation_key,

        coalesce(
            investigation_date.date_key,
            -1
        ) as investigation_date_key,

        coalesce(
            birth_date.date_key,
            -1
        ) as date_of_birth_key,

        c.source_system,
        c.source_row_id,
        c.source_record_id,

        c.source_person_name,
        c.source_person_identifier,
        c.source_person_sex,
        c.source_person_date_of_birth,
        c.source_person_nationality,

        c.investigation_date,
        c.investigation_datetime,

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

        c.investigation_count,

        c.latitude,
        c.longitude,

        c.symptom_onset_date,
        c.reported_date,

        c.ingested_at,
        c.batch_id,
        c.source_file,

        c.case_record_origin

    from unioned_case_investigations c

    left join {{ ref('dim_date') }} investigation_date
        on c.investigation_date = investigation_date.full_date

    left join {{ ref('dim_date') }} birth_date
        on c.source_person_date_of_birth = birth_date.full_date

)

select *
from final