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

        _ingested_at
            as ingested_at,

        _batch_id
            as batch_id,

        _source_file
            as source_file

    from adam_source

),

/*
 * Future source systems must return the same canonical columns.
 *
 * emr_case_investigations as (
 *
 *     select
 *         ...
 *
 * )
 */

unioned_case_investigations as (

    select *
    from adam_case_investigations

    /*
    union all

    select *
    from emr_case_investigations
    */

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

        c.ingested_at,
        c.batch_id,
        c.source_file

    from unioned_case_investigations c

    left join {{ ref('dim_date') }} investigation_date
        on c.investigation_date = investigation_date.full_date

    left join {{ ref('dim_date') }} birth_date
        on c.source_person_date_of_birth = birth_date.full_date

)

select *
from final