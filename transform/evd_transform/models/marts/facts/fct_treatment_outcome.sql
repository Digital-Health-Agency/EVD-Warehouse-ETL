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
        final_classification,

        outcome,
        date_of_death,

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
        checked_by

    from {{ ref('slv_adam_cases') }}

    where lower(trim(type)) = 'case'

      and (
          nullif(trim(outcome), '') is not null
          or date_of_death is not null
      )

),

adam_prepared as (

    select
        'ADAM'::text as source_system,

        id as source_row_id,

        coalesce(
            nullif(trim(id_field), ''),
            nullif(trim(identifier), ''),
            cast(id as text)
        ) as source_record_id,

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

        nullif(trim(vhf_disease), '')
            as disease,

        /*
         * Retained temporarily for internal transformation only.
         * This field is not exposed in the final fact.
         */
        nullif(trim(outcome), '')
            as original_source_outcome,

        date_of_death,

        created_timestamp
            as outcome_recorded_datetime,

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

        nullif(trim(final_laboratory_results), '')
            as source_final_laboratory_result,

        case
            when lower(trim(final_laboratory_results)) in (
                'positive',
                'detected',
                'reactive',
                'positive/detected',
                'positive / detected'
            )
                then 'POSITIVE'

            when lower(trim(final_laboratory_results)) in (
                'negative',
                'not detected',
                'not_detected',
                'non-reactive',
                'non reactive',
                'nonreactive',
                'negative/not detected',
                'negative / not detected'
            )
                then 'NEGATIVE'

            when lower(trim(final_laboratory_results)) in (
                'inconclusive',
                'indeterminate',
                'invalid'
            )
                then 'INCONCLUSIVE'

            when nullif(trim(final_laboratory_results), '') is null
                then 'UNKNOWN'

            else 'OTHER'
        end as final_laboratory_result,

        nullif(trim(specimen_id), '')
            as specimen_identifier,

        nullif(trim(reporting_county), '')
            as reporting_county,

        nullif(trim(reporting_subcounty), '')
            as reporting_subcounty,

        nullif(trim(health_facility), '')
            as health_facility,

        nullif(trim(checked_by), '')
            as checked_by,

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

adam_source_outcome_standardized as (

    select
        *,

        /*
         * Temporary source-outcome correction:
         *
         * Where ADAM reports a death outcome but the final
         * laboratory result is negative, expose ALIVE rather
         * than DEAD/DECEASED.
         *
         * Death values are otherwise removed from source_outcome.
         * A valid DECEASED treatment outcome is derived separately.
         */
        case
            when lower(trim(original_source_outcome)) in (
                'dead',
                'death',
                'died',
                'deceased',
                'deceased/dead',
                'deceased /dead',
                'deceased / dead'
            )
            and final_laboratory_result = 'NEGATIVE'
                then 'ALIVE'

            when lower(trim(original_source_outcome)) in (
                'dead',
                'death',
                'died',
                'deceased',
                'deceased/dead',
                'deceased /dead',
                'deceased / dead'
            )
                then null

            when lower(trim(original_source_outcome)) in (
                'alive',
                'living'
            )
                then 'ALIVE'

            when lower(trim(original_source_outcome)) in (
                'recovered',
                'recovery',
                'discharged',
                'alive and discharged'
            )
                then 'RECOVERED'

            when lower(trim(original_source_outcome)) in (
                'admitted',
                'on treatment',
                'under treatment',
                'ongoing',
                'hospitalized',
                'hospitalised'
            )
                then 'ON_TREATMENT'

            when lower(trim(original_source_outcome)) in (
                'transferred',
                'referred',
                'transfer out',
                'transferred out'
            )
                then 'TRANSFERRED'

            when lower(trim(original_source_outcome)) in (
                'lost to follow up',
                'lost to follow-up',
                'lost',
                'ltfu'
            )
                then 'LOST_TO_FOLLOW_UP'

            when nullif(trim(original_source_outcome), '') is null
                then null

            else upper(trim(original_source_outcome))
        end as source_outcome

    from adam_prepared

),

adam_death_validation as (

    select
        *,

        /*
         * A validated death is derived from the final case and
         * laboratory information, not from the ADAM outcome text.
         */
        case
            when date_of_death is not null
             and final_classification = 'CONFIRMED'
             and final_laboratory_result = 'POSITIVE'
                then 1

            else 0
        end::integer as died_flag,

        case
            when date_of_death is not null
             and final_classification = 'CONFIRMED'
             and final_laboratory_result = 'POSITIVE'
                then null

            when date_of_death is not null
             and final_classification <> 'CONFIRMED'
                then 'DEATH_DATE_WITHOUT_CONFIRMED_CLASSIFICATION'

            when date_of_death is not null
             and final_classification = 'CONFIRMED'
             and final_laboratory_result = 'NEGATIVE'
                then 'DEATH_DATE_WITH_NEGATIVE_LAB_RESULT'

            when date_of_death is not null
             and final_classification = 'CONFIRMED'
             and final_laboratory_result in (
                 'UNKNOWN',
                 'INCONCLUSIVE',
                 'OTHER'
             )
                then 'DEATH_DATE_WITHOUT_POSITIVE_LAB_RESULT'

            else null
        end as outcome_validation_status

    from adam_source_outcome_standardized

),

adam_treatment_outcomes as (

    select
        *,

        case
            when died_flag = 1
                then 'DECEASED'

            when source_outcome = 'ALIVE'
                then 'ALIVE'

            when source_outcome = 'RECOVERED'
                then 'RECOVERED'

            when source_outcome = 'ON_TREATMENT'
                then 'ON_TREATMENT'

            when source_outcome = 'TRANSFERRED'
                then 'TRANSFERRED'

            when source_outcome = 'LOST_TO_FOLLOW_UP'
                then 'LOST_TO_FOLLOW_UP'

            else 'UNKNOWN'
        end as treatment_outcome,

        case
            when died_flag = 1
                then date_of_death

            else null
        end as outcome_date,

        1::integer
            as treatment_outcome_count

    from adam_death_validation

),

unioned_treatment_outcomes as (

    select
        source_system,
        source_row_id,
        source_record_id,

        source_person_name,
        source_person_identifier,
        source_person_sex,
        source_person_date_of_birth,
        source_person_nationality,

        disease,

        source_outcome,
        treatment_outcome,

        died_flag,

        outcome_date,
        date_of_death,
        outcome_recorded_datetime,
        outcome_validation_status,

        source_initial_classification,
        initial_classification,

        source_final_classification,
        final_classification,

        source_final_laboratory_result,
        final_laboratory_result,

        specimen_identifier,

        reporting_county,
        reporting_subcounty,
        health_facility,

        checked_by,

        treatment_outcome_count,

        latitude,
        longitude,

        ingested_at,
        batch_id,
        source_file

    from adam_treatment_outcomes

),

final as (

    select
        {{
            dbt_utils.generate_surrogate_key([
                'o.source_system',
                'o.source_record_id'
            ])
        }} as treatment_outcome_key,

        coalesce(
            outcome_date_dim.date_key,
            -1
        ) as outcome_date_key,

        coalesce(
            outcome_recorded_date_dim.date_key,
            -1
        ) as outcome_recorded_date_key,

        coalesce(
            birth_date_dim.date_key,
            -1
        ) as date_of_birth_key,

        o.source_system,
        o.source_row_id,
        o.source_record_id,

        o.source_person_name,
        o.source_person_identifier,
        o.source_person_sex,
        o.source_person_date_of_birth,
        o.source_person_nationality,

        o.disease,

        o.source_outcome,
        o.treatment_outcome,

        o.died_flag,

        o.outcome_date,
        o.date_of_death,
        o.outcome_recorded_datetime,
        o.outcome_validation_status,

        o.source_initial_classification,
        o.initial_classification,

        o.source_final_classification,
        o.final_classification,

        o.source_final_laboratory_result,
        o.final_laboratory_result,

        o.specimen_identifier,

        o.reporting_county,
        o.reporting_subcounty,
        o.health_facility,

        o.checked_by,

        o.treatment_outcome_count,

        o.latitude,
        o.longitude,

        o.ingested_at,
        o.batch_id,
        o.source_file

    from unioned_treatment_outcomes o

    left join {{ ref('dim_date') }} outcome_date_dim
        on o.outcome_date = outcome_date_dim.full_date

    left join {{ ref('dim_date') }} outcome_recorded_date_dim
        on cast(o.outcome_recorded_datetime as date)
         = outcome_recorded_date_dim.full_date

    left join {{ ref('dim_date') }} birth_date_dim
        on o.source_person_date_of_birth
         = birth_date_dim.full_date

)

select *
from final