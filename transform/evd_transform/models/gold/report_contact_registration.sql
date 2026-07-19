
with contact_registrations as (

    select
        *

    from {{ ref('fct_contact_registration') }}

),

prepared as (

    select
        c.*,

        /*
         * Age at contact registration.
         */
        case
            when c.source_contact_date_of_birth is not null
             and c.registration_date is not null
                then extract(
                    year from age(
                        c.registration_date,
                        c.source_contact_date_of_birth
                    )
                )::integer

            else null
        end as age_at_registration

    from contact_registrations c

),

classified as (

    select
        p.*,

        /*
         * Calendar hierarchy.
         */
        extract(
            year from p.registration_date
        )::integer as reporting_year,

        extract(
            quarter from p.registration_date
        )::integer as reporting_quarter,

        extract(
            month from p.registration_date
        )::integer as reporting_month,

        trim(
            to_char(
                p.registration_date,
                'Month'
            )
        ) as reporting_month_name,

        to_char(
            p.registration_date,
            'YYYY-MM'
        ) as reporting_year_month,

        /*
         * Epidemiological hierarchy.
         */
        extract(
            isoyear from p.registration_date
        )::integer as reporting_epi_year,

        extract(
            week from p.registration_date
        )::integer as reporting_epi_week,

        concat(
            extract(
                isoyear from p.registration_date
            )::integer,
            '-W',
            lpad(
                extract(
                    week from p.registration_date
                )::integer::text,
                2,
                '0'
            )
        ) as reporting_epi_week_label,

        /*
         * Reporting age groups.
         */
        case
            when p.age_at_registration is null
                then 'UNKNOWN'

            when p.age_at_registration < 0
                then 'UNKNOWN'

            when p.age_at_registration < 1
                then '<1'

            when p.age_at_registration between 1 and 4
                then '1-4'

            when p.age_at_registration between 5 and 9
                then '5-9'

            when p.age_at_registration between 10 and 14
                then '10-14'

            when p.age_at_registration between 15 and 19
                then '15-19'

            when p.age_at_registration between 20 and 24
                then '20-24'

            when p.age_at_registration between 25 and 34
                then '25-34'

            when p.age_at_registration between 35 and 44
                then '35-44'

            when p.age_at_registration between 45 and 54
                then '45-54'

            when p.age_at_registration between 55 and 64
                then '55-64'

            when p.age_at_registration >= 65
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
        contact_registration_key,
        registration_date_key,
        contact_date_of_birth_key,

        /*
         * Source metadata.
         */
        source_system,
        source_row_id,
        source_record_id,

        /*
         * Contact attributes.
         */
        source_contact_name,
        source_contact_identifier,
        source_contact_sex,
        source_contact_date_of_birth,
        source_contact_nationality,

        age_at_registration,
        reporting_age_group,

        /*
         * Registration dates.
         */
        registration_date,
        registration_datetime,

        reporting_year,
        reporting_quarter,
        reporting_month,
        reporting_month_name,
        reporting_year_month,

        reporting_epi_year,
        reporting_epi_week,
        reporting_epi_week_label,

        /*
         * Disease.
         */
        disease,

        /*
         * Geography.
         */
        reporting_county,
        reporting_subcounty,
        health_facility,

        latitude,
        longitude,

        /*
         * Contact assessment.
         */
        source_initial_classification,
        initial_classification,

        source_final_classification,
        final_classification,

        /*
         * Laboratory linkage.
         */
        specimen_identifier,
        sample_collected_flag,
        source_samples_collected,
        source_final_laboratory_result,

        /*
         * Reviewer.
         */
        checked_by,

        /*
         * Base additive measure.
         */
        contact_registration_count
            as total_contact_registration_count,

        /*
         * Classification measures.
         */
        case
            when initial_classification = 'SUSPECTED'
                then contact_registration_count
            else 0
        end::integer as suspected_contact_count,

        case
            when initial_classification = 'PROBABLE'
                then contact_registration_count
            else 0
        end::integer as probable_contact_count,

        case
            when initial_classification = 'CONFIRMED'
                then contact_registration_count
            else 0
        end::integer as confirmed_contact_count,

        case
            when initial_classification = 'DISCARDED'
                then contact_registration_count
            else 0
        end::integer as discarded_contact_count,

        case
            when initial_classification = 'UNKNOWN'
                then contact_registration_count
            else 0
        end::integer as unknown_classification_count,

        /*
         * Sample collection measures.
         */
        case
            when sample_collected_flag = 1
                then contact_registration_count
            else 0
        end::integer as sampled_contact_count,

        case
            when sample_collected_flag = 0
                then contact_registration_count
            else 0
        end::integer as not_sampled_contact_count,

        /*
         * Data completeness measures.
         */
        case
            when specimen_identifier is not null
                then contact_registration_count
            else 0
        end::integer as specimen_identifier_available_count,

        case
            when source_contact_identifier is not null
                then contact_registration_count
            else 0
        end::integer as contact_identifier_available_count,

        case
            when disease is not null
                then contact_registration_count
            else 0
        end::integer as disease_available_count,

        case
            when registration_date is not null
                then contact_registration_count
            else 0
        end::integer as registration_date_available_count,

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