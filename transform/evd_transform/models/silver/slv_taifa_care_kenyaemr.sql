
with source_data as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,
        _processed,

        id_field,
        patient_id,
        sex,
        date_of_birth,
        hospital_id,
        interview_date,
        county,
        sub_county

    from {{ source('bronze', 'taifa_care_kenyaemr_raw') }}

),

cleaned as (

    select
        id,
        _ingested_at,
        nullif(trim(_source), '') as _source,
        _batch_id,
        nullif(trim(_source_file), '') as _source_file,
        coalesce(_processed, false) as _processed,

        nullif(trim(id_field), '') as id_field,
        nullif(trim(patient_id), '') as patient_id,

        case
            when lower(trim(sex)) in ('male', 'm') then 'Male'
            when lower(trim(sex)) in ('female', 'f') then 'Female'
            when nullif(trim(sex), '') is null then null
            else initcap(trim(sex))
        end as sex,

        case
            when nullif(trim(date_of_birth), '') is null then null
            else trim(date_of_birth)::timestamp::date
        end as date_of_birth,

        nullif(trim(hospital_id), '') as hospital_id,

        case
            when nullif(trim(interview_date), '') is null then null
            else trim(interview_date)::timestamp::date
        end as screening_date,

        initcap(nullif(trim(county), '')) as county,
        initcap(nullif(trim(sub_county), '')) as subcounty,

        true as screened_flag,
        'NOT RISK'::text as classification,
        false as cif_required,

        'EVD'::text as disease_screened,
        'FACILITY'::text as screening_context,
        'TAIFACARE_KENYAEMR'::text as source_system

    from source_data

),

ranked as (

    select
        *,

        row_number() over (
            partition by coalesce(
                id_field,
                '__bronze_id_' || id::text
            )
            order by
                _ingested_at desc nulls last,
                id desc
        ) as row_number

    from cleaned

)

select
    id,
    _ingested_at,
    _source,
    _batch_id,
    _source_file,
    _processed,

    id_field,
    patient_id,
    sex,
    date_of_birth,
    hospital_id,
    screening_date,
    county,
    subcounty,

    screened_flag,
    classification,
    cif_required,
    disease_screened,
    screening_context,
    source_system

from ranked
where row_number = 1