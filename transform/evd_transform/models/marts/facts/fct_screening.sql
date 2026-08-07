{{ config(
    materialized = 'table',
    schema = 'marts',
    alias = 'fct_screening'
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
        identifier,
        classification,
        point_of_entry,
        created_timestamp,
        latitude,
        longitude

    from {{ ref('slv_adam_travellers') }}

),

adam_deduplicated as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,

        id_field,
        name,
        identifier,
        classification,
        point_of_entry,
        created_timestamp,
        latitude,
        longitude

    from (

        select
            *,

            row_number() over (
                partition by coalesce(
                    nullif(trim(id_field), ''),
                    nullif(trim(identifier), ''),
                    cast(id as text)
                )
                order by
                    _ingested_at desc nulls last,
                    id desc
            ) as row_number

        from adam_source

    ) ranked

    where row_number = 1

),

adam_screenings as (

    select
        'ADAM'::text as source_system,
        'TRAVELLER'::text as surveillance_pathway,

        id::bigint as source_row_id,

        coalesce(
            nullif(trim(id_field), ''),
            nullif(trim(identifier), ''),
            cast(id as text)
        ) as source_record_id,

        nullif(trim(name), '') as person_name,
        nullif(trim(identifier), '') as person_identifier,

        (
            created_timestamp
            at time zone 'Africa/Nairobi'
        )::date as screening_date,

        created_timestamp as screening_datetime,

        nullif(trim(point_of_entry), '') as point_of_entry,

        lower(
            nullif(trim(point_of_entry), '')
        ) as point_of_entry_normalized,

        null::text as facility_code,

        nullif(trim(classification), '') as source_classification,

        case
            when lower(trim(classification)) = 'suspected'
                then 'SUSPECTED'

            when lower(trim(classification)) = 'probable'
                then 'PROBABLE'

            when nullif(trim(classification), '') is null
                then 'NORMAL'

            else upper(trim(classification))
        end as screening_outcome,

        1::integer as screening_count,

        case
            when nullif(trim(classification), '') is null
                then 1
            else 0
        end::integer as normal_flag,

        case
            when lower(trim(classification)) in (
                'suspected',
                'probable'
            )
                then 1
            else 0
        end::integer as flagged_flag,

        case
            when lower(trim(classification)) = 'suspected'
                then 1
            else 0
        end::integer as suspected_flag,

        case
            when lower(trim(classification)) = 'probable'
                then 1
            else 0
        end::integer as probable_flag,

        latitude::double precision as latitude,
        longitude::double precision as longitude,

        _ingested_at as ingested_at,
        _batch_id as batch_id,
        _source_file as source_file

    from adam_deduplicated

),

taifacare_source as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,

        id_field,
        patient_id,
        hospital_id,
        screening_date,
        screened_flag,
        classification,
        cif_required

    from {{ ref('slv_taifa_care_kenyaemr') }}

),

taifacare_deduplicated as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,

        id_field,
        patient_id,
        hospital_id,
        screening_date,
        screened_flag,
        classification,
        cif_required

    from (

        select
            *,

            row_number() over (
                partition by coalesce(
                    nullif(trim(id_field), ''),
                    nullif(trim(patient_id), ''),
                    cast(id as text)
                )
                order by
                    _ingested_at desc nulls last,
                    id desc
            ) as row_number

        from taifacare_source

    ) ranked

    where row_number = 1

),

taifacare_screenings as (

    select
        'TAIFACARE_KENYAEMR'::text as source_system,
        'FACILITY'::text as surveillance_pathway,

        id::bigint as source_row_id,

        coalesce(
            nullif(trim(id_field), ''),
            nullif(trim(patient_id), ''),
            cast(id as text)
        ) as source_record_id,

        null::text as person_name,
        nullif(trim(patient_id), '') as person_identifier,

        screening_date::date as screening_date,
        screening_date::timestamp as screening_datetime,

        null::text as point_of_entry,
        null::text as point_of_entry_normalized,

        nullif(trim(hospital_id), '') as facility_code,

        coalesce(
            nullif(trim(classification), ''),
            'NOT RISK'
        ) as source_classification,

        'NOT RISK'::text as screening_outcome,

        1::integer as screening_count,

        1::integer as normal_flag,
        0::integer as flagged_flag,
        0::integer as suspected_flag,
        0::integer as probable_flag,

        null::double precision as latitude,
        null::double precision as longitude,

        _ingested_at as ingested_at,
        _batch_id as batch_id,
        _source_file as source_file

    from taifacare_deduplicated

),

uhai_source as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,

        system_id,
        names,
        identifier_number,
        suspected,
        point_of_entry,
        reporting_date,
        created_at

    from {{ ref('slv_uhai_cases') }}

),

uhai_deduplicated as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,

        system_id,
        names,
        identifier_number,
        suspected,
        point_of_entry,
        reporting_date,
        created_at

    from (

        select
            *,

            row_number() over (
                partition by coalesce(
                    nullif(trim(system_id), ''),
                    cast(id as text)
                )
                order by
                    _ingested_at desc nulls last,
                    id desc
            ) as row_number

        from uhai_source

    ) ranked

    where row_number = 1

),

uhai_screenings as (

    select
        'UHAI'::text as source_system,
        'TRAVELLER'::text as surveillance_pathway,

        id::bigint as source_row_id,

        coalesce(
            nullif(trim(system_id), ''),
            cast(id as text)
        ) as source_record_id,

        nullif(trim(names), '') as person_name,
        nullif(trim(identifier_number), '') as person_identifier,

        coalesce(
            reporting_date,
            created_at::date
        ) as screening_date,

        created_at::timestamp as screening_datetime,

        {{ uhai_point_of_entry_name('point_of_entry') }} as point_of_entry,

        lower(
            {{ uhai_point_of_entry_name('point_of_entry') }}
        ) as point_of_entry_normalized,

        null::text as facility_code,

        nullif(trim(suspected), '') as source_classification,

        case
            when lower(trim(suspected)) = 'yes'
                then 'SUSPECTED'
            else 'NORMAL'
        end as screening_outcome,

        1::integer as screening_count,

        case
            when lower(trim(suspected)) = 'yes'
                then 0
            else 1
        end::integer as normal_flag,

        case
            when lower(trim(suspected)) = 'yes'
                then 1
            else 0
        end::integer as flagged_flag,

        case
            when lower(trim(suspected)) = 'yes'
                then 1
            else 0
        end::integer as suspected_flag,

        0::integer as probable_flag,

        null::double precision as latitude,
        null::double precision as longitude,

        _ingested_at as ingested_at,
        _batch_id as batch_id,
        _source_file as source_file

    from uhai_deduplicated

),

unioned_screenings as (

    select *
    from adam_screenings

    union all

    select *
    from uhai_screenings

    union all

    select *
    from taifacare_screenings

),

final as (

    select
        ------------------------------------------------------------------
        -- Fact key
        ------------------------------------------------------------------

        {{ dbt_utils.generate_surrogate_key([
            's.source_system',
            's.surveillance_pathway',
            's.source_record_id'
        ]) }} as screening_key,

        ------------------------------------------------------------------
        -- Dimension keys
        ------------------------------------------------------------------

        coalesce(
            dd.date_key,
            -1
        ) as screening_date_key,

        coalesce(
            dpoe.point_of_entry_key,
            {{ dbt_utils.generate_surrogate_key([
                "'UNKNOWN'"
            ]) }}
        ) as point_of_entry_key,

        facility.facility_key,

        ------------------------------------------------------------------
        -- Source and pathway
        ------------------------------------------------------------------

        s.source_system,
        s.surveillance_pathway,

        s.source_row_id,
        s.source_record_id,

        ------------------------------------------------------------------
        -- Person
        ------------------------------------------------------------------

        s.person_name,
        s.person_identifier,

        ------------------------------------------------------------------
        -- Screening date and time
        ------------------------------------------------------------------

        s.screening_date,
        s.screening_datetime,

        ------------------------------------------------------------------
        -- Traveller screening location
        ------------------------------------------------------------------

        s.point_of_entry,

        ------------------------------------------------------------------
        -- Facility screening location
        ------------------------------------------------------------------

        s.facility_code,
        facility.mfl_code,
        facility.facility_name,
        facility.province,
        facility.county,
        facility.subcounty,
        facility.ward,

        ------------------------------------------------------------------
        -- Classification and outcome
        ------------------------------------------------------------------

        s.source_classification,
        s.screening_outcome,

        ------------------------------------------------------------------
        -- Measures
        ------------------------------------------------------------------

        s.screening_count,
        s.normal_flag,
        s.flagged_flag,
        s.suspected_flag,
        s.probable_flag,

        ------------------------------------------------------------------
        -- Coordinates
        ------------------------------------------------------------------

        s.latitude,
        s.longitude,

        ------------------------------------------------------------------
        -- Data quality
        ------------------------------------------------------------------

        case
            when s.surveillance_pathway = 'FACILITY'
             and facility.facility_key is not null
                then true

            when s.surveillance_pathway = 'FACILITY'
                then false

            else null
        end as facility_matched_flag,

        ------------------------------------------------------------------
        -- Lineage
        ------------------------------------------------------------------

        s.ingested_at,
        s.batch_id,
        s.source_file

    from unioned_screenings s

    left join {{ ref('dim_date') }} dd
        on s.screening_date = dd.full_date

    left join {{ ref('dim_point_of_entry') }} dpoe
        on s.point_of_entry_normalized
         = dpoe.point_of_entry_normalized

    left join {{ ref('dim_facilitylist') }} facility
        on trim(s.facility_code)
         = trim(facility.mfl_code)

)

select *
from final