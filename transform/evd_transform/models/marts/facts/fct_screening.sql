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
                partition by
                    coalesce(
                        nullif(trim(id_field), ''),
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

        id as source_row_id,

        coalesce(
            nullif(trim(id_field), ''),
            cast(id as text)
        ) as source_record_id,

        nullif(trim(name), '')
            as source_person_name,

        nullif(trim(identifier), '')
            as source_person_identifier,

        cast(created_timestamp as date)
            as screening_date,

        created_timestamp
            as screening_datetime,

        nullif(trim(point_of_entry), '')
            as point_of_entry,

        lower(nullif(trim(point_of_entry), ''))
            as point_of_entry_normalized,

        nullif(trim(classification), '')
            as source_classification,

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
            when lower(trim(classification)) = 'suspected'
                then 1
            else 0
        end::integer as suspected_flag,

        case
            when lower(trim(classification)) = 'probable'
                then 1
            else 0
        end::integer as probable_flag,

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

        latitude,
        longitude,

        _ingested_at as ingested_at,
        _batch_id as batch_id,
        _source_file as source_file

    from adam_deduplicated

),

/*
Future sources must return the same canonical columns.

uhai_screenings as (

    select
        ...
),

emr_screenings as (

    select
        ...
)
*/

unioned_screenings as (

    select *
    from adam_screenings

    /*
    union all

    select *
    from uhai_screenings

    union all

    select *
    from emr_screenings
    */

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            's.source_system',
            's.source_record_id'
        ]) }} as screening_key,

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

        s.source_system,
        s.source_row_id,
        s.source_record_id,

        s.source_person_name,
        s.source_person_identifier,

        s.screening_date,
        s.screening_datetime,

        s.point_of_entry,

        s.source_classification,
        s.screening_outcome,

        s.screening_count,
        s.normal_flag,
        s.flagged_flag,
        s.suspected_flag,
        s.probable_flag,

        s.latitude,
        s.longitude,

        s.ingested_at,
        s.batch_id,
        s.source_file

    from unioned_screenings s

    left join {{ ref('dim_date') }} dd
        on s.screening_date = dd.full_date

    left join {{ ref('dim_point_of_entry') }} dpoe
        on s.point_of_entry_normalized
         = dpoe.point_of_entry_normalized

)

select *
from final