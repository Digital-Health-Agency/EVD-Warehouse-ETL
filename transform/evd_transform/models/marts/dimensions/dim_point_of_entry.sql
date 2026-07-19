
with adam_point_of_entry as (

    select distinct
        nullif(trim(point_of_entry), '') as point_of_entry,
        lower(nullif(trim(point_of_entry), '')) as point_of_entry_normalized

    from {{ ref('slv_adam_travellers') }}

    where nullif(trim(point_of_entry), '') is not null

),

deduplicated as (

    select
        point_of_entry_normalized,

        min(point_of_entry) as point_of_entry

    from adam_point_of_entry

    group by
        point_of_entry_normalized

),

known_points_of_entry as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'point_of_entry_normalized'
        ]) }} as point_of_entry_key,

        point_of_entry,
        point_of_entry_normalized,

        'ADAM'::text as source_system,

        true::boolean as is_active

    from deduplicated

),

unknown_point_of_entry as (

    select
        {{ dbt_utils.generate_surrogate_key([
            "'UNKNOWN'"
        ]) }} as point_of_entry_key,

        'Unknown'::text as point_of_entry,
        'unknown'::text as point_of_entry_normalized,

        'SYSTEM'::text as source_system,

        true::boolean as is_active

),

final as (

    select
        point_of_entry_key,
        point_of_entry,
        point_of_entry_normalized,
        source_system,
        is_active

    from known_points_of_entry

    union all

    select
        point_of_entry_key,
        point_of_entry,
        point_of_entry_normalized,
        source_system,
        is_active

    from unknown_point_of_entry

)

select *
from final