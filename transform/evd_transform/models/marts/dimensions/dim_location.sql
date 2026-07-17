with adam_traveller_locations as (
    select distinct
        nullif(trim(point_of_entry), '') as point_of_entry
    from {{ ref('slv_adam_travellers') }}

),
uhai_locations as (
    select distinct
        nullif(trim(point_of_entry), '') as point_of_entry
    from {{ ref('slv_uhai_cases') }}
),

combined as (
    select
        point_of_entry
    from adam_traveller_locations
    union all
    select
        point_of_entry
    from uhai_locations
),

deduplicated as (
    select distinct
        point_of_entry
    from combined
    where point_of_entry is not null
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'point_of_entry'
        ]) }} as point_of_entry_key,
        point_of_entry
    from deduplicated
)

select *
from final