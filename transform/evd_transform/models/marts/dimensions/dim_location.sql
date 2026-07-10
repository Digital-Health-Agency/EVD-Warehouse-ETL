{{ config(
    materialized='table',
    schema='marts'
) }}

with adam_case_locations as (

    select distinct
        nullif(trim(reporting_county), '') as county,
        nullif(trim(reporting_subcounty), '') as subcounty,
        cast(null as text) as ward,
        cast(null as text) as point_of_entry

    from {{ ref('slv_adam_cases') }}

),

adam_traveller_locations as (

    select distinct
        cast(null as text) as county,
        cast(null as text) as subcounty,
        cast(null as text) as ward,
        nullif(trim(point_of_entry), '') as point_of_entry

    from {{ ref('slv_adam_travellers') }}

),

uhai_locations as (

    select distinct
        nullif(trim(reporting_county), '') as county,
        nullif(trim(reporting_subcounty), '') as subcounty,
        nullif(trim(ward), '') as ward,
        nullif(trim(point_of_entry), '') as point_of_entry

    from {{ ref('slv_uhai_cases') }}

),

combined as (

    select * from adam_case_locations

    union all

    select * from adam_traveller_locations

    union all

    select * from uhai_locations

),

valid_locations as (

    select
        county,
        subcounty,
        ward,
        point_of_entry

    from combined

    where county is not null
       or subcounty is not null
       or ward is not null
       or point_of_entry is not null

),

deduplicated as (

    select distinct
        county,
        subcounty,
        ward,
        point_of_entry

    from valid_locations

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'county',
            'subcounty',
            'ward',
            'point_of_entry'
        ]) }} as location_key,

        county,
        subcounty,
        ward,
        point_of_entry

    from deduplicated

)

select *
from final