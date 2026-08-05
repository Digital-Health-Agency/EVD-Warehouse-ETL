
with mdharura_locations as (

    select
        nullif(trim(county), '') as county,
        nullif(trim(subcounty), '') as subcounty,
        nullif(trim(community_unit), '') as community_unit,
        nullif(trim(unit_name), '') as unit_name,
        nullif(trim(unit_type), '') as unit_type,

        'MDHARURA' as source_system,
        _ingested_at

    from {{ ref('slv_mdharura') }}

),

all_locations as (

    select *
    from mdharura_locations

    /*
    Future EBS source example:

    */

),

standardized as (

    select
        initcap(county) as county,
        initcap(subcounty) as subcounty,
        initcap(community_unit) as community_unit,
        initcap(unit_name) as unit_name,
        initcap(unit_type) as unit_type,

        source_system,
        _ingested_at

    from all_locations

    where county is not null
       or subcounty is not null
       or community_unit is not null
       or unit_name is not null

),

deduplicated as (

    select
        county,
        subcounty,
        community_unit,
        unit_name,
        unit_type,

        string_agg(
            distinct source_system,
            ', '
            order by source_system
        ) as source_systems,

        max(_ingested_at) as last_ingested_at

    from standardized

    group by
        county,
        subcounty,
        community_unit,
        unit_name,
        unit_type

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            "coalesce(county, 'UNKNOWN')",
            "coalesce(subcounty, 'UNKNOWN')",
            "coalesce(community_unit, 'UNKNOWN')",
            "coalesce(unit_name, 'UNKNOWN')",
            "coalesce(unit_type, 'UNKNOWN')"
        ]) }} as location_key,

        county,
        subcounty,
        community_unit,
        unit_name,
        unit_type,

        source_systems,
        last_ingested_at

    from deduplicated

)

select *
from final