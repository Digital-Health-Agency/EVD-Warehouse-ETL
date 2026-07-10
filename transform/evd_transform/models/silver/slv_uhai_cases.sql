
with src as (

    select *
    from {{ source('bronze', 'uhai_raw') }}

),

cleaned as (

    select
        nullif(system_id, '') as system_id,
        nullif(names, '') as names,
        upper(nullif(sex, '')) as sex,

        nullif(date_of_birth, '')::date as date_of_birth,
        nullif(nationality, '') as nationality,
        nullif(identifier_type, '') as identifier_type,
        nullif(identifier, '') as identifier_number,

        nullif(suspected, '') as suspected,
        nullif(screening, '') as screening,
        nullif(confirmed, '') as confirmed,
        nullif(died, '') as died,
        nullif(recovered, '') as recovered,
        nullif(tested, '') as tested,
        nullif(result, '') as result,

        nullif(point_of_entry, '') as point_of_entry,
        nullif(reporting_county, '') as reporting_county,
        nullif(reporting_sub_county, '') as reporting_subcounty,
        nullif(ward, '') as ward,
        nullif(facility_fid, '') as facility_id,
        nullif(community_health_unit_chu, '') as community_health_unit,

        nullif(reporting_date, '')::date as reporting_date,
        nullif(reporting_time, '') as reporting_time,
        nullif(created_at, '')::timestamp as created_at,

        dlt_load_id,
        dlt_id

    from src

)

select *
from cleaned