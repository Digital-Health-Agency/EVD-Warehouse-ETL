

with adam_screening as (

    select
        'ADAM'::text as source_system,
        cast(system_id as text) as source_record_id,

        cast(null as text) as facility_id,

        cast(null as text) as reporting_county,
        cast(null as text) as reporting_subcounty,
        cast(null as text) as ward,
        nullif(trim(point_of_entry), '') as point_of_entry,

        cast(created_at as date) as screening_date,
        created_at as screening_datetime,

        screened as is_screened,

        case
            when lower(trim(suspected_classification)) in (
                'suspected',
                'probable',
                'confirmed'
            ) then true

            when lower(trim(suspected_classification)) in (
                'not suspected',
                'negative',
                'no',
                'false',
                '0'
            ) then false

            else null
        end as is_suspected,

        case
            when lower(trim(suspected_classification)) = 'confirmed'
                then true

            when suspected_classification is not null
                then false

            else null
        end as is_confirmed,

        cast(null as boolean) as is_tested,
        cast(null as text) as test_result,

        nullif(trim(suspected_classification), '') as classification

    from {{ ref('slv_adam_travellers') }}

),

uhai_screening as (

    select
        'UHAI'::text as source_system,
        cast(system_id as text) as source_record_id,

        cast(facility_id as text) as facility_id,

        nullif(trim(reporting_county), '') as reporting_county,
        nullif(trim(reporting_subcounty), '') as reporting_subcounty,
        nullif(trim(ward), '') as ward,
        nullif(trim(point_of_entry), '') as point_of_entry,

        reporting_date as screening_date,

        coalesce(
            created_at,
            cast(reporting_date as timestamp)
        ) as screening_datetime,

        case
            when lower(trim(screening)) in (
                'yes',
                'true',
                '1',
                'screened'
            ) then true

            when lower(trim(screening)) in (
                'no',
                'false',
                '0',
                'not screened'
            ) then false

            else null
        end as is_screened,

        case
            when lower(trim(suspected)) in (
                'yes',
                'true',
                '1',
                'suspected'
            ) then true

            when lower(trim(suspected)) in (
                'no',
                'false',
                '0',
                'not suspected'
            ) then false

            else null
        end as is_suspected,

        case
            when lower(trim(confirmed)) in (
                'yes',
                'true',
                '1',
                'confirmed',
                'positive'
            ) then true

            when lower(trim(confirmed)) in (
                'no',
                'false',
                '0',
                'not confirmed',
                'negative'
            ) then false

            else null
        end as is_confirmed,

        case
            when lower(trim(tested)) in (
                'yes',
                'true',
                '1',
                'tested'
            ) then true

            when lower(trim(tested)) in (
                'no',
                'false',
                '0',
                'not tested'
            ) then false

            else null
        end as is_tested,

        nullif(trim(result), '') as test_result,

        case
            when lower(trim(confirmed)) in (
                'yes',
                'true',
                '1',
                'confirmed',
                'positive'
            ) then 'Confirmed'

            when lower(trim(suspected)) in (
                'yes',
                'true',
                '1',
                'suspected'
            ) then 'Suspected'

            else null
        end as classification

    from {{ ref('slv_uhai_cases') }}

),

combined as (

    select * from adam_screening

    union all

    select * from uhai_screening

),

locations as (

    select
        location_key,
        county,
        subcounty,
        ward,
        point_of_entry

    from {{ ref('dim_location') }}

),

facilities as (

    select
        facility_key,
        cast(mfl_code as text) as mfl_code

    from {{ ref('dim_facilitylist') }}

),

final as (

    select
        l.location_key,
        f.facility_key,

        s.source_system,
        s.source_record_id,

        s.screening_date,
        s.screening_datetime,

        s.is_screened,
        s.is_suspected,
        s.is_confirmed,
        s.is_tested,

        s.classification,
        s.test_result,

        s.facility_id,
        s.reporting_county,
        s.reporting_subcounty,
        s.ward,
        s.point_of_entry,

        1 as screening_count,

        case
            when s.is_screened is true then 1
            else 0
        end as screened_count,

        case
            when s.is_suspected is true then 1
            else 0
        end as suspected_count,

        case
            when s.is_confirmed is true then 1
            else 0
        end as confirmed_count,

        case
            when s.is_tested is true then 1
            else 0
        end as tested_count

    from combined s

    left join locations l
        on s.reporting_county is not distinct from l.county
       and s.reporting_subcounty is not distinct from l.subcounty
       and s.ward is not distinct from l.ward
       and s.point_of_entry is not distinct from l.point_of_entry

    left join facilities f
        on s.facility_id = f.mfl_code

)

select *
from final