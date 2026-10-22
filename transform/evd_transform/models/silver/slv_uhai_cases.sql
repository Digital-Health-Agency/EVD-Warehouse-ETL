{#-
    Bronze only creates a column once a record has had a value for it, so
    the age column is read only when this bronze table has it.
-#}
{%- set bronze_columns = adapter.get_columns_in_relation(source('bronze', 'uhai_raw'))
    | map(attribute='name') | map('lower') | list -%}

with src as (

    select *
    from {{ source('bronze', 'uhai_raw') }}

),

cleaned as (

    select
        id,
        _ingested_at,
        _source,
        _batch_id,
        _source_file,

        nullif(system_id, '') as system_id,
        nullif(names, '') as names,
        upper(nullif(sex, '')) as sex,

        nullif(date_of_birth, '')::date as date_of_birth,
        -- Age in whole years as reported by UHAI. UHAI carries no date of
        -- birth, so this is the only age it provides. Non-numeric values are
        -- dropped here; implausible ones are banded UNKNOWN in gold.
        {% if 'age' in bronze_columns -%}
        case
            when btrim(age::text) ~ '^[0-9]{1,5}$'
                then btrim(age::text)::integer
        end as age_years,
        {%- else -%}
        null::integer as age_years,
        {%- endif %}
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