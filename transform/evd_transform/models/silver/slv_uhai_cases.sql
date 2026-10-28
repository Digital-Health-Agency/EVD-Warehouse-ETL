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
        -- Contact, travel, clinical and exposure fields UHAI's API added on
        -- 27 Oct 2026. Read through column_or_null: bronze only has a column
        -- once it has seen a value for it. UHAI's `source` lands in bronze as
        -- source_field (bronze reserves _source for its own envelope).
        {{ column_or_null(bronze_columns, 'phone_number', "nullif(trim(phone_number::text), '')", 'text', 'phone_number') }},
        {{ column_or_null(bronze_columns, 'dependent_phone_number', "nullif(trim(dependent_phone_number::text), '')", 'text', 'dependent_phone_number') }},
        {{ column_or_null(bronze_columns, 'is_dependent', "nullif(trim(is_dependent::text), '')", 'text', 'is_dependent') }},
        {{ column_or_null(bronze_columns, 'country_of_departure', "nullif(trim(country_of_departure::text), '')", 'text', 'country_of_departure') }},
        {{ column_or_null(bronze_columns, 'entry_country', "nullif(trim(entry_country::text), '')", 'text', 'entry_country') }},
        {{ column_or_null(bronze_columns, 'arrival_date', "case when trim(arrival_date::text) ~ '^\d{4}-\d{2}-\d{2}' then left(trim(arrival_date::text), 10)::date end", 'date', 'arrival_date') }},
        {{ column_or_null(bronze_columns, 'mode_of_transport', "nullif(upper(trim(mode_of_transport::text)), '')", 'text', 'mode_of_transport') }},
        {{ column_or_null(bronze_columns, 'transport_number', "nullif(trim(transport_number::text), '')", 'text', 'transport_number') }},
        {{ column_or_null(bronze_columns, 'seat_number', "nullif(trim(seat_number::text), '')", 'text', 'seat_number') }},
        {{ column_or_null(bronze_columns, 'temperature', "case when trim(temperature::text) ~ '^\d+(\.\d+)?$' then trim(temperature::text)::numeric(4,1) end", 'numeric', 'temperature') }},
        {{ column_or_null(bronze_columns, 'symptoms', "nullif(trim(symptoms::text), '')", 'text', 'symptoms') }},
        {{ column_or_null(bronze_columns, 'risk_factors', "nullif(trim(risk_factors::text), '')", 'text', 'risk_factors') }},
        {{ column_or_null(bronze_columns, 'visited_affected_countries', "nullif(trim(visited_affected_countries::text), '')", 'text', 'visited_affected_countries') }},
        {{ column_or_null(bronze_columns, 'contact_with_confirmed_case', "nullif(trim(contact_with_confirmed_case::text), '')", 'text', 'contact_with_confirmed_case') }},
        {{ column_or_null(bronze_columns, 'healthcare_lab_work', "nullif(trim(healthcare_lab_work::text), '')", 'text', 'healthcare_lab_work') }},
        {{ column_or_null(bronze_columns, 'funeral_body_prep', "nullif(trim(funeral_body_prep::text), '')", 'text', 'funeral_body_prep') }},
        {{ column_or_null(bronze_columns, 'contact_animals', "nullif(trim(contact_animals::text), '')", 'text', 'contact_animals') }},
        {{ column_or_null(bronze_columns, 'bushmeat_handling', "nullif(trim(bushmeat_handling::text), '')", 'text', 'bushmeat_handling') }},
        {{ column_or_null(bronze_columns, 'forest_cave_exposure', "nullif(trim(forest_cave_exposure::text), '')", 'text', 'forest_cave_exposure') }},
        {{ column_or_null(bronze_columns, 'source_field', "nullif(trim(source_field::text), '')", 'text', 'screening_channel') }},

        dlt_load_id,
        dlt_id

    from src

)

select *
from cleaned