{{ config(materialized='table') }}

with source as (

    select
        nullif(trim(loinc_code), '') as loinc_code,
        nullif(trim(test_name), '') as test_name,
        nullif(trim(code_text), '') as code_text,
        nullif(trim(component_code), '') as component_code,
        nullif(trim(specimen_type), '') as specimen_type,
        nullif(trim(unit), '') as unit

    from {{ ref('slv_lims_results') }}

),

deduplicated as (

    select distinct
        loinc_code,
        test_name,
        code_text,
        component_code,
        specimen_type,
        unit

    from source

    where loinc_code is not null
       or test_name is not null

),

final as (

    select

        {{ dbt_utils.generate_surrogate_key([
            'loinc_code',
            'test_name',
            'specimen_type'
        ]) }} as labtest_key,

        loinc_code,
        test_name,
        code_text,
        component_code,
        specimen_type,
        unit

    from deduplicated

)

select *
from final