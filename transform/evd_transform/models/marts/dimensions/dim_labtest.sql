

with source as (

    select
        nullif(trim(code), '') as test_code,
        nullif(trim(test_name), '') as test_name,
        nullif(trim(code_text), '') as code_text,
        nullif(trim(component_code), '') as component_code,
        nullif(trim(specimen_type), '') as specimen_type,
        nullif(trim(unit), '') as unit

    from {{ ref('slv_lims_results') }}

),

valid_tests as (

    select
        test_code,
        test_name,
        code_text,
        component_code,
        specimen_type,
        unit

    from source

    where test_code is not null
       or test_name is not null
       or component_code is not null

),

ranked as (

    select
        *,

        row_number() over (
            partition by
                lower(coalesce(test_code, '')),
                lower(coalesce(test_name, '')),
                lower(coalesce(component_code, '')),
                lower(coalesce(specimen_type, '')),
                lower(coalesce(unit, ''))
            order by
                case
                    when code_text is not null then 0
                    else 1
                end,
                code_text
        ) as row_number

    from valid_tests

),

deduplicated as (

    select
        test_code,
        test_name,
        code_text,
        component_code,
        specimen_type,
        unit

    from ranked

    where row_number = 1

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'test_code',
            'test_name',
            'component_code',
            'specimen_type',
            'unit'
        ]) }} as labtest_key,

        test_code,
        test_name,
        code_text,
        component_code,
        specimen_type,
        unit

    from deduplicated

)

select *
from final