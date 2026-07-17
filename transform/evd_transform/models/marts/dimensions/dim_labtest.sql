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

standardized as (

    select
        test_code,
        test_name,
        code_text,
        component_code,
        specimen_type,
        unit,

        lower(test_code) as normalized_test_code,
        lower(test_name) as normalized_test_name,
        lower(code_text) as normalized_code_text,
        lower(component_code) as normalized_component_code,
        lower(specimen_type) as normalized_specimen_type,
        lower(unit) as normalized_unit

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
                coalesce(normalized_test_code, ''),
                coalesce(normalized_test_name, ''),
                coalesce(normalized_component_code, ''),
                coalesce(normalized_specimen_type, ''),
                coalesce(normalized_unit, '')
            order by
                test_code nulls last,
                test_name nulls last,
                code_text nulls last,
                component_code nulls last,
                specimen_type nulls last,
                unit nulls last
        ) as row_number

    from standardized

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