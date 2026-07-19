{{ config(
    materialized = 'table'
) }}

with source as (

    select
        nullif(trim(testing_lab_code), '') as laboratory_code,
        nullif(trim(testing_lab_name), '') as laboratory_name

    from {{ ref('slv_lims_results') }}

),

valid_laboratories as (

    select
        laboratory_code,
        laboratory_name

    from source

    where laboratory_code is not null
       or laboratory_name is not null

),

ranked as (

    select
        *,

        row_number() over (
            partition by
                lower(coalesce(laboratory_code, '')),
                lower(coalesce(laboratory_name, ''))
            order by
                laboratory_code nulls last,
                laboratory_name nulls last
        ) as row_number

    from valid_laboratories

),

deduplicated as (

    select
        laboratory_code,
        laboratory_name

    from ranked

    where row_number = 1

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'laboratory_code',
            'laboratory_name'
        ]) }} as laboratory_key,

        laboratory_code,
        laboratory_name

    from deduplicated

)

select *
from final