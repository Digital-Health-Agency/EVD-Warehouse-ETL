{{ config(
    materialized = 'table'
) }}

with dates as (

    select
        date_key,
        full_date as date_day

    from {{ ref('dim_date') }}

    where full_date is not null

),

epi_weeks as (

    select distinct
        (
            date_trunc('week', date_day + interval '1 day')
            - interval '1 day'
        )::date as start_of_week,

        (
            date_trunc('week', date_day + interval '1 day')
            + interval '5 days'
        )::date as end_of_week

    from dates

),

summary as (

    select
        start_of_week,
        end_of_week,

        trim(to_char(start_of_week, 'Day'))
            as start_week_day_name,

        trim(to_char(end_of_week, 'Day'))
            as end_week_day_name,

        extract(week from end_of_week)::integer
            as week_number,

        case
            when extract(month from end_of_week) = 1
             and extract(week from end_of_week) in (52, 53)
            then extract(year from end_of_week)::integer - 1

            else extract(year from end_of_week)::integer
        end as epi_year

    from epi_weeks

),

final_data as (

    select
        {{
            dbt_utils.generate_surrogate_key([
                'epi_year',
                'week_number'
            ])
        }} as epi_week_key,

        start_of_week,
        end_of_week,

        week_number,
        epi_year,

        concat(
            epi_year,
            '-W',
            lpad(week_number::text, 2, '0')
        ) as epi_week_label,

        start_week_day_name,
        end_week_day_name,

        case
            when current_date between start_of_week and end_of_week
            then true
            else false
        end as current_epi_week_flag,

        (
            extract(year from current_date)::integer = epi_year
        ) as current_epi_year_flag

    from summary

),

unset_record as (

    select
        'unset'::text as epi_week_key,

        '1900-01-01'::date as start_of_week,
        '1900-01-01'::date as end_of_week,

        -999::integer as week_number,
        -999::integer as epi_year,

        'unset'::text as epi_week_label,
        'unset'::text as start_week_day_name,
        'unset'::text as end_week_day_name,

        false as current_epi_week_flag,
        false as current_epi_year_flag

),

combined as (

    select * from final_data

    union all

    select * from unset_record

)

select
    epi_week_key,

    start_of_week,
    end_of_week,

    week_number,
    epi_year,
    epi_week_label,

    start_week_day_name,
    end_week_day_name,

    current_epi_week_flag,
    current_epi_year_flag,

    current_date as load_date

from combined