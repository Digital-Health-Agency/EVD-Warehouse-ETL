with calendar as (

    {{
        dbt_date.get_date_dimension(
            start_date = "2020-01-01",
            end_date   = "2035-12-31"
        )
    }}

)

select

    cast(to_char(date_day, 'YYYYMMDD') as bigint) as date_key,

    date_day as full_date,

    extract(year from date_day)::int as year,
    extract(quarter from date_day)::int as quarter,
    extract(month from date_day)::int as month,
    to_char(date_day, 'Month') as month_name,
    to_char(date_day, 'Mon') as month_short,

    extract(day from date_day)::int as day,
    extract(doy from date_day)::int as day_of_year,

    extract(week from date_day)::int as week_of_year,
    extract(isodow from date_day)::int as day_of_week,

    to_char(date_day, 'Day') as day_name,
    to_char(date_day, 'Dy') as day_short,

    case
        when extract(isodow from date_day) in (6,7)
        then true
        else false
    end as is_weekend,

    case
        when extract(month from date_day) between 1 and 3 then 1
        when extract(month from date_day) between 4 and 6 then 2
        when extract(month from date_day) between 7 and 9 then 3
        else 4
    end as quarter_number,

    concat(
        extract(year from date_day),
        '-Q',
        extract(quarter from date_day)
    ) as year_quarter,

    concat(
        extract(year from date_day),
        '-',
        lpad(extract(month from date_day)::text,2,'0')
    ) as year_month

from calendar