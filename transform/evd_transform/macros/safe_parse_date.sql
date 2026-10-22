{% macro safe_parse_date(column) -%}
    {#-
        Safely convert a text date column to date.

        Accepts 'YYYY-MM-DD' or 'DD/MM/YYYY'. The parsed value must
        round-trip back to the original text, which rejects impossible
        dates (e.g. 31/02/2026) and year 0000. Anything else is null.
    -#}
    case
        when nullif(trim({{ column }}), '') is null then null

        when trim({{ column }})
                ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
             and substring(trim({{ column }}), 1, 4)::integer
                 between 1 and 9999
             and to_char(
                 to_date(trim({{ column }}), 'YYYY-MM-DD'),
                 'YYYY-MM-DD'
             ) = trim({{ column }})
            then to_date(
                trim({{ column }}),
                'YYYY-MM-DD'
            )

        when trim({{ column }})
                ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
             and right(trim({{ column }}), 4)::integer
                 between 1 and 9999
             and to_char(
                 to_date(trim({{ column }}), 'DD/MM/YYYY'),
                 'DD/MM/YYYY'
             ) = trim({{ column }})
            then to_date(
                trim({{ column }}),
                'DD/MM/YYYY'
            )

        else null
    end
{%- endmacro %}
