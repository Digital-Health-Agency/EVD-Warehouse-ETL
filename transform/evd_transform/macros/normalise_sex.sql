{#
  Conformed sex value for screening records.

  Upper-cases and trims the source value and expands the single-letter codes
  some sending systems use ('M' -> 'MALE', 'F' -> 'FEMALE'). Other non-blank
  values (e.g. ADaM 'INTERSEX', UHAI 'OTHER') are kept as reported; a blank
  becomes null so reports can show it as unknown.
#}
{% macro normalise_sex(expr) -%}
    case upper(nullif(trim({{ expr }}), ''))
        when 'M' then 'MALE'
        when 'F' then 'FEMALE'
        else upper(nullif(trim({{ expr }}), ''))
    end
{%- endmacro %}
