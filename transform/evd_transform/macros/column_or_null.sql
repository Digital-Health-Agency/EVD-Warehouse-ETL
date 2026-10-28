{#
  Select a bronze column if the table has it, else a typed null with the
  same name. Bronze only creates a column after it has seen a value, so a
  field a source has only just started sending (or has never filled) may be
  absent, and a hard-coded reference would fail the whole model.

  columns: lower-cased column names of the relation, e.g.
           adapter.get_columns_in_relation(rel) | map(attribute='name') | map('lower') | list
  column:  bronze column to read
  expr:    optional SQL expression over that column (default: the column)
  type:    SQL type of the null used when the column is absent
  alias:   output name (default: column)
#}
{% macro column_or_null(columns, column, expr=none, type='text', alias=none) -%}
  {%- set name = alias or column -%}
  {%- if column | lower in columns -%}
    {{ expr if expr else column }} as {{ name }}
  {%- else -%}
    null::{{ type }} as {{ name }}
  {%- endif -%}
{%- endmacro %}
