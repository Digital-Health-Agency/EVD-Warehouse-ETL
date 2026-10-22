{#
  Whitespace-free LIMS link key.

  Source systems reference LIMS identifiers with stray whitespace
  (NPHIIS: 'NVRL/ 1098/ 26' ~ 'NVRL/1098/26'). Every marts/gold model
  that links a case record to LIMS must use this same normalisation so
  the rule cannot drift between layers.
#}
{% macro lims_link_key(expr) %}regexp_replace({{ expr }}, '\s', '', 'g'){% endmacro %}
