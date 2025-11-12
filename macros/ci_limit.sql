{% macro ci_limit() -%}
  {# set row count limit on base tables for ci runs - '0' is default value if not explicitly defined #}
  {% if target.name == 'ci' %}
    limit 0
  {% endif %}
{%- endmacro %}