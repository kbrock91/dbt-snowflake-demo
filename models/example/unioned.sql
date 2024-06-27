{{
    config(
        materialized='view'
    )
}}
{% set number_of_models = 4 %}

{% set model_refs = [] %}

{% for i in range(1, number_of_models+1) %} {# Adjust the range as needed #}
  {% set model_name = 'model_' ~ '%04d' | format(i) %}
  {% do model_refs.append(ref(model_name)) %}
{% endfor %}


{{ dbt_utils.union_relations(relations=model_refs) }}
