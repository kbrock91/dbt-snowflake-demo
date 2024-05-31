{% set model_names = [ref('model_0002'), ref('model_0001')] -%}

{{ dbt_utils.union_relations(relations=model_names) }}
