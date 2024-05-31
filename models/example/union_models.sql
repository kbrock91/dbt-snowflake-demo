{% set model_names = [ref('model_0001'), ref('model_0002'), ref('model_0006'), ref('model_0003'), ref('model_0004'), ref('model_0005')] -%}

{{ dbt_utils.union_relations(relations=model_names) }}
