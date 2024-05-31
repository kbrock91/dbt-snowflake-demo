-- depends_on: {{ ref('model_0001') }}
-- depends_on: {{ ref('model_0002') }}
-- depends_on: {{ ref('model_0003') }}
-- depends_on: {{ ref('model_0004') }}


{% set subfolder_path = 'models/example/input_models' -%}  {# Update this path based on your subfolder #}
{% set model_names = [] -%}

{% for node, node_info in graph.nodes.items() -%}
  {% if node_info.resource_type == 'model' and subfolder_path in node_info.original_file_path -%}
    {% do model_names.append(ref(node_info.name)) -%}
  {% endif -%}
{% endfor -%}




{{ dbt_utils.union_relations(relations=model_names) }}