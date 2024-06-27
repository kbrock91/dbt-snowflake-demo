{% macro models_from_subfolder(subfolder_path='models/example/input_models') %}
    


{% set model_names = [] -%}

{% for node, node_info in graph.nodes.items() -%}
  {% if node_info.resource_type == 'model' and subfolder_path in node_info.original_file_path -%}
    {% do model_names.append(ref(node_info.name)) -%}
  {% endif -%}
{% endfor -%}

{# Output the list of model names #}
{{ log(model_names, info=True) }}
return {{ model_names }}

{% endmacro %}
