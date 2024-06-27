{% macro get_models_in_subfolder(subfolder_path) %}
  {% set result_models = [] %}

  {# Loop through all nodes in the graph #}
  {% for node, attributes in graph.nodes.items() %}
    {# Check if the node is a model and if its path matches the subfolder path #}
    {% if attributes.resource_type == 'model' and subfolder_path in attributes.path %}
      {% do result_models.append(node) %}
    {% endif %}
  {% endfor %}

  {# Return the list of filtered models #}
  {{ return(result_models) }}
{% endmacro %}