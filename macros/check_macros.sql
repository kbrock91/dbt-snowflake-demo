

{% macro check_parent_tags(model) %}
  {{ log('my logging' ~model, info=True) }}
  {% if execute %}
    

  {% set parent_model = model.get('config', {}).get('depends_on', {}) %}

  {{ log(parent_model, info=True) }}
  {% if parent_model is not none %}
    {% set parent_tags = parent_model.get('config', {}).get('tags', []) %}
    {% set child_tags = model.get('config', {}).get('tags', []) %}
    {% for tag in parent_tags %}
      {% if tag not in child_tags %}
        {{ exceptions.raise_compiler_error("Parent model has tag '" ~ tag ~ "' which is not present in child model") }}
      {% endif %}
    {% endfor %}
  {% endif %} 
  {% endif %}
{% endmacro %}

{% macro validate_model_name(model, ruleset=None) %}

  {% if ruleset == None %}
      {{ return() }}
  {% elif ruleset == 'stage' %}  
    {% if not model.identifier.startswith('stg_') %}
      {{ exceptions.raise_compiler_error("Invalid model name validation. Staging models must start with 'stg_'. Got: " ~ model.identifier) }}
    {% endif %}
  {% else %}  
    {{ exceptions.raise_compiler_error("Invalid model name validation ruleset. Got: " ~ ruleset) }}
  {% endif %}
{% endmacro %}