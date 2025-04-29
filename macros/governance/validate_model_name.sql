{% macro validate_model_name() %}
    {{ log('validating model name for naming convention standards: ' ~ model.path, info=True) }}
    {% if model.path.startswith('staging/') %}
        {% if not model.name.startswith('stg_') %}
            {{ exceptions.raise_compiler_error("Invalid model name validation. Staging models must start with 'stg_'. Got: " ~ model.identifier) }}            
        {% endif %}
    {% endif %}
{% endmacro %}