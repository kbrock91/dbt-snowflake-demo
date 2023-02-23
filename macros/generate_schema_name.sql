{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {% set nodepath = node.path.split('/') %}

    {%- if target.name.upper() in ["STAGING", "DEV", "DEMO", "TEST", "LIVE"] and custom_schema_name is not none -%}

        {{ custom_schema_name | trim }}

    {%- else -%}

        {{nodepath[0]|replace("-","_")}}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}