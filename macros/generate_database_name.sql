{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}

    {%- if target.name.upper() in ["STAGING", "DEV", "DEMO", "TEST", "LIVE"] and custom_database_name is not none -%}

        {{ custom_database_name | trim }}        

    {%- else -%}

        {{ default_database }}

    {%- endif -%}

{%- endmacro %}