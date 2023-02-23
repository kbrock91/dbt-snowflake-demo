{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}

    {%- if target.name.upper() in ["STAGING", "DEV", "DEMO", "TEST", "LIVE"] and custom_database_name is not none -%}

        {{ custom_database_name | trim }}        

    {%- else -%} --should probably update the order of this conditional so that it checks if target.name = default first

        {{ default_database }}

    {%- endif -%}

{%- endmacro %}