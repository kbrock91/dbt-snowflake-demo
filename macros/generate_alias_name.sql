{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if 'dbt_' in target.schema and node.version and 'dbt_something_is_wrong' not in target.schema -%}
   
        {{ return(node.name ~ "_v" ~ (node.version | replace(".", "_"))) }}

    {%- elif 'dbt_' in target.schema and 'dbt_something_is_wrong' not in target.schema -%}
   
        {{ node.name }}

    {%- elif custom_alias_name -%}

        {{ custom_alias_name | trim }}

    {%- elif node.version -%}

        {{ return(node.name ~ "_v" ~ (node.version | replace(".", "_"))) }}

    {%- else -%}

        {{ node.name }}

    {%- endif -%}

{%- endmacro %}s