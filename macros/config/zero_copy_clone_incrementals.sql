{% macro clone_modified_incrementals(prod_schema) %}
{{ log('target name is ' ~ target.name) }}

{%- if execute -%}

    {% do log('selected resources are ' ~ selected_resources, info=true) %}

    {%- if target.name == 'ci' -%}
        {{ log('within if loop, target name is ' ~ target.name) }}

        {%- for node in graph.nodes.values() -%}
            

            {%- if node.unique_id in selected_resources and node.resource_type == 'model' and node.config.materialized == 'incremental' -%}
                {{ log('node name is ' ~ node.name) }}

                {% set prod_schema_query %}
                select
                    table_schema
                from {{ target.database }}.information_schema.tables
                where lower(table_schema) = lower('{{prod_schema}}')
                and lower(table_name) = lower('{{ node.name }}'); 
                {% endset %}

                {% set results = run_query(prod_schema_query) %}

                {% if execute %}
                {% set prod_schema_list = results.columns[0].values() %}
                {% else %}
                {% set prod_schema_list = [] %}
                {% endif %}
                
                {{ log('prod_schema_list is ' ~ prod_schema_list) }}


                {% if prod_schema_list|length == 1 %}

                    {% set prod_schema = prod_schema_list[0] %}

                    {%- set from_relation = (adapter.get_relation(database= target.database , schema=prod_schema, identifier=node.name)) -%} 
                    {%- if from_relation.is_table -%}

                    create or replace transient table {{ target.database }}.{{ generate_schema_name(custom_schema_name = node.config.schema, node = node.name) }}.{{ node.name }} clone {{ target.database }}.{{ prod_schema }}.{{ node.name }};

                    {% do log("Cloned incremental model " ~  target.database  ~ "." ~ prod_schema ~ "." ~ node.name ~ " into target schema.", info=true) %}
                
                    {%- endif -%}

                {% elif prod_schema_list|length > 1 %}

                    {{ exceptions.raise_compiler_error("Incremental model " ~ node.name ~ " exists in multiple production schemas: " ~ prod_schema_list) }}

                {% else %}
                    select 1; {# do nothing, if incremental doesnt exist, it should just be created in the PR by default behavior of the incremental model. hooks will error if they dont have valid SQL in them, this handles that! #}
                    
                    {# {{ exceptions.raise_compiler_error("Incremental model " ~ node.name ~ " does not exist in the supplied production database: " ~ prod_db) }} #}
                
                {% endif %}
                
            {%- endif -%}
            
        {%- endfor -%}

        
    
    {%- else -%}

    select 2; {# hooks will error if they dont have valid SQL in them, this handles that! #}
    {{ log('target.name not == ''ci''') }}


    {%- endif -%}

{%- endif -%}

{% endmacro %}