{% macro clone_modified_incrementals(from_db, from_schema) %}

{%- if execute -%}

    {%- if target.name == 'ci' -%}
    
        {%- for node in graph.nodes.values() -%}
            {%- if node.unique_id in selected_resources and node.resource_type == 'model' and node.config.materialized == 'incremental' -%}
                {{ log('node is selected: ' ~ node.name) }}
                {%- set from_relation = (adapter.get_relation(database=from_db, schema=from_schema, identifier=node.name)) -%} 
                {%- if from_relation.is_table -%}

                create or replace transient table {{ target.database }}.{{ generate_schema_name(custom_schema_name = node.config.schema, node = node.name) }}.{{ node.name }} clone {{ from_db }}.{{ from_schema }}.{{ node.name }};
                
                {% do log("Cloned incremental model " ~ from_db ~ "." ~ from_schema ~ "." ~ node.name ~ " into target schema.", info=true) %}
                
                {%- endif -%}
                
            {%- endif -%}
            {%- else -%}
            select 1; {# hooks will error if they dont have valid SQL in them, this handles that! #}
        {%- endfor -%}

        select 2; {# hooks will error if they dont have valid SQL in them, this handles that! #}
    
    {%- else -%}

    select 3; {# hooks will error if they dont have valid SQL in them, this handles that! #}

    {%- endif -%}

{%- endif -%}

{% endmacro %}