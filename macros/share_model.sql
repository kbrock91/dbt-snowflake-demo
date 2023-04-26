{% macro share_model(view_schema, view_name, share_name) %}

    USE ROLE ACCOUNTADMIN;

    GRANT USAGE ON SCHEMA {{ view_schema }} TO SHARE {{ share_name }};
    
    GRANT SELECT ON TABLE {{ view_schema}}.{{ view_name }} TO SHARE {{ share_name }}

{% endmacro %}