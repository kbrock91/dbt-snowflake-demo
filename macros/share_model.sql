{% macro share_model(view_schema, view_name, share_name) %}

      GRANT USAGE ON SCHEMA {{ target.database }}.{{ view_schema }} TO SHARE {{ share_name }};
      GRANT SELECT ON TABLE {{ target.database }}.{{ view_schema}}.{{ view_name }} TO SHARE {{ share_name }}

{% endmacro %}