{% macro where_clause_bad_source_data(source_name, source_table) %}
{%- set where_clause=[] -%}
{{ log('logging empty where clause: ' ~where_clause, info=True) }}
    {%- if execute -%}
    {% set query %}
        select distinct where_clause from analytics.dbt_kbrock.test_results_central where SOURCE_refs = '{{source_name}}.{{source_table}}'
    {% endset %}
    {%- set where_filters = run_query(query).columns[0].values() -%}
    {%- for filter in where_filters -%}
        {%- do where_clause.append(filter) -%}
        {{ log('logging first where clause: ' ~ where_clause, info=True) }}
        {%- do where_clause.append(' AND ') if not loop.last -%}
        {{ log('logging with AND: ' ~ where_clause, info=True) }}

    {%- endfor -%}
    {%- set where_clause_str=where_clause|join('') -%}
    {%- endif -%}

  where {{where_clause_str}}

{% endmacro %}

