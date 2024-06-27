{% set sql_statement %}
        select 
            ID,
            NAME, 
            SEGMENT_TYPE, 
            sf_logic_query from {{ source('segway_example', 'segway_metadata') }}
{% endset %}
  
{% set query_result = run_query(sql_statement) %}

{% if execute %}
    {% set queries = query_result.columns[3].values() %}
    {% set union_sql = [] %}
    {% for row in query_result.rows %}

        {% set segment_id=row[0] %}
        {% set segment_name=row[1] %}
        {% set code_type=row[2] %}
        {% set query = row[3] %}

        {% set query_statement %}

            with data as (
                {query}
            )
        
            SELECT data.SITE_ID as SITE_ID,
            data.HOUSE_NBR as HOUSE_NBR,
            data.VAL as VAL,
            '{{segment_id}}' as SEGMENT_ID,
            '{{segment_name}}' as SEGMENT_NAME,
            '{{code_type}}' as CODE_TYPE,
            from  data
        {% endset %}
        
        {% do union_sql.append('(' ~ query ~ ')') %}

    {% endfor %}
    {{ log(union_sql, info=True) }}
    {% set final_sql = ' UNION ALL '.join(union_sql) %}
    {{ log(final_sql, info=True) }}
{% endif %}

{{final_sql}}
