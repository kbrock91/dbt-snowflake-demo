{% snapshot my_dbt_snapshot %}
    {{
        config(
            unique_key='id',
            strategy='check'    ,
            check_cols = ['value'],
            target_schema = 'dbt_kbrock_snapshot',
            invalidate_hard_deletes=True    )
    }}

    select id, value from {{ ref('my_first_dbt_model') }}
    
 {% endsnapshot %}