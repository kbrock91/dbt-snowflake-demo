{% snapshot my_dbt_snapshot %}
    {{
        config(
            unique_key='new_id',
            strategy='check'    ,
            check_cols = ['value'],
            target_schema = 'dbt_kbrock',
            invalidate_hard_deletes=True    )
    }}

    select id as new_id, value from {{ ref('my_first_dbt_model') }}
    
 {% endsnapshot %}