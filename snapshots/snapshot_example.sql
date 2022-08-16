{% snapshot snapshot_example_new %}
    {{
        config(
            unique_key='new_id',
            strategy='check'    ,
            check_cols = ['value'],
            target_schema = 'dbt_kbrock'    )
    }}

    select id as new_id, value from {{ ref('my_first_dbt_model') }}
    
 {% endsnapshot %}