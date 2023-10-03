{% snapshot donor_snapshot %}
    {{
        config(
            unique_key='donor_id',
            strategy='timestamp'    ,
            updated_at='updated_dt',
            target_schema = 'dbt_kbrock'    )
    }}

    select * from {{ ref('donors') }}
    
 {% endsnapshot %}