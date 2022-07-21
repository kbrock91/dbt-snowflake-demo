{% snapshot snapshot_dim_customers %}

{{
    config(
        unique_key = 'customer_key',
        strategy   = 'timestamp',
        target_schema = target.schema,
        updated_at = 'updated_date'
    )
}}

select * from {{ ref('dim_customers') }}

{% endsnapshot %}