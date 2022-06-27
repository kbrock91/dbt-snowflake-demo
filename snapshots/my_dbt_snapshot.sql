{% snapshot my_dbt_snapshot %}

{{
    config(
      target_database='analytics',
      target_schema='dbt_kbrock',
      unique_key='id',
      strategy='check',
      check_cols=["MATH"],
      updated_at='_LOADED_AT'
    )
}}

select * from {{ ref('my_second_dbt_model') }}

{% endsnapshot %}