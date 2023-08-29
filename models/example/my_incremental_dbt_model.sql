{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change = 'sync_all_columns'
    )
}}

with data as 

( 

    select * 
    from {{ ref('my_first_dbt_model') }}

    {% if is_incremental() %}

        where id >= (select max(id) from {{ this }})


    {% endif %}

)

select * from data
