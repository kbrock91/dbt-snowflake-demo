{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

with data as 

( 

    select 4 as new, * 
    from {{ ref('my_first_dbt_model') }}

    {% if is_incremental() %}

        where id >= (select max(id) from {{ this }})


    {% endif %}

)

select * from data
