{{
    config(
        materialized='incremental',
        unique_key='donor_id'
    )
}}

with data as 

( 

    select * , current_timestamp() as dbt_update_dt
    from {{ ref('donors') }}

    {% if is_incremental() %}

        where updated_dt >= (select max(updated_dt) from {{ this }})


    {% endif %}

)

select * from data
