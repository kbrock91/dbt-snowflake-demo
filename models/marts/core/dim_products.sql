{{
    config(
        materialized='table'
    )
}}

with data as ( 

    select
        product_key,
        brand,
        category_name,
        product_name,
        _fivetran_synced
    from 
        {{ ref("stg_products") }}
) 

select             
    *          
from 
    data
