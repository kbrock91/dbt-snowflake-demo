{{
    config(
        materialized = 'view'
    )
}}
with part as (

    select * from {{ ref('stg_tpch_parts') }}

),

final as (
    select
        part_key,
        manufacturer,
        name,
        brand,
        1 as size,
        container,
        retail_price,
        type,
    'new_1' as new_col, 
    'another_one' as another_one
    from
        part
)
select *
from final  
order by part_key
