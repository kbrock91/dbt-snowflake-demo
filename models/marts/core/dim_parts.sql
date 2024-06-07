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
        1 as part_key,
        manufacturer,
        name,
        brand,
        size,
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
