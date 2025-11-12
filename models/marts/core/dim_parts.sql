{{
    config(
        materialized = 'table'
    )
}}
with part as (

    select * from {{ ref('stg_tpch_parts') }}

),

final as (
    select
        part_key,
        manufacturer,
        'dummy_name' as name,
        brand,
        size,
        container as new_container, 
        retail_price,
        type,
    'new_1' as NEW_COL
    from
        part
)
select *
from final  
order by part_key, manufacturer 
