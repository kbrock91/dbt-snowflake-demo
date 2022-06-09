{{
    config(
        materialized = 'table'
    )
}}
with part as (

    select * from {{ref('stg_tpch_parts')}}

),

final as (
    select
        part_key,
        manufacturer,
        name,
        brand,
        size*4,
        container,
        retail_price,
        upper(type),
        comment
    from
        part
)
select *
from final  
order by part_key