
{{
    config(
        materialized='incremental',
        unique_key='o_orderkey',
        alias = 'orders'
    )
}}

with source as (

    select * from {{ source('tpch', 'orders') }}

),

rename as (

    select
    
        o_orderkey,
        o_custkey,
        o_orderstatus,
        o_totalprice,
        o_orderdate,
        o_orderpriority,
        o_clerk,
        o_shippriority,
        o_comment,
        -- add field for freshness
        -- on full-refresh it will fail freshness
        -- on incremental run it will pass freshness
        
        {% if is_incremental() %}
            getdate()
        {% else %}
            dateadd(day,-1,getdate())
        {% endif %}
            as o_inserted_at

    from source
    {% if is_incremental() %}
        -- we limit the data so we keep some older data on append
        limit 500
    {% endif %}

)

select * from rename