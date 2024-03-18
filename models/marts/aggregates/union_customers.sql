select * from {{ ref('region_1', 'stg_tpch_customers') }}

union

select * from {{ ref('region_2', 'stg_tpch_customers') }}