select 
       order_key,
       sum(total_price) as amount
from {{ ref('region_1','stg_tpch_orders') }}
group by 1
having not (amount >= 0)