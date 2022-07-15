select 
       order_key,
       sum(total_price) as amount
from {{ ref('stg_tpch_orders') }}
group by 1
having not (amount >= 0)