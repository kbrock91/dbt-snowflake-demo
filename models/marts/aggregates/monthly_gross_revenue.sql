{{
    config(
        materialized='table'
    )
}}

with order_items as 
(
    select * from {{ ref('fct_order_items') }}
),

customers as 
(
    select * from {{ ref('dim_customers') }}
)


select 
    date_trunc(MONTH, order_items.order_date) as order_month,
    sum(order_items.gross_item_sales_amount) as gross_revenue
from order_items
left join customers 
    on order_items.customer_key = customers.customer_key
group by 1