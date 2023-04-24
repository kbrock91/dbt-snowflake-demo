{{
    config(
        materialized='table', 
        pre_hook= "{{create_share('kbrock_share_dev', 'UTB91939') }}",
        post_hook = "{{share_model(  'dbt_kbrock', 'yearly_gross_revenue','kbrock_share_dev')}}"
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
    date_trunc(year, order_items.order_date) as order_year,
    sum(order_items.gross_item_sales_amount) as gross_revenue
from order_items
left join customers 
    on order_items.customer_key = customers.customer_key
group by 1