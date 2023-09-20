{{
    config(
        materialized='table'
    )
}}

select * from {{ ref('salesforce__daily_activity') }}
where date_day >=  current_date - interval '30 days' 