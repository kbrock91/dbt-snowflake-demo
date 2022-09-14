{{
    config(
        materialized='table'
    )
}}

select * from analytics.dbt_kbrock.my_first_dbt_model