{{
    config(
        materialized='table', 
        schema='my_own_schema_with_a_capital_s'
    )
}}

select 
1 as id,
'example_name_longer' as name, 
'example_last_name' as last_name,
cast (12123 as varchar) as zip,
'new' as address, 
'21334202342' as phone