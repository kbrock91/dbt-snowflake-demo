{{
    config(
        materialized='table'
    )
}}

with source_data as 

(
    select
        1 as donor_id,
        'Jack' as donor_name,
        to_date('2023-01-01') as updated_dt

    union 

    select
        2 as donor_id,
        'Jill' as donor_name,
        to_date('2023-02-01') as updated_dt

     union 

    select
        3 as donor_id,
        'Ben' as donor_name,
        to_date('2023-02-01') as updated_dt       

    union 

    select
        4 as donor_id,
        'Susie' as donor_name,
        to_date('2023-10-03') as updated_dt     

    union 

    select
        5 as donor_id,
        'Katie' as donor_name,
        to_date('2023-10-03') as updated_dt     

)


select * from source_data
