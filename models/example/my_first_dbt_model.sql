with source_data as 

(
    select
        1 as id,
        1 / 2 as value,
        to_date('2023-01-01') as updated_dt

    union 

    select
        2 as id,
        2 / 2 as value,
        to_date('2023-02-01') as updated_dt

     union 

    select
        3 as id,
        2 / 2 as value,
        to_date('2023-02-01') as updated_dt       

    union 

    select
        4 as id,
        2 / 3 as value,
        to_date('2023-10-02') as updated_dt       

)


select * from source_data
