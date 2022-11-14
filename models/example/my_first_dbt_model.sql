with source_data as 

(
    select
        1 as id,
        1 / 2 as value

    union 

    select
        2 as id,
        2 / 3 as value

)


select * from source_data
