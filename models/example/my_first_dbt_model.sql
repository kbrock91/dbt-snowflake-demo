with source_data as 

(
    select
        1111 as id,
        1 / 2 as value

    union 

    select
        2 as id,
        2 / 2 as value

            union 

    select
        7 as id,
        2 / 2 as value

                union 

    select
        9 as id,
        2 / 2 as value
)


select * from source_data
