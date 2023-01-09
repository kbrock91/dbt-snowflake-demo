with source_data as 

(
    select
        1 as id,
        1 / 2 as value,
        'hello' as text

    union 

    select
        1 as id,
        2 / 2 as value,
        'hello' as text

    union 

    select
        2 as id,
        2 / 2 as value,
        'hello' as text
            union 

    select
        3 as id,
        2 / 2 as value,
        'hello3' as text

                    union 

    select
       null as id,
        2 / 2 as value,
        'hello4' as text           
        union 

    select
       null as id,
        2 / 2 as value,
        'hello' as text

            union 

    select
       3 as id,
        2 / 2 as value,
        'hello' as text
)


select * from source_data
