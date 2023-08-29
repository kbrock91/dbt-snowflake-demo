with source_data as 

(
    select
        -0.0::NUMBER as my_test_column, 
        1 as id,
        1 / 2 as value

    union 

    select
        -0.0::NUMBER as my_test_column, 
        2 as id,
        2 / 2 as value
   union 

    select
        4.0::NUMBER as my_test_column, 
        3 as id,
        2 / 2 as value
   union 

    select
        4.0::NUMBER as my_test_column, 
        4 as id,
        2 / 2 as value
           union 

    select
        4.0::NUMBER as my_test_column, 
        5 as id,
        2 / 2 as value           
        union 

    select
        4.0::NUMBER as my_test_column, 
        6 as id,
        2 / 2 as value
)


select * from source_data
