with source_data as 

(
    select
    1 as id,
    1 / 3 as math

    union 

    select
        2 as id,
        2 / 2 as math


    union 

    select
        3 as id,
        3 / 2 as math

    union 

    select
        4 as id,
        4 / 2 as math
)

select * from source_data 
