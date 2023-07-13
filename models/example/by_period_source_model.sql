with
    source_data as

    (
        select 1 as id, 1 / 2 as value, to_date('2023-07-01') as we_dt

        union

        select 2 as id, 4 / 2 as value, to_date('2023-07-01') as we_dt

        union
        
        select 1 as id, 1 / 2 as value, to_date('2023-06-30') as we_dt

        union

        select 2 as id, 2 / 2 as value, to_date('2023-06-30') as we_dt

    )

select *
from source_data
