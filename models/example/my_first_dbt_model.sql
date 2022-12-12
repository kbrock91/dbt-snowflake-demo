with
    source_data as

    (
        select null as id, 1 / 2 as value

        union

        select 2 as id, 2 / 2 as value

        union

        select 3 as id, 2 / 3 as value

        union

        select 4 as id, 2 / 4 as value


    )

select *
from source_data
