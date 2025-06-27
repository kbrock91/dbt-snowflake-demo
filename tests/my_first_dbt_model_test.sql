

{{
    config(
        enabled=true
            )
}}

with data as ( select * from {{ ref('my_first_dbt_model') }} )

select *
from   data