{{
    config(
        materialized = 'table'
            )
}}

select
    id,
    value * 2 as doubled_value, --change to double_value
    '2024-04-18T09:29:46.662000-07:00'  as _loaded_at
from
    {{ ref('my_first_dbt_model') }}
