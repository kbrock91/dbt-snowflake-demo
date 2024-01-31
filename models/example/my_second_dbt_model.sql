{{
    config(
        materialized = 'table'
            )
}}

select
    id,
    value * 2 as doubled_value, --change to double_value
    current_timestamp as _loaded_at,
    joke_python() as joke
from
    {{ ref('my_first_dbt_model') }}
