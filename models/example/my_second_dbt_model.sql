{{
    config(
        materialized = 'view'
            )
}}

select
    id,
    value * 4 as doubled_value, --change to double_value
    current_timestamp as _loaded_at
from
    {{ ref('my_first_dbt_model') }}
