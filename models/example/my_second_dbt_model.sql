{{
    config(
        materialized = 'view'
            )
}}

select
    id,
    value * 2 as doubled_value, --change to double_value
    current_timestamp as _loaded_at
from
    {{ ref('stg_salesforce__account') }}
--new comment