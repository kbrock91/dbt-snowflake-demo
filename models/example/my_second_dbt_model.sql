{{
    config(
        materialized = 'view'
            )
}}

select
    *,
    current_timestamp as _loaded_at
from
    {{ ref('stg_salesforce__account') }}
