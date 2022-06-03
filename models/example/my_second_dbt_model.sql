{{
    config(
        materialized = 'view'
            )
}}

select 
    *, 
              current_timestamp as _loaded_at
from 
    {{ ref('my_first_dbt_model') }}