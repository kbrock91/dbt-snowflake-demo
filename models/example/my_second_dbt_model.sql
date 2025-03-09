{{
    config(
        materialized = 'table'
            )
}}

select
    1 as id,
    value * 2 as doubled_value, --change to double_value
    current_timestamp as _loaded_at, 
'{{ env_var('DBT_CLOUD_RUN_REASON', 'default') }}' as dbt_cloud_run_reason
from
    {{ ref('my_first_dbt_model') }}
