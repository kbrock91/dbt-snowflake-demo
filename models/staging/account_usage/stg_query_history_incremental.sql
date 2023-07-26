{{
    config(
        materialized='incremental',
        unique_key = 'QUERY_ID'
    )
}}

select 
    * 
from 
    {{ source('account_usage', 'query_history') }}

{% if is_incremental() %}

    where END_TIME >= (select max(END_TIME) from {{ this }})


{% endif %}
