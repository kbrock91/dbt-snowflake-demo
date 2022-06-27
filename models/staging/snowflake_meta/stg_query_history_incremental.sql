{{
    config(
        materialized='incremental',
        unique_key = 'QUERY_ID'
    )
}}

select 
    * 
from 
    {{ source('snowflake_meta', 'query_history') }}

{% if is_incremental() %}

    where end_time > (select max(end_time) from {{ this }})


{% endif %}
