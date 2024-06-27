{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

with data as 

( 

    select * 
    from {{ ref('unioned') }}


)

select * from data
{% if is_incremental() %}

    where _loaded_at >= (select max(_loaded_at) from {{ this }})


{% endif %}
