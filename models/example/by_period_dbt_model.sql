{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='we_dt'
    )
}}
with data as 

( 

    select * 
    from {{ ref('by_period_source_model') }}
    where we_dt = to_date( '{{ var('dWndDate') }}' )

)

select * from data
