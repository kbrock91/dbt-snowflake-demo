{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change = 'sync_all_columns'
    )
}}

with data as 

( 

with source_data as 

(
    select
        1 as id,        
        10.0::FLOAT as my_test_column
    union 

    select
        2 as id,
        5::FLOAT as my_test_column

)


select * from source_data


    {% if is_incremental() %}

        where id >= (select max(id) from {{ this }})


    {% endif %}

)

select * from data
