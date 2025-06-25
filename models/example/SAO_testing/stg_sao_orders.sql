with 

source as (

    select * from {{ source('sao_source', 'view_raw_orders') }}

),

renamed as (

    select
        id,
        product_id,
        order_date

    from source

)

select * from renamed
