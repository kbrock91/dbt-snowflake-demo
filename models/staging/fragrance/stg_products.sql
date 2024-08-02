with 

    source as (
        
        select 
            * 
        from 
            {{ source("google_sheets", "fragrance_products") }}
    ),

    renamed as (

        select
            product_key,
            brand,
            decode(
                category, 
                'B', 'Beauty', 
                'C', 'Chemical', 
                'F', 'Food', 
                'Other'
            ) as category_name,
            product_name,
            _fivetran_synced

        from source

    )

select *
from renamed
