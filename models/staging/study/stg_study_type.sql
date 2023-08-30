with 

source as (

    select * from {{ source('study_data', 'raw_study_type') }}

),

renamed as (

    select
        study_type_id,
        study_type_desc

    from source

)

select * from renamed
