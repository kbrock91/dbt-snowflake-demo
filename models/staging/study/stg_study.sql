with 

source as (

    select * from {{ source('study_data', 'raw_study') }}

),

renamed as (

    select
        study_id,
        study_title,
        study_version,
        study_tag,
        study_type_id,
        study_phase_id,
        case 
            when study_status = 'valid'  then  1
            when study_status = 'invalid'  then 2
        end 
            as study_status,
        study_protocol_id,
        study_protocol_version

    from source

)

select * from renamed
