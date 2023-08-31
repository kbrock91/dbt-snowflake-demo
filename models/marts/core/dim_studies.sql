{{
    config(
        materialized='table'
    )
}}


with study as (

    select * from {{ ref('stg_study') }}

),
type as (

    select * from {{ ref('stg_study_type') }}
)

select 
    study.*, 
    type.study_type_desc 

from 
    study 
    
left join type 
    on study.study_type_id = type.study_type_id