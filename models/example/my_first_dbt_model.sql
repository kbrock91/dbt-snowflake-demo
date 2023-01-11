{{
    config(
        pre_hook="{{use_role('team_1_role')}}"
    )
}}

with source_data as 

(
    select
        1 as id,
        1 / 2 as value

    union 

    select
        2 as id,
        2 / 2 as value

)


select * from source_data
