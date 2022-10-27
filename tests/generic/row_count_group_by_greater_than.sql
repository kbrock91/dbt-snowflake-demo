{% test row_count_group_by_greater_than(model, group_by_columns, threshold) %}

with validation as (

    select
        {{group_by_columns}}, 
        count(*) as count


    from {{ model }}

    group by 

    {{group_by_columns}}

),

validation_errors as (

    select
        {{group_by_columns}}

    from validation
    -- if this is true, then row count doesn't meet threshold
    where count < {{ threshold }}

)

select *
from validation_errors

{% endtest %}