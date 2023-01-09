{{ config(materialized="table") }}

-- change to double_value
select id, value * 2 as doubled_value, current_timestamp as _loaded_at

from {{ source('example', 'my_first_dbt_model') }} 

{{where_clause_bad_source_data(source_name = 'example', source_table = 'my_first_dbt_model' )}}