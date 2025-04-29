{% set column_name_query %}

select distinct json.key from {{ source('json','example_json_table') }},
lateral flatten (input => json_data) json

{% endset %}

{% set results = run_query(column_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

select
{% for column_name in results_list %}
json_data:{{ column_name }}::varchar as {{ column_name }}{% if not loop.last %},{% endif %}
{% endfor %}
from {{ source('json','example_json_table') }}