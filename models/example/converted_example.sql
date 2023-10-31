
-- Define the model
{{ config(
    materialized='table'
) }}

-- Common Table Expression (CTE) to transform the data
with stg_node as (
    select
        state::varchar(16777216) as state,
        date::timestamp_ntz(9) as date,
        inpatient_beds_occupied::number(38,0) as inpatient_beds_occupied,
        inpatient_beds_lower_bound::number(38,0) as inpatient_beds_lower_bound,
        inpatient_beds_upper_bound::number(38,0) as inpatient_beds_upper_bound,
        inpatient_beds_in_use_pct::float as inpatient_beds_in_use_pct,
        inpatient_beds_in_use_pct_lower_bound::float as inpatient_beds_in_use_pct_lower_bound,
        inpatient_beds_in_use_pct_upper_bound::float as inpatient_beds_in_use_pct_upper_bound,
        total_inpatient_beds::number(38,0) as total_inpatient_beds,
        total_inpatient_beds_lower_bound::number(38,0) as total_inpatient_beds_lower_bound,
        total_inpatient_beds_upper_bound::number(38,0) as total_inpatient_beds_upper_bound,
        iso3166_1::varchar(2) as iso3166_1,
        iso3166_2::varchar(2) as iso3166_2,
        last_reported_flag::boolean as last_reported_flag
    from {{ source('source1', 'source_model1') }}
)

-- Create the final table
select * from stg_node