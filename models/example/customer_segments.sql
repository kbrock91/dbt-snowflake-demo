/* This model is an example of an incremental model. It defaults to the Snowflake merge behavior (https://docs.getdbt.com/reference/resource-configs/snowflake-configs#merge-behavior-incremental-models)
when it is rebuilding only the latest roles.

Full documentation: https://docs.getdbt.com/docs/building-a-dbt-project/building-models/configuring-incremental-models

*/


{{
    config(
        materialized = 'incremental',
        unique_key = 'customer_segments_id'
        )

}}
    
    select {{ dbt_utils.generate_surrogate_key(['site_id', 'account_nbr', 'segment_name']) }} as customer_segments_id,
    site_id, account_nbr, segment_name, val, update_timestamp from {{ ref('customer_segments_stg') }} 


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run. Incremental models can be used to only rebuild the defined latest rows without rebuilding the entire table. 
  -- full documentation: https://docs.getdbt.com/docs/building-a-dbt-project/building-models/configuring-incremental-models#understanding-the-is_incremental-macro

 
where customer_segments_id not in (select distinct customer_segments_id from {{ this }})

{% endif %}