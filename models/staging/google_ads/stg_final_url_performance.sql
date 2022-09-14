{{
    config(
        materialized='view'
    )
}}

select 
    _fivetran_id as _fivetran_key,
    customer_id as customey_key,
    date,
    _fivetran_synced,
    account_descriptive_name,
    campaign_id as campaign_key,
    campaign_name,
    campaign_status,
    ad_group_id as ad_group_key,
    ad_group_name,
    ad_group_status,
    clicks,
    cost,
    effective_final_url,
    impressions,
    external_customer_id as external_customer_key

from 
    {{ source('google_ads', 'final_url_performance') }}