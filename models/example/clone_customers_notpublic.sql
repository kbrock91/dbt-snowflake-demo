{{
    config(
        materialized='view',
        grants = {'ownership': ['KBROCK_SECONDARY_ROLE_ACCESS_TEST']}
            )
}}

select * from raw.tpch_sf001_clone_kb.customer