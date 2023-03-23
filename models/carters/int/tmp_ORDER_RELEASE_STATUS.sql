--TODO set values for v_load_run_id and  v_DTL_UPDATETS

{% set v_load_run_id = '1000' %}
{% set v_DTL_UPDATETS = 'dateadd(day,-3,current_timestamp)' %}

select
    d.ord_source_id,
    d.q_ord_txn_hdr_id as q_ord_txn_hdr_id,
    nvl(d.q_ord_txn_hdr_snum, ' ') as q_ord_txn_hdr_snum,
    nvl(d.q_ord_txn_detl_snum, ' ') as q_ord_txn_detl_snum,
    nvl(s.order_release_status_key, ' ') as q_ord_txn_detl_status_snum,
    d.prime_line_no as prime_line_no,
    d.sub_line_no as sub_line_no,
    d.ordr_no as ordr_no,
    nvl(d.order_line_type, ' ') as order_line_type,
    d.q_ord_md_typ as q_ord_md_typ,
    d.q_ord_rtn_flag as q_ord_rtn_flag,
    d.q_ord_dt_id as q_ord_dt_id,
    date_trunc('DAY', s.status_date) as q_ord_status_dt_id,
    s.status_date as q_ord_status_tm_id,
    date_part('HOUR', s.status_date)
    || ':'
    || lpad(date_part('MINUTES', s.status_date), 2, 0) as q_ord_status_time,
    d.q_sku_id as q_sku_id,
    d.q_sku_snum as q_sku_snum,
    d.q_str_id as q_str_id,
    d.q_str_snum as q_str_snum,
    d.q_pick_str_id as q_pick_str_id,
    d.q_pick_str_snum as q_pick_str_snum,
    nvl(d.ord_rtl_sku_id, ' ') as ord_rtl_sku_id,
    d.ord_pick_store_id as ord_pick_store_id,
    s.status as status,
    v.status_desc as status_desc,
    d.unit_price as unit_price,
    d.ord_dlrs_orig_retl as ord_orig_retl_price,
    d.ord_dlrs_curr_retl as ord_curr_retl_price,
    d.ord_dlrs_msrp as ord_msrp,
    s.status_quantity as q_ord_status_units,
    d.q_ord_cogs_cdlrs
    / nullif(d.q_ord_units, 0)
    * s.status_quantity as q_ord_status_cdlrs,
    d.q_ord_rdlrs / nullif(d.q_ord_units, 0) * s.status_quantity as q_ord_status_rdlrs,
    d.lines_per_order as lines_cnt_per_order,
    d.q_ord_units as q_ord_line_units,
    d.q_ord_cogs_cdlrs as q_ord_line_cdlrs,
    d.q_ord_rdlrs as q_ord_line_rdlrs,
    d.q_shared_ord_units as q_shared_ord_line_units,
    d.q_shared_ord_cogs_cdlrs as q_shared_ord_line_cdlrs,
    d.q_shared_ord_rdlrs as q_shared_ord_line_rdlrs,
    d.bo_from_store,
    s.createts::timestamp,
    s.modifyts as modifyts,
    current_timestamp as dwload_datets,
    current_timestamp as dwload_updatets,
    s.load_id as load_id,
    s.load_run_id as load_run_id,
    d.split_flag as split_flag,
    d.split_cnt as split_cnt,
    d.split_str_type as split_str_type
from
    (
        select *
        from
            (
                select
                    order_release_status_key,
                    order_line_key,
                    order_header_key,
                    status,
                    status_date,
                    status_quantity,
                    total_quantity,
                    createts,
                    modifyts,
                    load_id,
                    load_run_id,
                    row_number() over (
                        partition by order_release_status_key
                        order by modifyts desc, load_run_id desc
                    ) as rnk
                from
                    (
                        select
                            order_release_status_key,
                            order_line_key,
                            order_header_key,
                            status,
                            status_date,
                            status_quantity,
                            total_quantity,
                            createts,
                            modifyts,
                            load_id,
                            load_run_id
                        from {{ ref('stg_STG_SC_YFS_ORDER_RELEASE_STATUS') }}
                        where load_run_id >  {{v_load_run_id}} -- v_load_run_id, hardcoded for now, this can be built incrementally
                        union all
                        select
                            order_release_status_key,
                            order_line_key,
                            order_header_key,
                            status,
                            status_date,
                            status_quantity,
                            total_quantity,
                            createts::timestamp,
                            modifyts,
                            load_id,
                            load_run_id
                        from {{ ref('stg_STG_SC_YFS_ORDER_RELEASE_STATUS') }}
                        where
                            order_line_key in (
                                select q_ord_txn_detl_snum
                                from {{ ref('stg_DW_ORD_TXN_DTL') }}
                                where
                                    -- ORD_SOURCE_ID = 'SCOM'
                                    ord_source_id in ('SCOM', 'SCOM_DFS_EA')
                                    and dwload_updatets >= {{v_DTL_UPDATETS}}   -- v_DTL_UPDATETS, hardcoded for now, this can be built incrementally 
                                    and document_type in ('0001', '0003')
                            )
                    ) stg_status
            ) a
        where a.rnk = 1
    ) s
inner join
    {{ ref('stg_DW_ORD_TXN_DTL') }} d
    on s.order_line_key = d.q_ord_txn_detl_snum
    and s.order_header_key = d.q_ord_txn_hdr_snum
    and d.ord_source_id in ('SCOM', 'SCOM_DFS_EA')
    and d.document_type in ('0001', '0003')
left outer join
    (
        select *
        from
            (
                select
                    st_status,
                    substring(trim(status_desc), 1, 100) as status_desc,
                    row_number() over (
                        partition by st_status order by document_type, status_desc
                    ) rnk
                from {{ ref('stg_DW_ORD_PROCESS_TYPE_STATUS_V') }}
            ) a
        where a.rnk = 1
    ) v
    on s.status = v.st_status
    and v.rnk = 1
where s.rnk = 1