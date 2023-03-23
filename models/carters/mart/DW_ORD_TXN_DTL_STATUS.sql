{{
    config(
        materialized='incremental',
        unique_key=['q_ord_txn_detl_snum', 'q_ord_txn_detl_status_snum','q_ord_txn_hdr_id', 'q_ord_txn_hdr_snum']
    )
}}

select
    s.ord_source_id,
    s.q_ord_txn_hdr_id,
    s.q_ord_txn_hdr_snum,
    s.q_ord_txn_detl_snum,
    s.q_ord_txn_detl_status_snum,
    s.prime_line_no,
    s.sub_line_no,
    s.ordr_no,
    s.order_line_type,
    s.q_ord_md_typ,
    s.q_ord_rtn_flag,
    s.q_ord_dt_id,
    s.q_ord_status_dt_id,
    s.q_ord_status_tm_id,
    s.q_ord_status_time,
    s.q_sku_id,
    s.q_sku_snum,
    s.q_str_id,
    s.q_str_snum,
    s.q_pick_str_id,
    s.q_pick_str_snum,
    s.ord_rtl_sku_id,
    s.ord_pick_store_id,
    s.status,
    s.status_desc,
    s.unit_price,
    s.ord_orig_retl_price,
    s.ord_curr_retl_price,
    s.ord_msrp,
    s.q_ord_status_units,
    s.q_ord_status_cdlrs,
    s.q_ord_status_rdlrs,
    s.lines_cnt_per_order,
    s.q_ord_line_units,
    s.q_ord_line_cdlrs,
    s.q_ord_line_rdlrs,
    s.q_shared_ord_line_units,
    s.q_shared_ord_line_cdlrs,
    s.q_shared_ord_line_rdlrs,
    s.bo_from_store,
    s.createts::timestamp,
    s.modifyts::timestamp,
    s.dwload_datets::timestamp,
    s.dwload_updatets::timestamp,
    s.load_id,
    s.load_run_id,
    s.split_flag,
    s.split_cnt,
    s.split_str_type
from {{ ref('tmp_ORDER_RELEASE_STATUS') }} s

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    -- if it is the first run -> you will want to insert ALL records and will have no updated records to union together
    -- on subsequent incremental runs, it will only merge NEW or UPDATED records
    
    where
    
    --this filter grabs the new records

    not exists (
        select 1
        from {{this}} t
        where
            t.q_ord_txn_detl_snum = s.q_ord_txn_detl_snum
            and t.q_ord_txn_detl_status_snum = s.q_ord_txn_detl_status_snum
            and t.q_ord_txn_hdr_id = s.q_ord_txn_hdr_id
            and t.q_ord_txn_hdr_snum = s.q_ord_txn_hdr_snum
            and t.ord_source_id in ('SCOM', 'SCOM_DFS_EA')
    )

union

--this union appends the updated records 

select 
s.prime_line_no,
s.sub_line_no,
s.ordr_no,
s.order_line_type,
s.q_ord_md_typ,
s.q_ord_rtn_flag,
s.q_ord_dt_id,
s.q_ord_status_dt_id,
s.q_ord_status_tm_id,
s.q_ord_status_time,
s.q_sku_id,
s.q_sku_snum,
s.q_str_id,
s.q_str_snum,
s.q_pick_str_id,
s.q_pick_str_snum,
s.ord_rtl_sku_id,
s.ord_pick_store_id,
s.status,
s.status_desc,
s.unit_price,
s.ord_orig_retl_price,
s.ord_curr_retl_price,
s.ord_msrp,
s.q_ord_status_units,
s.q_ord_status_cdlrs,
s.q_ord_status_rdlrs,
s.lines_cnt_per_order,
s.q_ord_line_units,
s.q_ord_line_cdlrs,
s.q_ord_line_rdlrs,
s.q_shared_ord_line_units,
s.q_shared_ord_line_cdlrs,
s.q_shared_ord_line_rdlrs,
s.bo_from_store,
s.createts::timestamp,
s.modifyts::timestamp,
t.dwload_datets::timestamp,
current_timestamp as dwload_updatets,
s.load_id,
s.load_run_id,
s.split_flag,
s.split_cnt,
s.split_str_type

from 
{{this}} t
            JOIN {{ ref('tmp_ORDER_RELEASE_STATUS') }} S ON t.ORD_SOURCE_ID in('SCOM','SCOM_DFS_EA')
                AND t.Q_ORD_TXN_DETL_STATUS_SNUM = s.Q_ORD_TXN_DETL_STATUS_SNUM
                AND t.Q_ORD_TXN_HDR_ID = s.Q_ORD_TXN_HDR_ID
                AND t.Q_ORD_TXN_HDR_SNUM = s.Q_ORD_TXN_HDR_SNUM
                AND t.Q_ORD_TXN_DETL_SNUM = s.Q_ORD_TXN_DETL_SNUM
        WHERE (DECODE(t.PRIME_LINE_NO , s.PRIME_LINE_NO , 1 , 0) = 0
            OR DECODE(t.SUB_LINE_NO , s.SUB_LINE_NO , 1 , 0) = 0
            OR DECODE(t.ORDR_NO , s.ORDR_NO , 1 , 0) = 0
            OR DECODE(t.ORDER_LINE_TYPE , s.ORDER_LINE_TYPE , 1 , 0) = 0
            OR DECODE(t.Q_ORD_MD_TYP , s.Q_ORD_MD_TYP , 1 , 0) = 0
            OR DECODE(t.Q_ORD_RTN_FLAG , s.Q_ORD_RTN_FLAG , 1 , 0) = 0
            OR DECODE(t.Q_ORD_DT_ID , s.Q_ORD_DT_ID , 1 , 0) = 0
            OR DECODE(t.Q_ORD_STATUS_DT_ID , s.Q_ORD_STATUS_DT_ID , 1 , 0) = 0
            OR DECODE(t.Q_ORD_STATUS_TM_ID , s.Q_ORD_STATUS_TM_ID , 1 , 0) = 0
            OR DECODE(t.Q_ORD_STATUS_TIME , s.Q_ORD_STATUS_TIME , 1 , 0) = 0
            OR DECODE(t.Q_SKU_ID , s.Q_SKU_ID , 1 , 0) = 0
            OR DECODE(t.Q_SKU_SNUM , s.Q_SKU_SNUM , 1 , 0) = 0
            OR DECODE(t.Q_STR_ID , s.Q_STR_ID , 1 , 0) = 0
            OR DECODE(t.Q_STR_SNUM , s.Q_STR_SNUM , 1 , 0) = 0
            OR DECODE(t.Q_PICK_STR_ID , s.Q_PICK_STR_ID , 1 , 0) = 0
            OR DECODE(t.Q_PICK_STR_SNUM , s.Q_PICK_STR_SNUM , 1 , 0) = 0
            OR DECODE(t.ORD_RTL_SKU_ID , s.ORD_RTL_SKU_ID , 1 , 0) = 0
            OR DECODE(t.ORD_PICK_STORE_ID , s.ORD_PICK_STORE_ID , 1 , 0) = 0
            OR DECODE(t.STATUS , s.STATUS , 1 , 0) = 0
            OR DECODE(t.STATUS_DESC , s.STATUS_DESC , 1 , 0) = 0
            OR DECODE(t.UNIT_PRICE , s.UNIT_PRICE , 1 , 0) = 0
            OR DECODE(t.ORD_ORIG_RETL_PRICE , s.ORD_ORIG_RETL_PRICE , 1 , 0) = 0
            OR DECODE(t.ORD_CURR_RETL_PRICE , s.ORD_CURR_RETL_PRICE , 1 , 0) = 0
            OR DECODE(t.ORD_MSRP , s.ORD_MSRP , 1 , 0) = 0
            OR DECODE(t.Q_ORD_STATUS_UNITS , s.Q_ORD_STATUS_UNITS , 1 , 0) = 0
            OR DECODE(t.Q_ORD_STATUS_CDLRS , s.Q_ORD_STATUS_CDLRS , 1 , 0) = 0
            OR DECODE(t.Q_ORD_STATUS_RDLRS , s.Q_ORD_STATUS_RDLRS , 1 , 0) = 0
            OR DECODE(t.LINES_CNT_PER_ORDER , s.LINES_CNT_PER_ORDER , 1 , 0) = 0
            OR DECODE(t.Q_ORD_LINE_UNITS , s.Q_ORD_LINE_UNITS , 1 , 0) = 0
            OR DECODE(t.Q_ORD_LINE_CDLRS , s.Q_ORD_LINE_CDLRS , 1 , 0) = 0
            OR DECODE(t.Q_ORD_LINE_RDLRS , s.Q_ORD_LINE_RDLRS , 1 , 0) = 0
            OR DECODE(t.Q_SHARED_ORD_LINE_UNITS , s.Q_SHARED_ORD_LINE_UNITS , 1 , 0) = 0
            OR DECODE(t.Q_SHARED_ORD_LINE_CDLRS , s.Q_SHARED_ORD_LINE_CDLRS , 1 , 0) = 0
            OR DECODE(t.Q_SHARED_ORD_LINE_RDLRS , s.Q_SHARED_ORD_LINE_RDLRS , 1 , 0) = 0
			OR DECODE(t.SPLIT_FLAG , s.SPLIT_FLAG , 1 , 0) = 0
			OR DECODE(t.SPLIT_CNT , s.SPLIT_CNT , 1 , 0) = 0
			OR DECODE(t.SPLIT_STR_TYPE , s.SPLIT_STR_TYPE , 1 , 0) = 0)
{% endif %}