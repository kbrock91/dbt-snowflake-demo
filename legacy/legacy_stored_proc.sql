CREATE OR REPLACE PROCEDURE edw.load_dw_ord_txn_dtl_status($1 int4)
	LANGUAGE plpgsql
	SECURITY DEFINER
AS $$
	
	
	
	 	
    /* ***************************************************************************************************
     Proc Name:      LOAD_DW_ORD_TXN_DTL_STATUS
     Created by:     Sreeja Nair
     Created On:     03/01/2019
     Description:    Load table LOAD_DW_ORD_TXN_DTL_STATUS

     Modified On:    05/01/2019 Sreeja Nair
     Reason:         Shortened Desc fields - part of Sterling update .
	 Modified On:    09/30/2019 Siddhesh Sawant
     Reason:         'SCOM_DFS_EA' Added in ORD_SOURCE_ID Column Filter.
     *************************************************************************************************** */
DECLARE
    p_load_run_id ALIAS FOR $1;
    v_START_TIME TIMESTAMP WITHOUT TIME ZONE;
    v_STEP_START_TIME TIMESTAMP WITHOUT TIME ZONE;
    v_END_TIME TIMESTAMP WITHOUT TIME ZONE;
    v_ROW_CNT BIGINT ;--Changed to Bigint from INTEGER by Gatekas
    v_TOT_ROW_CNT BIGINT ;--Changed to Bigint from INTEGER By Gatekas
    v_STEP_SEQ INTEGER;
    v_RNK INTEGER;
    v_MAX_RNK INTEGER DEFAULT 1;
    v_RUNNING_JOB_CNT INTEGER;
    v_RUN_TIME CHARACTER VARYING(15);
    v_PROC_SQL_CODE CHARACTER VARYING(20);
    v_PROC_DISPOSITION CHARACTER VARYING(20);
    v_STEP_NM CHARACTER VARYING(50);
    v_PROC_NM CHARACTER VARYING(50);
    v_PROC_MESSAGE CHARACTER VARYING(200);
    v_LOAD_RUN_ID INTEGER;
    v_LOAD_RUN_ID1 INTEGER;
    v_TRGT_TBL_NM CHARACTER VARYING(50);
    v_STEP_DDL CHARACTER VARYING(4000);
    v_DTL_UPDATETS TIMESTAMP WITHOUT TIME ZONE;
	RETURN_VALUE INTEGER;
BEGIN
    RETURN_VALUE := NULL;
    v_LOAD_RUN_ID := p_load_run_id;
    v_PROC_NM := 'LOAD_DW_ORD_TXN_DTL_STATUS';
    v_TRGT_TBL_NM := 'DW_ORD_TXN_DTL_STATUS';
    v_TOT_ROW_CNT := 0;
    v_START_TIME := SYSDATE::TIMESTAMP;
    v_PROC_SQL_CODE := '0';
    v_PROC_MESSAGE := NULL;
    v_PROC_DISPOSITION := 'RUNNING';
    v_STEP_SEQ := 10;
    v_STEP_NM := 'CREATE TEMP TABLE';
    v_STEP_START_TIME := SYSDATE::TIMESTAMP;
    RAISE NOTICE 'STEP: % START TIME: %' , v_STEP_NM , v_STEP_START_TIME;

    /* ---------------------------------------------------------------------- */
    v_STEP_DDL := 'SELECT COUNT(*) FROM JOB_LOGS WHERE ''STARTED'' AND ''LOAD_DW_ORD_TXN_DTL_STATUS''';
    SELECT
        COUNT(*) INTO v_RUNNING_JOB_CNT
    FROM
        edw.job_logs AS lgs
        INNER JOIN edw.job_steps AS stps ON lgs.jobid = stps.jobid
            AND lgs.jobseq = stps.jobseq
    WHERE
        job_disposition = 'STARTED'
        AND stps.table_name = v_TRGT_TBL_NM
        AND stps.job_ddl LIKE '%LOAD_DW_ORD_TXN_DTL_STATUS%';
              
        RAISE NOTICE '1';
        RAISE NOTICE 'v_RUNNING_JOB_CNT %' , v_RUNNING_JOB_CNT;
        IF v_RUNNING_JOB_CNT > 1 THEN
            RAISE EXCEPTION 'CURRENTLY ANOTHER RUNNING LOAD FOR LOAD_DW_ORD_TXN_DTL_STATUS';
        END IF;
       
        COMMIT;

        /* ----------------------------------------------------------------------------- */
        SELECT
            NVL (MAX(load_run_id)
                , 0) INTO v_LOAD_RUN_ID1
        FROM
            edw.dw_ord_txn_dtl_status;
        IF v_LOAD_RUN_ID = 0 THEN
            v_LOAD_RUN_ID := v_LOAD_RUN_ID1;
        END IF;
        RAISE NOTICE 'LOAD_RUN_ID : %' , v_LOAD_RUN_ID;

        /* ----------------------------------------------------------------------------- */
        SELECT
            NVL (MIN(dwload_updatets::TIMESTAMP)
                , TRUNC(SYSDATE) - 1) INTO v_DTL_UPDATETS
        FROM
            edw.dw_ord_txn_dtl
        WHERE
            dwload_updatets >= (
                SELECT
                    MAX(dwload_updatets::TIMESTAMP)
                FROM
                    edw.dw_ord_txn_dtl_status)
                -- AND ord_source_id = 'SCOM'
				AND ORD_SOURCE_ID in('SCOM','SCOM_DFS_EA') 
			    AND document_type IN ('0001'
                , '0003');
        RAISE NOTICE 'UPDATE DATE FROM DW_ORD_ : %' , v_DTL_UPDATETS;

        /* ----------------------------------------------------------------------------- */
        v_STEP_SEQ := 10;
        v_STEP_NM := 'CREATE TEMP TABLE';
        v_STEP_START_TIME := SYSDATE::TIMESTAMP;
        RAISE NOTICE 'STEP: % START TIME: %' , v_STEP_NM , v_STEP_START_TIME;

        /* ----------------------------------------------------------------------------- */
        /* ----------------------------------------------------------------------------- */
        
        --KB Notes: This temp table is joining together the source tables to prep data to be loaded into the final DTL_STATUS table
        
        CREATE temp TABLE tmp_ORDER_RELEASE_STATUS AS
        SELECT
            d.ORD_SOURCE_ID
            , d.Q_ORD_TXN_HDR_ID AS Q_ORD_TXN_HDR_ID
            , nvl (d.Q_ORD_TXN_HDR_SNUM , ' ') AS Q_ORD_TXN_HDR_SNUM
            , nvl (d.Q_ORD_TXN_DETL_SNUM , ' ') AS Q_ORD_TXN_DETL_SNUM
            , nvl (s.ORDER_RELEASE_STATUS_KEY , ' ') AS Q_ORD_TXN_DETL_STATUS_SNUM
            , d.PRIME_LINE_NO AS PRIME_LINE_NO
            , d.SUB_LINE_NO AS SUB_LINE_NO
            , d.ORDR_NO AS ORDR_NO
            , NVL (d.ORDER_LINE_TYPE , ' ') AS ORDER_LINE_TYPE
            , d.Q_ORD_MD_TYP AS Q_ORD_MD_TYP
            , d.Q_ORD_RTN_FLAG AS Q_ORD_RTN_FLAG
            , d.Q_ORD_DT_ID AS Q_ORD_DT_ID
            , DATE_TRUNC('DAY' , s.status_date) AS Q_ORD_STATUS_DT_ID
            , s.status_date AS Q_ORD_STATUS_TM_ID
            , DATE_PART('HOUR' , s.status_date) || ':' || LPAD(DATE_PART('MINUTES' , s.status_date) , 2 , 0) AS Q_ORD_STATUS_TIME
            , d.Q_SKU_ID AS Q_SKU_ID
            , d.Q_SKU_SNUM AS Q_SKU_SNUM
            , d.Q_STR_ID AS Q_STR_ID
            , d.Q_STR_SNUM AS Q_STR_SNUM
            , d.Q_PICK_STR_ID AS Q_PICK_STR_ID
            , d.Q_PICK_STR_SNUM AS Q_PICK_STR_SNUM
            , NVL (d.ORD_RTL_SKU_ID , ' ') AS ORD_RTL_SKU_ID
            , d.ORD_PICK_STORE_ID AS ORD_PICK_STORE_ID
            , s.STATUS AS STATUS
            , v.STATUS_DESC AS STATUS_DESC
            , d.UNIT_PRICE AS UNIT_PRICE
            , d.ORD_DLRS_ORIG_RETL AS ORD_ORIG_RETL_PRICE
            , d.ORD_DLRS_CURR_RETL AS ORD_CURR_RETL_PRICE
            , d.ORD_DLRS_MSRP AS ORD_MSRP
            , s.STATUS_QUANTITY AS Q_ORD_STATUS_UNITS
            , d.Q_ORD_COGS_CDLRS / nullif (d.Q_ORD_UNITS , 0) * s.STATUS_QUANTITY AS Q_ORD_STATUS_CDLRS
            , d.Q_ORD_RDLRS / nullif (d.Q_ORD_UNITS , 0) * s.STATUS_QUANTITY AS Q_ORD_STATUS_RDLRS
            , d.LINES_PER_ORDER AS LINES_CNT_PER_ORDER
            , d.Q_ORD_UNITS AS Q_ORD_LINE_UNITS
            , d.Q_ORD_COGS_CDLRS AS Q_ORD_LINE_CDLRS
            , d.Q_ORD_RDLRS AS Q_ORD_LINE_RDLRS
            , d.Q_SHARED_ORD_UNITS AS Q_SHARED_ORD_LINE_UNITS
            , d.Q_SHARED_ORD_COGS_CDLRS AS Q_SHARED_ORD_LINE_CDLRS
            , d.Q_SHARED_ORD_RDLRS AS Q_SHARED_ORD_LINE_RDLRS
            , d.BO_FROM_STORE
            , s.CREATETS :: TIMESTAMP
            , s.MODIFYTS AS MODIFYTS
            , CURRENT_TIMESTAMP AS DWLOAD_DATETS
            , CURRENT_TIMESTAMP AS DWLOAD_UPDATETS
            , s.LOAD_ID AS LOAD_ID
            , s.LOAD_RUN_ID AS LOAD_RUN_ID
			, d.SPLIT_FLAG AS SPLIT_FLAG 
			, d.SPLIT_CNT AS SPLIT_CNT
			, d.SPLIT_STR_TYPE AS SPLIT_STR_TYPE
        FROM (
            SELECT
                *
            FROM (
                SELECT
                    ORDER_RELEASE_STATUS_KEY
                    , ORDER_LINE_KEY
                    , ORDER_HEADER_KEY
                    , STATUS
                    , STATUS_DATE
                    , STATUS_QUANTITY
                    , TOTAL_QUANTITY
                    , CREATETS
                    , MODIFYTS
                    , LOAD_ID
                    , LOAD_RUN_ID
                    , ROW_NUMBER() OVER (PARTITION BY ORDER_RELEASE_STATUS_KEY ORDER BY MODIFYTS DESC , LOAD_RUN_ID DESC) AS rnk
                FROM (
                    SELECT
                        ORDER_RELEASE_STATUS_KEY
                        , ORDER_LINE_KEY
                        , ORDER_HEADER_KEY
                        , STATUS
                        , STATUS_DATE
                        , STATUS_QUANTITY
                        , TOTAL_QUANTITY
                        , CREATETS
                        , MODIFYTS
                        , LOAD_ID
                        , LOAD_RUN_ID
                    FROM
                        STG_SC_YFS_ORDER_RELEASE_STATUS
                    WHERE
                        load_run_id > v_LOAD_RUN_ID --what is the purpose of this filter? it is based off a variable, is there another way to flag the records to bring in?
                    UNION ALL
                    SELECT
                        ORDER_RELEASE_STATUS_KEY
                        , ORDER_LINE_KEY
                        , ORDER_HEADER_KEY
                        , STATUS
                        , STATUS_DATE
                        , STATUS_QUANTITY
                        , TOTAL_QUANTITY
                        , CREATETS :: TIMESTAMP
                        , MODIFYTS
                        , LOAD_ID
                        , LOAD_RUN_ID
                    FROM
                        STG_SC_YFS_ORDER_RELEASE_STATUS
                    WHERE
                        ORDER_LINE_KEY IN (
                            SELECT
                                Q_ORD_TXN_DETL_SNUM
                            FROM
                                DW_ORD_TXN_DTL
                            WHERE
                                -- ORD_SOURCE_ID = 'SCOM'
								ORD_SOURCE_ID in('SCOM','SCOM_DFS_EA') 
                                AND DWLOAD_UPDATETS >= v_DTL_UPDATETS --what is the purpose of this filter? potential to explore an incremental model here in the future, but for now we can hardcode 
                                AND DOCUMENT_TYPE IN ('0001' , '0003'))) stg_status) a
                WHERE
                    a.rnk = 1) s
            INNER JOIN DW_ORD_TXN_DTL d ON s.ORDER_LINE_KEY = d.Q_ORD_TXN_DETL_SNUM
                AND s.ORDER_HEADER_KEY = d.Q_ORD_TXN_HDR_SNUM
                AND d.ORD_SOURCE_ID in('SCOM','SCOM_DFS_EA')
                AND d.DOCUMENT_TYPE IN ('0001'
                    , '0003')
        LEFT OUTER JOIN (
        SELECT
            *
        FROM (
            SELECT
                st_status
                , SUBSTRING(TRIM(status_desc) , 1 , 100) AS STATUS_DESC
                , ROW_NUMBER() OVER (PARTITION BY st_status ORDER BY DOCUMENT_TYPE , status_desc) rnk
            FROM DW_ORD_PROCESS_TYPE_STATUS_V) a
    WHERE a.rnk = 1) v ON s.STATUS = v.st_status
        AND v.rnk = 1
    WHERE
        s.rnk = 1;

        /* --------------------------------------------------------------------------- */
        GET DIAGNOSTICS v_ROW_CNT = ROW_COUNT;
        v_TOT_ROW_CNT := v_TOT_ROW_CNT + v_ROW_CNT;
        RAISE NOTICE 'STEP: % ROW COUNT: %' , v_STEP_NM , v_ROW_CNT;
        v_END_TIME := SYSDATE::TIMESTAMP;
        v_RUN_TIME := (v_END_TIME - v_STEP_START_TIME);
        RAISE NOTICE 'STEP: % END TIME: % RUN TIME: %' , v_STEP_NM , v_END_TIME , v_RUN_TIME;

        /* --------------------------------------------------------------------------- */
        /* --------------------------------------------------------------------------- */
        v_STEP_SEQ := 20;
        v_STEP_NM := 'UPDATE DW_ORD_TXN_DTL_STATUS SET';
        v_STEP_START_TIME := SYSDATE::TIMESTAMP;
        RAISE NOTICE 'STEP: % START TIME: %' , v_STEP_NM , v_STEP_START_TIME;
    
        -- KB Notes: This UPDATE is updating records that already exist in DTL_STATUS but are updated in the tmp table based on the unique combo of:
        -- 'q_ord_txn_detl_snum', 'q_ord_txn_detl_status_snum','q_ord_txn_hdr_id', 'q_ord_txn_hdr_snum' 
        -- only update if the values of specific columns have changed
        -- is there a reliable updated timestamp field we could use instead? this could help consolidate inserts/updates 

        UPDATE
            DW_ORD_TXN_DTL_STATUS
        SET
            PRIME_LINE_NO = s.PRIME_LINE_NO
            , SUB_LINE_NO = s.SUB_LINE_NO
            , ORDR_NO = s.ORDR_NO
            , ORDER_LINE_TYPE = s.ORDER_LINE_TYPE
            , Q_ORD_MD_TYP = s.Q_ORD_MD_TYP
            , Q_ORD_RTN_FLAG = s.Q_ORD_RTN_FLAG
            , Q_ORD_DT_ID = s.Q_ORD_DT_ID
            , Q_ORD_STATUS_DT_ID = s.Q_ORD_STATUS_DT_ID
            , Q_ORD_STATUS_TM_ID = s.Q_ORD_STATUS_TM_ID
            , Q_ORD_STATUS_TIME = s.Q_ORD_STATUS_TIME
            , Q_SKU_ID = s.Q_SKU_ID
            , Q_SKU_SNUM = s.Q_SKU_SNUM
            , Q_STR_ID = s.Q_STR_ID
            , Q_STR_SNUM = s.Q_STR_SNUM
            , Q_PICK_STR_ID = s.Q_PICK_STR_ID
            , Q_PICK_STR_SNUM = s.Q_PICK_STR_SNUM
            , ORD_RTL_SKU_ID = s.ORD_RTL_SKU_ID
            , ORD_PICK_STORE_ID = s.ORD_PICK_STORE_ID
            , STATUS = s.STATUS
            , STATUS_DESC = s.STATUS_DESC
            , UNIT_PRICE = s.UNIT_PRICE
            , ORD_ORIG_RETL_PRICE = s.ORD_ORIG_RETL_PRICE
            , ORD_CURR_RETL_PRICE = s.ORD_CURR_RETL_PRICE
            , ORD_MSRP = s.ORD_MSRP
            , Q_ORD_STATUS_UNITS = s.Q_ORD_STATUS_UNITS
            , Q_ORD_STATUS_CDLRS = s.Q_ORD_STATUS_CDLRS
            , Q_ORD_STATUS_RDLRS = s.Q_ORD_STATUS_RDLRS
            , LINES_CNT_PER_ORDER = s.LINES_CNT_PER_ORDER
            , Q_ORD_LINE_UNITS = s.Q_ORD_LINE_UNITS
            , Q_ORD_LINE_CDLRS = s.Q_ORD_LINE_CDLRS
            , Q_ORD_LINE_RDLRS = s.Q_ORD_LINE_RDLRS
			, CREATETS = s.CREATETS
            , Q_SHARED_ORD_LINE_UNITS = s.Q_SHARED_ORD_LINE_UNITS
            , Q_SHARED_ORD_LINE_CDLRS = s.Q_SHARED_ORD_LINE_CDLRS
            , Q_SHARED_ORD_LINE_RDLRS = s.Q_SHARED_ORD_LINE_RDLRS
            , DWLOAD_UPDATETS = CURRENT_TIMESTAMP
			, SPLIT_FLAG = s.SPLIT_FLAG
			, SPLIT_CNT = s.SPLIT_CNT
			, SPLIT_STR_TYPE = s.SPLIT_STR_TYPE
        FROM
            DW_ORD_TXN_DTL_STATUS t
            JOIN tmp_ORDER_RELEASE_STATUS S ON t.ORD_SOURCE_ID in('SCOM','SCOM_DFS_EA')
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
			OR DECODE(t.SPLIT_STR_TYPE , s.SPLIT_STR_TYPE , 1 , 0) = 0);
        
        /* --------------------------------------------------------------------------- */
        GET DIAGNOSTICS v_ROW_CNT = ROW_COUNT;
        v_TOT_ROW_CNT := v_TOT_ROW_CNT + v_ROW_CNT;
        RAISE NOTICE 'STEP: % ROW COUNT: %' , v_STEP_NM , v_ROW_CNT;
        v_END_TIME := SYSDATE::TIMESTAMP;
        v_RUN_TIME := (v_END_TIME - v_STEP_START_TIME);
        RAISE NOTICE 'STEP: % END TIME: % RUN TIME: %' , v_STEP_NM , v_END_TIME , v_RUN_TIME;           
       
        /* --------------------------------------------------------------------------- */
        /* --------------------------------------------------------------------------- */       
       
        v_STEP_SEQ := 30;
        v_STEP_NM := 'INSERT INTO DW_ORD_TXN_DTL_STATUS';
        v_STEP_START_TIME := SYSDATE::TIMESTAMP;
        RAISE NOTICE 'STEP: % START TIME: %' , v_STEP_NM , v_STEP_START_TIME;           

        -- KB Note: This INSERT is updating net new records into DTL_STATUS
        -- new records are identified based on the unique combo of 'q_ord_txn_detl_snum', 'q_ord_txn_detl_status_snum','q_ord_txn_hdr_id', 'q_ord_txn_hdr_snum'

        INSERT INTO DW_ORD_TXN_DTL_STATUS (
            SELECT
                s.ORD_SOURCE_ID
                , s.Q_ORD_TXN_HDR_ID
                , s.Q_ORD_TXN_HDR_SNUM
                , s.Q_ORD_TXN_DETL_SNUM
                , s.Q_ORD_TXN_DETL_STATUS_SNUM
                , s.PRIME_LINE_NO
                , s.SUB_LINE_NO
                , s.ORDR_NO
                , s.ORDER_LINE_TYPE
                , s.Q_ORD_MD_TYP
                , s.Q_ORD_RTN_FLAG
                , s.Q_ORD_DT_ID
                , s.Q_ORD_STATUS_DT_ID
                , s.Q_ORD_STATUS_TM_ID
                , s.Q_ORD_STATUS_TIME
                , s.Q_SKU_ID
                , s.Q_SKU_SNUM
                , s.Q_STR_ID
                , s.Q_STR_SNUM
                , s.Q_PICK_STR_ID
                , s.Q_PICK_STR_SNUM
                , s.ORD_RTL_SKU_ID
                , s.ORD_PICK_STORE_ID
                , s.STATUS
                , s.STATUS_DESC
                , s.UNIT_PRICE
                , s.ORD_ORIG_RETL_PRICE
                , s.ORD_CURR_RETL_PRICE
                , s.ORD_MSRP
                , s.Q_ORD_STATUS_UNITS
                , s.Q_ORD_STATUS_CDLRS
                , s.Q_ORD_STATUS_RDLRS
                , s.LINES_CNT_PER_ORDER
                , s.Q_ORD_LINE_UNITS
                , s.Q_ORD_LINE_CDLRS
                , s.Q_ORD_LINE_RDLRS
                , s.Q_SHARED_ORD_LINE_UNITS
                , s.Q_SHARED_ORD_LINE_CDLRS
                , s.Q_SHARED_ORD_LINE_RDLRS
                , s.BO_FROM_STORE
                , S.CREATETS :: TIMESTAMP
                , s.MODIFYTS:: TIMESTAMP
                , s.DWLOAD_DATETS:: TIMESTAMP
                , s.DWLOAD_UPDATETS:: TIMESTAMP
                , s.LOAD_ID
                , s.LOAD_RUN_ID
				, s.SPLIT_FLAG
				, s.SPLIT_CNT
				, s.SPLIT_STR_TYPE
            FROM
                tmp_ORDER_RELEASE_STATUS S
            WHERE
                NOT EXISTS (
                    SELECT
                        1
                    FROM
                        DW_ORD_TXN_DTL_STATUS T
                    WHERE
                        t.Q_ORD_TXN_DETL_SNUM = s.Q_ORD_TXN_DETL_SNUM
                        AND t.Q_ORD_TXN_DETL_STATUS_SNUM = s.Q_ORD_TXN_DETL_STATUS_SNUM
                        AND t.Q_ORD_TXN_HDR_ID = s.Q_ORD_TXN_HDR_ID
                        AND t.Q_ORD_TXN_HDR_SNUM = s.Q_ORD_TXN_HDR_SNUM
                        AND t.ORD_SOURCE_ID in('SCOM','SCOM_DFS_EA')));
		                        
		GET DIAGNOSTICS v_ROW_CNT = ROW_COUNT;
        RAISE NOTICE 'STEP: % ROW COUNT: %' , v_STEP_NM , v_ROW_CNT;
        v_END_TIME := SYSDATE::TIMESTAMP;
        v_RUN_TIME := (v_END_TIME - v_STEP_START_TIME);
        RAISE NOTICE 'STEP: % END TIME: % RUN TIME: %' , v_STEP_NM , v_END_TIME , v_RUN_TIME;
        RETURN_VALUE := 1;
        RETURN;

        /* ----------------------------------------------------------------------------- */
END;
 


$$
;