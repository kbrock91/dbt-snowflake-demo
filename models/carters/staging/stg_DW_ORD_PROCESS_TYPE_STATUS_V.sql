select * from {{ source("edw", "DW_ORD_PROCESS_TYPE_STATUS_V") }}
