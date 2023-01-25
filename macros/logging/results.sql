{% macro audit_model_start() %}

    insert into dbt_kbrock.dummy_audit_table (
PROCESS_UNIT	,
PROCESS_STATUS_ID	,
SOURCE_ROWS	,
TARGET_ROWS	,
LOAD_DURATION,
CUSTOM_LOGS_JSON	,
RUN_ID,
row_created_date
    )

    values (

        '{{this.name}}', 
        -1, 
        null, 
        null, 
        null,
        null,
        '{{ invocation_id }}',
        current_timestamp::timestamp_ntz
    );

commit
;

{% endmacro %}

{% macro audit_model_end() %}

    update dbt_kbrock.dummy_audit_table 

    set 
        PROCESS_STATUS_ID = 0,
        SOURCE_ROWS	= 0 ,
        TARGET_ROWS	= 0 ,
        LOAD_DURATION = datediff(current_timestamp::timestamp_ntz, row_created_date), 
        CUSTOM_LOGS_JSON = 0 , 
        row_updated_date = current_timestamp::timestamp_ntz 
        

    where 
        RUN_ID = '{{ invocation_id }}'
        and 
        process_unit = '{{this.name}}'


{% endmacro %}
