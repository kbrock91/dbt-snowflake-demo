{% macro create_share(share_name, account) %}

    USE ROLE ACCOUNTADMIN;

    CREATE SHARE IF NOT EXISTS {{ share_name }};
    GRANT USAGE ON DATABASE {{ target.database }} TO SHARE {{ share_name }};
    ALTER SHARE {{ share_name }} ADD ACCOUNTS = {{ account }};

{% endmacro %}