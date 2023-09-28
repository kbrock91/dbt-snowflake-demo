-- depends_on: {{ ref('stg_protocol_roles') }}

{% macro create_roles(model) %}
    {% if execute %}

        {% set ls_action = "CREATE" %}

        {% set roles_query %}

            select * from {{this}}

        {% endset %}

        {% set roles = run_query(roles_query).rows %}

        {{ log(roles, info=True) }}
        {% if roles %}
            {% for role in roles %}
                {{ log(role[0], info=True) }}

                {% set userid = role[0] %}
                {% set user_email = role[1] %}
                {% set c_role = role[2] %}

                {% set ls_msg = (
                    "Creating role : "
                    ~ c_role
                    ~ " FOR user email : "
                    ~ user_email
                ) %}
                {% set role_creation_timestamp = "CURRENT_TIMESTAMP" %}

                {% set ls_command = (
                    "insert into rls_user_audit VALUES ("
                    ~ userid
                    ~ ",'"
                    ~ user_email
                    ~ "','"
                    ~ c_role
                    ~ "','"
                    ~ ls_action
                    ~ "','"
                    ~ ls_msg
                    ~ "',"
                    ~ role_creation_timestamp
                    ~ ")"
                ) %}

                {% do run_query(ls_command) %}

                {{
                    log(
                        "create role record added to rls_user_audit for user email: "
                        ~ user_email,
                        info=True,
                    )
                }}

                {{ log("begin role creation for: " ~ c_role, info=True) }}

                {% set create_role_command = "CREATE ROLE IF NOT EXISTS " ~ c_role %}

                {% do run_query("use role accountadmin") %}

                {% do run_query(create_role_command) %}

                {{ log("create role executed for: " ~ c_role, info=True) }}

            {% endfor %}

        {% else %} {% do log("No roles to apply.", True) %}

        {% endif %}
    {% endif %}

{% endmacro %}
