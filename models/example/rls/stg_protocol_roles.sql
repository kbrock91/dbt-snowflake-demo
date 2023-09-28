{{
    config(
        post_hook= create_roles(this)
    )
}}

     SELECT DISTINCT
        contact_id,
        contact_email,
        upper(translate(contact_email,'@.-+',CHR(95) || CHR(95))) AS role_name
    FROM {{ ref('protocol_access') }}
