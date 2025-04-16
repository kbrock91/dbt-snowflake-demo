{{ config(
    materialized='incremental',
    unique_key='journal_line_surrogate_key',
    incremental_strategy='merge'
) }}

with deduped_ct as (
    select *
    from (
        select *,
               row_number() over (
                   partition by journal_id, line_id
                   order by header__change_seq desc
               ) as row_num
        from {{ ref('source_journal_data__ct') }}
        {% if is_incremental() %}
        where header__change_seq > (
            select max(last_change_seq) from {{ this }}
        )
        {% endif %}
    )
    where row_num = 1
),

filtered_src as (
    select *
    from {{ ref('source_journal_data') }} src
    where (src.journal_id, src.line_id) in (
        select journal_id, line_id from deduped_ct
    )
)
,
staged as (
    select
        {{ dbt_utils.generate_surrogate_key(['ct.journal_id', 'ct.line_id']) }} as journal_line_surrogate_key,
        coalesce(src.journal_id, ct.journal_id) as journal_id,
        coalesce(src.line_id, ct.line_id) as line_id,
        src.account,
        src.amount,
        src.currency,
        src.effective_date,
        case when ct.header__change_oper = 'D' then true else false end as is_deleted,
        ct.header__change_seq as last_change_seq,
        ct.header__operation as last_operation,
        ct.header__timestamp as last_updated_at
    from deduped_ct ct
        left join filtered_src src
        on ct.journal_id = src.journal_id
        and ct.line_id = src.line_id
)

select * from staged
