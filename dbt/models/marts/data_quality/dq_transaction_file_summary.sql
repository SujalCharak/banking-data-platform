{%- set rejection_reasons = [
    'MISSING_TIMESTAMP', 'INVALID_TIMESTAMP', 'INVALID_AMOUNT', 'NEGATIVE_AMOUNT',
    'ZERO_AMOUNT', 'UNKNOWN_ACCOUNT', 'INVALID_DIRECTION', 'CURRENCY_MISMATCH'
] -%}

with rows_received as (

    select
        _source_file,
        _loaded_at,
        txn_id,
        txn_ts,
        rejection_reason,
        warning_reason,
        {{ try_parse_date('left(right(_source_file, 15), 8)', '%Y%m%d') }} as file_date,
        row_number() over (
            partition by txn_id
            order by _loaded_at, _source_file, _source_row
        ) as occurrence
    from {{ ref('int_transactions__validated') }}

)

select
    _source_file                                                          as source_file,
    file_date,
    min(_loaded_at)                                                       as loaded_at,
    count(*)                                                              as rows_received,
    sum(case when occurrence > 1 then 1 else 0 end)                       as duplicate_rows,
    sum(case when rejection_reason is not null then 1 else 0 end)         as rejected_rows,
    {%- for reason in rejection_reasons %}
    sum(case when rejection_reason = '{{ reason }}' then 1 else 0 end)    as rejected_{{ reason | lower }},
    {%- endfor %}
    sum(case when warning_reason is not null then 1 else 0 end)           as warning_rows,
    sum(case
            when occurrence = 1
             and rejection_reason is null
             and cast(txn_ts as date) < cast({{ dbt.dateadd('day', -1, 'file_date') }} as date)
            then 1 else 0
        end)                                                              as late_arriving_rows,
    min(cast(txn_ts as date))                                             as min_txn_date,
    max(cast(txn_ts as date))                                             as max_txn_date
from rows_received
group by _source_file, file_date
