{{
    config(
        materialized='incremental',
        unique_key=['_source_file', '_source_row'],
        incremental_strategy=merge_strategy()
    )
}}

select
    txn_id,
    account_id,
    txn_ts_raw,
    amount_raw,
    currency,
    direction,
    txn_type,
    rejection_reason,
    _source_file,
    _source_row,
    _loaded_at
from {{ ref('int_transactions__validated') }}
where rejection_reason is not null
{% if is_incremental() %}
  and {{ loaded_since_last_run() }}
{% endif %}
