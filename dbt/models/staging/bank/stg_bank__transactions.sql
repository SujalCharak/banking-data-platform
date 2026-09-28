with source as (

    select * from {{ source('bank', 'transactions') }}

)

select
    trim(source.txn_id)                                            as txn_id,
    trim(source.account_id)                                        as account_id,
    {{ try_parse_timestamp('trim(source.txn_ts)', ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S']) }} as txn_ts,
    try_cast(replace(trim(source.amount), ',', '.') as decimal(18, 2)) as amount,
    upper(trim(source.currency))                                   as currency,
    upper(trim(source.direction))                                  as direction,
    upper(trim(source.txn_type))                                   as txn_type,
    upper(trim(source.status))                                     as status,
    upper(trim(source.channel))                                    as channel,
    nullif(trim(source.merchant_id), '')                           as merchant_id,
    nullif(trim(source.counterparty_iban), '')                     as counterparty_iban,
    nullif(trim(source.original_txn_id), '')                       as original_txn_id,
    trim(source.description)                                       as description,
    nullif(trim(source.device_id), '')                             as device_id,
    source.txn_ts                                                  as txn_ts_raw,
    source.amount                                                  as amount_raw,
    source._source_file,
    source._source_row,
    source._loaded_at
from source
