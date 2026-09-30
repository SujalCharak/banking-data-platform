with source as (

    select * from {{ source('bank', 'transactions_stream') }}

)

select
    trim(source.txn_id)                                            as txn_id,
    trim(source.account_id)                                        as account_id,
    {{ try_parse_timestamp('trim(source.txn_ts)', ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S']) }} as txn_ts,
    try_cast(replace(trim(source.amount), ',', '.') as decimal(18, 2)) as amount,
    upper(trim(source.currency))                                   as currency,
    upper(trim(source.txn_type))                                   as txn_type,
    upper(trim(source.status))                                     as status,
    source._kafka_topic,
    source._kafka_partition,
    source._kafka_offset,
    source._kafka_timestamp_ms,
    source._parse_error,
    source._ingested_at
from source
