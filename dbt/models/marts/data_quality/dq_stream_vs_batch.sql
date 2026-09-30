{#-
    The stream is the fast path for alerting; the daily extract stays the book of record for reporting.
    For every transaction date seen on the stream, compare which transactions each path delivered.
-#}

with streamed as (

    select distinct txn_id, cast(txn_ts as date) as txn_date
    from {{ ref('stg_bank__transactions_stream') }}
    where txn_id is not null
      and txn_ts is not null

),

stream_days as (

    select distinct txn_date from streamed

),

batch as (

    select distinct txn_id, cast(txn_ts as date) as txn_date
    from {{ ref('stg_bank__transactions') }}
    where txn_id is not null
      and cast(txn_ts as date) in (select txn_date from stream_days)

),

matched as (

    select
        coalesce(s.txn_date, b.txn_date) as txn_date,
        s.txn_id                         as streamed_txn_id,
        b.txn_id                         as batch_txn_id
    from streamed s
    full outer join batch b
        on b.txn_id = s.txn_id

)

select
    txn_date,
    count(streamed_txn_id)                                                           as streamed_txns,
    count(batch_txn_id)                                                              as batch_txns,
    count(case when streamed_txn_id is not null and batch_txn_id is not null then 1 end) as in_both,
    count(case when batch_txn_id is null then 1 end)                                 as stream_only,
    count(case when streamed_txn_id is null then 1 end)                              as batch_only,
    -- share of the day's file transactions also seen on the stream; days at the edge of a replay are partial
    cast(
        count(case when streamed_txn_id is not null and batch_txn_id is not null then 1 end) * 1.0
        / nullif(count(batch_txn_id), 0)
        as decimal(7, 4)
    )                                                                                as stream_coverage
from matched
group by txn_date
