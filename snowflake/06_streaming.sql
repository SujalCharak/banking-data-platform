-- Run as BANK_ADMIN after 01 to 04. Adds the Kafka landing table and runs large transaction
-- screening on both the file feed and the Kafka feed, triggered as soon as either has new rows.

use role BANK_ADMIN;
use warehouse BANK_LOAD_WH;

create table if not exists RAW.BANK.TRANSACTIONS_STREAM (
    txn_id varchar, account_id varchar, txn_ts varchar, amount varchar, currency varchar,
    direction varchar, txn_type varchar, status varchar, channel varchar, merchant_id varchar,
    counterparty_iban varchar, original_txn_id varchar, description varchar, device_id varchar,
    _kafka_topic varchar, _kafka_partition number, _kafka_offset number, _kafka_timestamp_ms number,
    _raw_value varchar, _parse_error varchar,
    _ingested_at timestamp_ltz
)
comment = 'Transaction events landed from Kafka topic bank.transactions, one row per message';

grant select, insert on table RAW.BANK.TRANSACTIONS_STREAM to role BANK_LOADER;
grant select on table RAW.BANK.TRANSACTIONS_STREAM to role BANK_TRANSFORMER;

create stream if not exists RAW.OPS.KAFKA_TRANSACTIONS_NEW_ROWS
    on table RAW.BANK.TRANSACTIONS_STREAM
    append_only = true
    comment = 'Kafka rows not yet screened by the large transaction task';

alter table RAW.OPS.REALTIME_LARGE_TXN_ALERTS add column if not exists ingest_path varchar;
alter table RAW.OPS.REALTIME_LARGE_TXN_ALERTS add column if not exists produced_at timestamp_ltz;
alter table RAW.OPS.REALTIME_LARGE_TXN_ALERTS add column if not exists landed_at timestamp_ltz;
update RAW.OPS.REALTIME_LARGE_TXN_ALERTS set ingest_path = 'FILE' where ingest_path is null;

alter task if exists RAW.OPS.T_REALTIME_LARGE_TXN_ALERTS suspend;

-- Triggered task: no schedule, runs within seconds of new rows on either stream (at most every 10 seconds).
-- One statement reads both streams, so both advance together when it commits.
-- Each path alerts a transaction once; comparing the two paths shows whether the fast path missed anything.
create or replace task RAW.OPS.T_REALTIME_LARGE_TXN_ALERTS
    target_completion_interval = '1 MINUTE'
    user_task_minimum_trigger_interval_in_seconds = 10
    comment = 'Flag single transactions of 10,000 or more in account currency, from files and from Kafka'
    when system$stream_has_data('RAW.OPS.TRANSACTIONS_NEW_ROWS')
      or system$stream_has_data('RAW.OPS.KAFKA_TRANSACTIONS_NEW_ROWS')
as
    insert into RAW.OPS.REALTIME_LARGE_TXN_ALERTS
        (txn_id, account_id, amount, currency, txn_type, txn_ts_raw, _source_file, ingest_path, produced_at, landed_at)
    with new_rows as (
        select
            trim(txn_id) as txn_id, trim(account_id) as account_id, amount, currency, txn_type, status,
            txn_ts as txn_ts_raw, _source_file, 'FILE' as ingest_path,
            null::timestamp_ltz as produced_at, _loaded_at as landed_at
        from RAW.OPS.TRANSACTIONS_NEW_ROWS
        union all
        select
            trim(txn_id), trim(account_id), amount, currency, txn_type, status,
            txn_ts, 'kafka://' || _kafka_topic || '/' || _kafka_partition || '/' || _kafka_offset, 'KAFKA',
            to_timestamp_ltz(_kafka_timestamp_ms, 3), _ingested_at
        from RAW.OPS.KAFKA_TRANSACTIONS_NEW_ROWS
    ),
    screened as (
        select
            txn_id, account_id,
            try_cast(replace(trim(amount), ',', '.') as number(18, 2)) as amount,
            upper(trim(currency)) as currency, upper(trim(txn_type)) as txn_type,
            txn_ts_raw, _source_file, ingest_path, produced_at, landed_at
        from new_rows
        where txn_id is not null
          and upper(trim(status)) = 'POSTED'
          and try_cast(replace(trim(amount), ',', '.') as number(18, 2)) >= 10000
        qualify row_number() over (partition by txn_id, ingest_path order by landed_at) = 1
    )
    select s.*
    from screened s
    where not exists (
        select 1
        from RAW.OPS.REALTIME_LARGE_TXN_ALERTS a
        where a.txn_id = s.txn_id
          and a.ingest_path = s.ingest_path
    );

-- Seconds from the event being produced to Kafka, to landing in Snowflake, to the alert.
create or replace view RAW.OPS.V_STREAM_LATENCY as
with landed as (
    select datediff('millisecond', to_timestamp_ltz(_kafka_timestamp_ms, 3), _ingested_at) / 1000 as produce_to_land_s
    from RAW.BANK.TRANSACTIONS_STREAM
    where _kafka_timestamp_ms is not null
),
alerted as (
    select
        datediff('millisecond', produced_at, landed_at) / 1000  as produce_to_land_s,
        datediff('millisecond', landed_at, detected_at) / 1000  as land_to_alert_s,
        datediff('millisecond', produced_at, detected_at) / 1000 as produce_to_alert_s
    from RAW.OPS.REALTIME_LARGE_TXN_ALERTS
    where ingest_path = 'KAFKA'
)
select 'all landed messages' as measure, count(*) as events,
       percentile_cont(0.5) within group (order by produce_to_land_s)  as p50_s,
       percentile_cont(0.95) within group (order by produce_to_land_s) as p95_s,
       max(produce_to_land_s) as max_s
from landed
union all
select 'alert: landed to alerted', count(*),
       percentile_cont(0.5) within group (order by land_to_alert_s),
       percentile_cont(0.95) within group (order by land_to_alert_s),
       max(land_to_alert_s)
from alerted
union all
select 'alert: produced to alerted', count(*),
       percentile_cont(0.5) within group (order by produce_to_alert_s),
       percentile_cont(0.95) within group (order by produce_to_alert_s),
       max(produce_to_alert_s)
from alerted;

-- Alerts the file path raised for days the stream also carried, that the stream path did not.
create or replace view RAW.OPS.V_STREAM_MISSED_ALERTS as
select f.*
from RAW.OPS.REALTIME_LARGE_TXN_ALERTS f
where f.ingest_path = 'FILE'
  and f.txn_id in (select trim(txn_id) from RAW.BANK.TRANSACTIONS_STREAM)
  and not exists (
      select 1 from RAW.OPS.REALTIME_LARGE_TXN_ALERTS k
      where k.ingest_path = 'KAFKA' and k.txn_id = f.txn_id
  );

-- The streaming demo runs with the task live; suspend it afterwards:
-- alter task RAW.OPS.T_REALTIME_LARGE_TXN_ALERTS resume;
-- alter task RAW.OPS.T_REALTIME_LARGE_TXN_ALERTS suspend;
