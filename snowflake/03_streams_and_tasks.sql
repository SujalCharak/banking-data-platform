-- Run as BANK_ADMIN after 02. Tasks are created suspended; resume them when you want the schedule live.

use role BANK_ADMIN;

create or replace stream RAW.OPS.TRANSACTIONS_NEW_ROWS
    on table RAW.BANK.TRANSACTIONS
    append_only = true
    comment = 'New raw transaction rows not yet screened by the realtime alert task';

-- Hourly load of whatever has landed in the stage.
create or replace task RAW.OPS.T_LOAD_LANDING
    warehouse = BANK_LOAD_WH
    schedule = 'USING CRON 5 * * * * UTC'
    comment = 'Load new landing files into RAW'
as
    call RAW.OPS.SP_LOAD_ALL();

-- Screens only rows that arrived since the last run. The task is skipped (no compute) when the stream is empty.
create or replace task RAW.OPS.T_REALTIME_LARGE_TXN_ALERTS
    user_task_managed_initial_warehouse_size = 'XSMALL'
    schedule = '10 MINUTE'
    comment = 'Flag single raw transactions of 10,000 or more in account currency'
    when system$stream_has_data('RAW.OPS.TRANSACTIONS_NEW_ROWS')
as
    insert into RAW.OPS.REALTIME_LARGE_TXN_ALERTS (txn_id, account_id, amount, currency, txn_type, txn_ts_raw, _source_file)
    select
        trim(txn_id),
        trim(account_id),
        try_cast(replace(trim(amount), ',', '.') as number(18, 2)),
        upper(trim(currency)),
        upper(trim(txn_type)),
        txn_ts,
        _source_file
    from RAW.OPS.TRANSACTIONS_NEW_ROWS
    where try_cast(replace(trim(amount), ',', '.') as number(18, 2)) >= 10000
      and upper(trim(status)) = 'POSTED';

-- Daily control over the last seven days, after the morning dbt run.
create or replace task RAW.OPS.T_DAILY_RECONCILIATION
    warehouse = BANK_LOAD_WH
    schedule = 'USING CRON 30 6 * * * UTC'
    comment = 'Reconcile RAW against ANALYTICS.CORE.FCT_TRANSACTIONS'
as
    call RAW.OPS.SP_RECONCILE_TRANSACTIONS(dateadd(day, -7, current_date()), current_date());

-- To go live:
-- alter task RAW.OPS.T_REALTIME_LARGE_TXN_ALERTS resume;
-- alter task RAW.OPS.T_LOAD_LANDING resume;
-- alter task RAW.OPS.T_DAILY_RECONCILIATION resume;
