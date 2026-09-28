-- Run as BANK_ADMIN after 01. Load, reconciliation and alerting procedures.

use role BANK_ADMIN;
use warehouse BANK_LOAD_WH;

-- Loads every new file for one entity from the stage. COPY's own load history makes reruns skip loaded files.
create or replace procedure RAW.OPS.SP_LOAD_LANDING(ENTITY varchar)
returns variant
language sql
execute as caller
as
$$
declare
    allowed array default array_construct('customers', 'accounts', 'merchants', 'fx_rates', 'transactions');
    unknown_entity exception (-20001, 'Unknown entity. Expected customers, accounts, merchants, fx_rates or transactions.');
    run_id varchar default uuid_string();
    target_entity varchar;
    copy_sql varchar;
    copy_query_id varchar;
    first_column varchar;
    summary variant;
begin
    target_entity := lower(entity);
    if (not array_contains(target_entity::variant, allowed)) then
        raise unknown_entity;
    end if;

    copy_sql := 'copy into RAW.BANK.' || upper(target_entity) ||
                ' from @RAW.OPS.LANDING/' || target_entity || '/' ||
                ' file_format = (format_name = ''RAW.OPS.FF_LANDING_CSV'')' ||
                ' match_by_column_name = case_insensitive' ||
                ' include_metadata = (_source_file = METADATA$FILENAME, _source_row = METADATA$FILE_ROW_NUMBER,' ||
                ' _loaded_at = METADATA$START_SCAN_TIME)' ||
                ' pattern = ''.*' || target_entity || '_[0-9]{8}[.]csv[.]gz''' ||
                ' on_error = continue';
    execute immediate :copy_sql;
    copy_query_id := last_query_id();

    -- With no new files COPY returns a single status column instead of per file rows.
    select $1 into :first_column from table(result_scan(:copy_query_id)) limit 1;

    if (first_column like 'Copy executed with 0 files processed%') then
        insert into RAW.OPS.LOAD_AUDIT (run_id, entity, file_name, status, rows_parsed, rows_loaded, errors_seen)
        values (:run_id, :target_entity, null, 'NO_NEW_FILES', 0, 0, 0);
    else
        insert into RAW.OPS.LOAD_AUDIT (run_id, entity, file_name, status, rows_parsed, rows_loaded, errors_seen, first_error)
        select :run_id, :target_entity, "file", "status", "rows_parsed", "rows_loaded", "errors_seen", "first_error"
        from table(result_scan(:copy_query_id));
    end if;

    select object_construct(
               'entity', :target_entity,
               'run_id', :run_id,
               'files_loaded', count_if(file_name is not null),
               'rows_loaded', coalesce(sum(rows_loaded), 0),
               'files_with_errors', count_if(errors_seen > 0)
           )
      into :summary
      from RAW.OPS.LOAD_AUDIT
     where run_id = :run_id;

    return summary;
end;
$$;


create or replace procedure RAW.OPS.SP_LOAD_ALL()
returns variant
language sql
execute as caller
as
$$
declare
    results array default array_construct();
    result variant;
begin
    -- Reference data first so new transactions can be matched to accounts in the same run.
    call RAW.OPS.SP_LOAD_LANDING('customers') into :result;
    results := array_append(results, result);
    call RAW.OPS.SP_LOAD_LANDING('accounts') into :result;
    results := array_append(results, result);
    call RAW.OPS.SP_LOAD_LANDING('merchants') into :result;
    results := array_append(results, result);
    call RAW.OPS.SP_LOAD_LANDING('fx_rates') into :result;
    results := array_append(results, result);
    call RAW.OPS.SP_LOAD_LANDING('transactions') into :result;
    results := array_append(results, result);
    return results;
end;
$$;


-- Independent control: rebuilds daily counts and net amounts straight from RAW with its own parsing
-- and compares them with the dbt fact table. Fails loudly if any day disagrees.
create or replace procedure RAW.OPS.SP_RECONCILE_TRANSACTIONS(FROM_DATE date, TO_DATE date)
returns variant
language sql
execute as caller
as
$$
declare
    run_id varchar default uuid_string();
    failed_days number;
    checked_days number;
    reconciliation_failed exception (-20002, 'Reconciliation failed. See RAW.OPS.RECONCILIATION_RESULTS.');
begin
    insert into RAW.OPS.RECONCILIATION_RESULTS
        (run_id, txn_date, source_rows, target_rows, source_net_amount, target_net_amount, status)
    with parsed as (
        select
            trim(txn_id) as txn_id,
            try_cast(replace(trim(amount), ',', '.') as number(18, 2)) as amount,
            upper(trim(direction)) as direction,
            coalesce(
                try_to_timestamp_ntz(trim(txn_ts), 'YYYY-MM-DD HH24:MI:SS'),
                try_to_timestamp_ntz(trim(txn_ts), 'DD/MM/YYYY HH24:MI:SS')
            )::date as txn_date,
            _source_file,
            _source_row,
            _loaded_at
        from RAW.BANK.TRANSACTIONS
    ),
    accepted as (
        select p.*
        from parsed p
        where not exists (
            select 1
            from ANALYTICS.DATA_QUALITY.DQ_REJECTED_TRANSACTIONS q
            where q._source_file = p._source_file
              and q._source_row = p._source_row
        )
        qualify row_number() over (
            partition by txn_id order by _source_file desc, _loaded_at desc, _source_row desc
        ) = 1
    ),
    source_side as (
        select txn_date, count(*) as n, sum(iff(direction = 'DR', -amount, amount)) as net
        from accepted
        where txn_date between :from_date and :to_date
        group by txn_date
    ),
    target_side as (
        select txn_date, count(*) as n, sum(signed_amount) as net
        from ANALYTICS.CORE.FCT_TRANSACTIONS
        where txn_date between :from_date and :to_date
        group by txn_date
    )
    select
        :run_id,
        coalesce(s.txn_date, t.txn_date),
        s.n,
        t.n,
        s.net,
        t.net,
        iff(equal_null(s.n, t.n) and equal_null(s.net, t.net), 'PASS', 'FAIL')
    from source_side s
    full outer join target_side t
        on t.txn_date = s.txn_date;

    select count(*), count_if(status = 'FAIL')
      into :checked_days, :failed_days
      from RAW.OPS.RECONCILIATION_RESULTS
     where run_id = :run_id;

    if (failed_days > 0) then
        raise reconciliation_failed;
    end if;

    return object_construct('run_id', :run_id, 'days_checked', :checked_days, 'days_failed', :failed_days);
end;
$$;


grant usage on procedure RAW.OPS.SP_LOAD_LANDING(varchar) to role BANK_LOADER;
grant usage on procedure RAW.OPS.SP_LOAD_ALL()            to role BANK_LOADER;
grant usage on procedure RAW.OPS.SP_RECONCILE_TRANSACTIONS(date, date) to role BANK_TRANSFORMER;
