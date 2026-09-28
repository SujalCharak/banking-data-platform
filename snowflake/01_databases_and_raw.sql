-- Run as BANK_ADMIN. Databases, landing stage, raw tables and operational tables.

use role BANK_ADMIN;
use warehouse BANK_LOAD_WH;

create database if not exists RAW       comment = 'Landed source data, loaded as text';
create database if not exists ANALYTICS comment = 'dbt managed models';

create schema if not exists RAW.BANK with managed access comment = 'Raw core banking extracts';
create schema if not exists RAW.OPS  with managed access comment = 'Stage, file formats, audit tables, procedures, tasks';
create schema if not exists ANALYTICS.GOVERNANCE comment = 'Masking policies';

create file format if not exists RAW.OPS.FF_LANDING_CSV
    type = csv
    parse_header = true
    field_optionally_enclosed_by = '"'
    empty_field_as_null = true
    error_on_column_count_mismatch = false
    compression = auto;

create stage if not exists RAW.OPS.LANDING
    file_format = RAW.OPS.FF_LANDING_CSV
    directory = (enable = true)
    comment = 'Daily extract drop zone: <entity>/<entity>_YYYYMMDD.csv.gz';

create table if not exists RAW.BANK.CUSTOMERS (
    customer_id varchar, first_name varchar, last_name varchar, email varchar, birth_date varchar,
    country_code varchar, segment varchar, risk_rating varchar, kyc_status varchar,
    created_at varchar, updated_at varchar,
    _source_file varchar, _source_row number, _loaded_at timestamp_ltz
);

create table if not exists RAW.BANK.ACCOUNTS (
    account_id varchar, customer_id varchar, iban varchar, account_type varchar, currency varchar,
    status varchar, opened_date varchar, closed_date varchar, opening_balance varchar, updated_at varchar,
    _source_file varchar, _source_row number, _loaded_at timestamp_ltz
);

create table if not exists RAW.BANK.MERCHANTS (
    merchant_id varchar, merchant_name varchar, mcc varchar, country_code varchar,
    _source_file varchar, _source_row number, _loaded_at timestamp_ltz
);

create table if not exists RAW.BANK.FX_RATES (
    rate_date varchar, currency varchar, rate_to_eur varchar, source varchar,
    _source_file varchar, _source_row number, _loaded_at timestamp_ltz
);

create table if not exists RAW.BANK.TRANSACTIONS (
    txn_id varchar, account_id varchar, txn_ts varchar, amount varchar, currency varchar,
    direction varchar, txn_type varchar, status varchar, channel varchar, merchant_id varchar,
    counterparty_iban varchar, original_txn_id varchar, description varchar, device_id varchar,
    _source_file varchar, _source_row number, _loaded_at timestamp_ltz
);

create table if not exists RAW.OPS.LOAD_AUDIT (
    run_id varchar, entity varchar, file_name varchar, status varchar,
    rows_parsed number, rows_loaded number, errors_seen number, first_error varchar,
    loaded_at timestamp_ltz default current_timestamp()
);

create table if not exists RAW.OPS.RECONCILIATION_RESULTS (
    run_id varchar, txn_date date,
    source_rows number, target_rows number,
    source_net_amount number(18, 2), target_net_amount number(18, 2),
    status varchar, checked_at timestamp_ltz default current_timestamp()
);

create table if not exists RAW.OPS.REALTIME_LARGE_TXN_ALERTS (
    txn_id varchar, account_id varchar, amount number(18, 2), currency varchar, txn_type varchar,
    txn_ts_raw varchar, _source_file varchar, detected_at timestamp_ltz default current_timestamp()
);

grant usage on database RAW to role BANK_LOADER;
grant usage on database RAW to role BANK_TRANSFORMER;
grant usage on schema RAW.BANK to role BANK_LOADER;
grant usage on schema RAW.BANK to role BANK_TRANSFORMER;
grant usage on schema RAW.OPS  to role BANK_LOADER;
grant usage on schema RAW.OPS  to role BANK_TRANSFORMER;

grant select, insert on all tables in schema RAW.BANK to role BANK_LOADER;
grant select         on all tables in schema RAW.BANK to role BANK_TRANSFORMER;
grant select, insert on all tables in schema RAW.OPS  to role BANK_LOADER;
grant select         on all tables in schema RAW.OPS  to role BANK_TRANSFORMER;
grant read, write on stage RAW.OPS.LANDING       to role BANK_LOADER;
grant usage on file format RAW.OPS.FF_LANDING_CSV to role BANK_LOADER;

grant usage, create schema on database ANALYTICS to role BANK_TRANSFORMER;
grant usage on schema ANALYTICS.GOVERNANCE to role BANK_TRANSFORMER;
grant usage on database ANALYTICS to role BANK_ANALYST;
