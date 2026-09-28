-- Builds benchmark tables at roughly 40x the base volume (about 110M rows).
-- Run as BANK_ADMIN after a prod dbt build. Uses a MEDIUM warehouse for a few minutes, then drops back to XSMALL.

use role BANK_ADMIN;
alter warehouse BANK_BENCH_WH set warehouse_size = medium;
use warehouse BANK_BENCH_WH;

create schema if not exists ANALYTICS.BENCH;

-- 10 years x 4 copies of the account book. Rows are written in join order, so every
-- micro-partition mixes many dates: a realistic "never maintained" table.
create or replace table ANALYTICS.BENCH.TXN_UNCLUSTERED as
with copies as (
    select seq4() as copy_no from table(generator(rowcount => 40))
)
select
    t.txn_id || '-' || c.copy_no                              as txn_id,
    t.account_id || '-' || floor(c.copy_no / 10)              as account_id,
    t.customer_id || '-' || floor(c.copy_no / 10)             as customer_id,
    dateadd(year, -mod(c.copy_no, 10), t.txn_ts)              as txn_ts,
    dateadd(year, -mod(c.copy_no, 10), t.txn_date)            as txn_date,
    t.txn_type,
    t.txn_category,
    t.direction,
    t.status,
    t.channel,
    t.merchant_id,
    t.currency,
    t.amount,
    t.signed_amount,
    t.amount_eur,
    t.signed_amount_eur,
    t.is_posted,
    t.description
from ANALYTICS.CORE.FCT_TRANSACTIONS t
cross join copies c;

-- Same rows, sorted on the cluster key at write time, so pruning works from the first query.
create or replace table ANALYTICS.BENCH.TXN_CLUSTERED
    cluster by (txn_date)
as
select * from ANALYTICS.BENCH.TXN_UNCLUSTERED
order by txn_date;

-- Enterprise edition only. Builds in the background; the benchmark script waits for it.
create or replace table ANALYTICS.BENCH.TXN_SEARCH_OPTIMIZED clone ANALYTICS.BENCH.TXN_UNCLUSTERED;
alter table ANALYTICS.BENCH.TXN_SEARCH_OPTIMIZED add search optimization on equality(txn_id, account_id);

alter warehouse BANK_BENCH_WH set warehouse_size = xsmall;

select 'unclustered' as table_name, parse_json(system$clustering_information('ANALYTICS.BENCH.TXN_UNCLUSTERED', '(txn_date)')):average_depth::float as avg_depth
union all
select 'clustered', parse_json(system$clustering_information('ANALYTICS.BENCH.TXN_CLUSTERED', '(txn_date)')):average_depth::float;
