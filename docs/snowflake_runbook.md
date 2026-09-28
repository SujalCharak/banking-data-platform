# Running on Snowflake

This takes about an hour the first time. Everything runs from a Snowsight worksheet and a terminal on your machine.

Snowflake credit usage is hard to predict exactly, but loading and a full dbt build use well under one credit. The benchmark section uses the most: a few minutes on a MEDIUM warehouse to build 110M rows, plus background credits for search optimization. The resource monitor created in step 1 suspends the platform's warehouses at 40 credits a month.

You need:
* A Snowflake account with ACCOUNTADMIN. A trial works.
  * Choose **Enterprise** edition if you want search optimization and masking.
  * Everything else works on Standard.
* Python 3.10 to 3.13 and `openssl`. dbt does not support 3.14 yet.

## 1. Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[snowflake,dev]"
```

## 2. Create the platform

In a Snowsight worksheet, run each file in full, in this order:

| File | Run as | Creates |
|---|---|---|
| `snowflake/00_account_setup.sql` | ACCOUNTADMIN | roles, warehouses, resource monitor, `BANK_SVC` service user |
| `snowflake/01_databases_and_raw.sql` | BANK_ADMIN | `RAW`, `ANALYTICS`, stage, file format, raw and audit tables |
| `snowflake/02_procedures.sql` | BANK_ADMIN | load, load all, and reconciliation procedures |
| `snowflake/03_streams_and_tasks.sql` | BANK_ADMIN | stream and three tasks (created suspended) |
| `snowflake/04_future_grants.sql` | ACCOUNTADMIN | read access for analysts on everything dbt builds |
| `snowflake/05_governance_optional.sql` | BANK_ADMIN | masking policies (Enterprise only, optional) |

Each script sets its own role on the first line. For `01` to `03`, switch the worksheet role to BANK_ADMIN if Snowsight doesn't pick it up. The role becomes available to you after `00` runs.

## 3. Key pair auth for the service user

Snowflake is phasing out password only sign in, so the tooling authenticates with a key pair.

```bash
mkdir -p ~/.snowflake
openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes256 -inform PEM -out ~/.snowflake/bank_svc_key.p8
openssl rsa -in ~/.snowflake/bank_svc_key.p8 -pubout -out ~/.snowflake/bank_svc_key.pub
grep -v "PUBLIC KEY" ~/.snowflake/bank_svc_key.pub | tr -d '\n'; echo
```

The first command asks for a passphrase. Copy the last line of output and run this in the worksheet as ACCOUNTADMIN:

```sql
alter user BANK_SVC set rsa_public_key = '<paste here>';
```

## 4. Environment

Copy `.env.example` to `.env`, fill it in, and load it:

```bash
cp .env.example .env         # edit the values
set -a && source .env && set +a
```

Your account identifier is in Snowsight under your account details. It looks like `ORGNAME-ACCOUNTNAME`. Use an absolute path for the key file, because dbt doesn't expand `~`.

Check the connection:

```bash
cd dbt && dbt debug --target prod --profiles-dir . && cd ..
```

## 5. Generate and load

```bash
bankdp generate
bankdp load-snowflake --through 2025-09-12
```

This uploads the files with PUT, then calls `RAW.OPS.SP_LOAD_LANDING` once per entity. `--through` loads only the files up to that date, so you can watch the incremental models pick up the rest later. Running the same command again uploads and loads nothing new.

To check what loaded:

```sql
select entity, count(*) as files, sum(rows_loaded) as rows_loaded, sum(errors_seen) as errors
from RAW.OPS.LOAD_AUDIT
group by entity;
```

## 6. Build with dbt

```bash
cd dbt
dbt seed  --target prod --profiles-dir .
dbt build --target prod --profiles-dir .
cd ..
```

Expect 97 passes and no failures.

The `prod` target writes to `ANALYTICS.CORE`, `ANALYTICS.FINANCE` and the other mart schemas. The `dev` target writes to schemas prefixed with `DEV_<DBT_DEV_SCHEMA_SUFFIX>`, so you can work without touching prod.

Now load the rest of the year and build again:

```bash
bankdp load-snowflake
cd dbt && dbt build --target prod --profiles-dir . && cd ..
```

This time `fct_transactions` merges only the new rows. `fct_account_daily_balance` logs the date it recomputes from, which is a few days before the cutoff because of late arrivals.

In total, expect about 2.81M transaction rows across 370 files, with 0 load errors.

## 7. Reconcile and validate

```sql
use role BANK_ADMIN;
use warehouse BANK_LOAD_WH;
call RAW.OPS.SP_RECONCILE_TRANSACTIONS(to_date('2025-01-01'), to_date('2025-12-31'));
select status, count(*) from RAW.OPS.RECONCILIATION_RESULTS group by status;
```

Every day should be PASS. Then compare against the ground truth from your terminal:

```bash
bankdp validate --target snowflake --out docs/validation_report_snowflake.md
```

The numbers should match the local run exactly, because the data is seeded.

## 8. Stream and task

The stream has been collecting rows since step 2, so the first run screens the whole load:

```sql
execute task RAW.OPS.T_REALTIME_LARGE_TXN_ALERTS;
-- wait a minute
select count(*) from RAW.OPS.REALTIME_LARGE_TXN_ALERTS;
select system$stream_has_data('RAW.OPS.TRANSACTIONS_NEW_ROWS');   -- now false
select name, state, error_message, scheduled_time
from table(information_schema.task_history(task_name => 'T_REALTIME_LARGE_TXN_ALERTS'))
order by scheduled_time desc limit 5;
```

If Snowflake refuses to run a suspended task, resume it, run it, then suspend it again.

To put the schedules live, run the `alter task ... resume` lines at the end of `03_streams_and_tasks.sql`. Suspend them again when you're done, so they stop using credits.

## 9. Benchmarks

Run `snowflake/benchmarks/00_build_benchmark_tables.sql` in a worksheet as BANK_ADMIN. It builds three copies of about 110M rows:
* unclustered;
* sorted and clustered on `txn_date`;
* with search optimization (Enterprise only).

Then run:

```bash
bankdp benchmark
```

The script:
1. Waits for the search optimization build to reach 100%.
2. Suspends the warehouse before each variant, so the first run is cold, and runs each variant three times.
3. Writes `docs/benchmark_results.md`.

On Standard edition it skips the point lookup experiment.

When you're finished, drop the benchmark tables. Automatic clustering and search optimization keep using credits in the background while they exist:

```sql
drop schema ANALYTICS.BENCH;
```

## 10. Cost report

`ACCOUNT_USAGE` views lag by up to a few hours. Run this the next day for complete numbers:

```bash
bankdp cost-report --days 7
```

It writes `docs/cost_report.md`. Credits are split by query tag, which is set separately for loads, dbt dev, dbt prod, benchmarks and this report.

## 11. Masking (optional, Enterprise)

After running `05_governance_optional.sql`:

```bash
cd dbt && dbt build --target prod --profiles-dir . --vars '{enable_masking: true, enable_search_optimization: true}'
```

Then, in a worksheet, compare the output for the two roles:

```sql
use role BANK_ANALYST;
select email from ANALYTICS.CORE.DIM_CUSTOMER limit 3;   -- a***@example.com
use role BANK_ADMIN;
select email from ANALYTICS.CORE.DIM_CUSTOMER limit 3;   -- full value
```

## Teardown

Run `snowflake/99_teardown.sql` as ACCOUNTADMIN.

## Troubleshooting

* **JWT token is invalid:**
  * Check that the public key was pasted without its header lines.
  * Check that the account identifier is in `ORGNAME-ACCOUNTNAME` form.
  * Check that `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` matches the passphrase you set.
* **Insufficient privileges on COPY:** `load-snowflake` runs as BANK_LOADER. Rerun `01_databases_and_raw.sql` if tables were created after the grants.
* **Object does not exist in the reconciliation procedure:** it reads `ANALYTICS.CORE` and `ANALYTICS.DATA_QUALITY`, so run the dbt build with `--target prod` first.
* **Search optimization errors:** your account is Standard edition. Leave `enable_search_optimization` off.
