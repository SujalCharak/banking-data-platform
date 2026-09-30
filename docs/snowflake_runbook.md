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
| `snowflake/06_streaming.sql` | BANK_ADMIN | Kafka landing table, stream, triggered screening task, latency views (step 12) |

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

Expect 106 passes and no failures.

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

## 12. Streaming through Kafka

This adds a real time path next to the daily files. Kafka runs locally in Docker, and a consumer lands each micro batch in Snowflake. The screening task then runs on those rows within seconds.

**Start Kafka.** You need Docker Desktop running. From the repo root:

```bash
docker compose up -d
pip install -e ".[snowflake,streaming,dev]"
```

**Optional: prove it locally first.** This needs the local warehouse from `bankdp run-local`. It replays three days through Kafka into DuckDB, stops the consumer mid run as if it crashed, restarts it, and checks every message landed exactly once:

```bash
bankdp stream-run-local --from 2025-12-01 --through 2025-12-03
```

**Set up Snowflake.** Run `snowflake/06_streaming.sql` in a worksheet as BANK_ADMIN, then turn the screening task on:

```sql
alter task RAW.OPS.T_REALTIME_LARGE_TXN_ALERTS resume;
```

It is now a triggered task: it has no schedule and runs when either the file stream or the Kafka stream has new rows, at most every 10 seconds. The file stream has been collecting rows since step 5, so its first run screens the whole year.

**Measure latency.** Use two terminals, each with the conda environment active and `.env` loaded (`set -a && source .env && set +a`).

1. Terminal A: start the consumer. It creates the topic if needed and waits for events.

   ```bash
   bankdp stream-consume --sink snowflake
   ```

2. Terminal B: replay the first week of December, about 54,000 events at 500 a second. Every 5,000th message is deliberately corrupted.

   ```bash
   bankdp stream-produce --from 2025-12-01 --through 2025-12-07 --rate 500 --corrupt-every 5000
   ```

3. When the producer finishes, wait for terminal A to log its last batch, then stop it with Ctrl+C. Check that Snowflake holds every message in the topic exactly once:

   ```bash
   bankdp stream-check --sink snowflake
   ```

4. Record the latency now, before the crash test adds catch up delays to it:

   ```sql
   use role BANK_ADMIN;
   use warehouse BANK_LOAD_WH;
   select * from RAW.OPS.V_STREAM_LATENCY;                  -- seconds from produce to land to alert
   select ingest_path, count(*) from RAW.OPS.REALTIME_LARGE_TXN_ALERTS group by 1;
   select * from RAW.OPS.V_STREAM_MISSED_ALERTS;            -- file alerts the stream path missed
   select count(*) as rows_landed, count(_parse_error) as unparseable from RAW.BANK.TRANSACTIONS_STREAM;
   ```

The missed alerts view should be empty. A row there means a large transaction whose Kafka message was one of the corrupted ones, which is exactly what that control is for.

**Crash test.**

1. Terminal A: start a consumer that lands 2 batches and then stops without committing to Kafka, as if the process died:

   ```bash
   bankdp stream-consume --sink snowflake --max-batches 2 --no-commit
   ```

2. Terminal B: replay three more days as fast as possible:

   ```bash
   bankdp stream-produce --from 2025-12-08 --through 2025-12-10 --rate 0
   ```

3. When terminal A stops, start it again normally. It resumes from what Snowflake holds, not from Kafka's committed offsets:

   ```bash
   bankdp stream-consume --sink snowflake --idle-exit 20
   ```

4. Check again. It should still pass, with no duplicates and no gaps:

   ```bash
   bankdp stream-check --sink snowflake
   ```

Then compare the stream with the daily files in dbt:

```bash
cd dbt && dbt build --target prod --profiles-dir . --select stg_bank__transactions_stream+; cd ..
```

```sql
select * from ANALYTICS.DATA_QUALITY.DQ_STREAM_VS_BATCH order by txn_date;
```

`stream_only` should be 0. Days inside the replay should show `stream_coverage` close to 1; the gap is transactions the source delivered late, in files after 10 December that the replay didn't include. The first few dates only hold late arrivals from earlier days, so their coverage is near 0.

**Stop everything** so nothing keeps using credits:

```sql
alter task RAW.OPS.T_REALTIME_LARGE_TXN_ALERTS suspend;
```

```bash
docker compose down
```

Paste me the `stream-check` table, the latency view and the dbt comparison, and I'll add them to the README.

## 13. Iceberg (optional)

This converts `fct_account_daily_balance` to an Apache Iceberg table on Snowflake storage. There's no S3 bucket or IAM setup; it needs an account on AWS or Azure.

1. Record a fingerprint of the current table in a worksheet:

   ```sql
   use role BANK_ADMIN;
   use warehouse BANK_ANALYST_WH;
   select count(*), sum(closing_balance), max(balance_date) from ANALYTICS.CORE.FCT_ACCOUNT_DAILY_BALANCE;
   ```

2. Rebuild it as Iceberg. Changing a table's format needs a full refresh:

   ```bash
   cd dbt
   dbt build --target prod --profiles-dir . --select fct_account_daily_balance+ --full-refresh --vars '{enable_iceberg: true}'
   cd ..
   ```

3. Confirm it's Iceberg and the data is unchanged:

   ```sql
   show iceberg tables in schema ANALYTICS.CORE;
   select system$get_iceberg_table_information('ANALYTICS.CORE.FCT_ACCOUNT_DAILY_BALANCE');
   select count(*), sum(closing_balance), max(balance_date) from ANALYTICS.CORE.FCT_ACCOUNT_DAILY_BALANCE;
   ```

   The last query must match step 1 exactly.

4. Make it permanent: set `enable_iceberg: true` in `dbt/dbt_project.yml`. Then run a normal `dbt build`, which merges into the Iceberg table incrementally. Without this, the next build stops and asks for `--full-refresh`, because the model would otherwise switch back to a standard table.

## Teardown

Run `snowflake/99_teardown.sql` as ACCOUNTADMIN, and `docker compose down -v` to remove Kafka.

## Troubleshooting

* **JWT token is invalid:**
  * Check that the public key was pasted without its header lines.
  * Check that the account identifier is in `ORGNAME-ACCOUNTNAME` form.
  * Check that `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` matches the passphrase you set.
* **Insufficient privileges on COPY:** `load-snowflake` runs as BANK_LOADER. Rerun `01_databases_and_raw.sql` if tables were created after the grants.
* **Object does not exist in the reconciliation procedure:** it reads `ANALYTICS.CORE` and `ANALYTICS.DATA_QUALITY`, so run the dbt build with `--target prod` first.
* **Search optimization errors:** your account is Standard edition. Leave `enable_search_optimization` off.
