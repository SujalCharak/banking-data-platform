from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = ROOT / "dbt"
DEFAULT_DATA = ROOT / "data"
DEFAULT_DB = ROOT / "warehouse" / "bank.duckdb"

log = logging.getLogger("bankdp")


def run_dbt(args: list[str], target: str = "local", db_path: Path = DEFAULT_DB) -> None:
    import os

    from dbt.cli.main import dbtRunner

    os.environ.setdefault("DBT_TARGET", target)
    os.environ["BANK_DUCKDB_PATH"] = str(db_path)
    result = dbtRunner().invoke(
        [*args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR), "--target", target]
    )
    if not result.success:
        raise SystemExit(f"dbt {' '.join(args)} failed")


def cmd_generate(a: argparse.Namespace) -> None:
    from bankdp.generate import BankDataGenerator, GenConfig

    cfg = GenConfig(customers=a.customers, days=a.days, start=a.start, seed=a.seed, out_dir=a.out)
    summary = BankDataGenerator(cfg).run()
    log.info("generated %s", summary)


def cmd_load_local(a: argparse.Namespace) -> None:
    from bankdp.local_load import load_landing

    totals = load_landing(a.db, a.data / "landing", a.through)
    log.info("loaded %s", totals)


def cmd_validate(a: argparse.Namespace) -> None:
    from bankdp.validate import validate

    if a.target == "local":
        import duckdb

        con = duckdb.connect(str(a.db))
        query = lambda sql: con.execute(sql).df()  # noqa: E731
        schemas = {s: s for s in ["data_quality", "core", "risk", "intermediate"]}
    else:
        from bankdp.snowflake_io import connect, query_df

        con = connect()
        query = lambda sql: query_df(con, sql)  # noqa: E731
        schemas = {s: f"ANALYTICS.{s.upper()}" for s in ["data_quality", "core", "risk", "intermediate"]}

    report = validate(query, a.data / "ground_truth", schemas)
    print(report.to_markdown())
    if a.out:
        a.out.write_text(report.to_markdown() + "\n")
    if not report.passed:
        raise SystemExit("validation failed")


# run bookkeeping columns legitimately differ between an incremental history and a single rebuild
INCREMENTAL_MODELS = {
    "core.fct_transactions": "",
    "core.fct_account_daily_balance": "exclude (source_loaded_through)",
    "data_quality.dq_rejected_transactions": "",
}


def check_incremental_matches_full_refresh(db_path: Path) -> None:
    import duckdb

    con = duckdb.connect(str(db_path))
    con.execute("create schema if not exists verify")
    for table in INCREMENTAL_MODELS:
        con.execute(f"create or replace table verify.{table.split('.')[1]} as select * from {table}")
    con.close()

    run_dbt(
        [
            "run",
            "--full-refresh",
            "--select",
            "fct_transactions",
            "fct_account_daily_balance",
            "dq_rejected_transactions",
        ],
        db_path=db_path,
    )

    con = duckdb.connect(str(db_path))
    for table, exclude in INCREMENTAL_MODELS.items():
        name = table.split(".")[1]
        diff = con.execute(
            f"select count(*) from ((select * {exclude} from {table} except all select * {exclude} from verify.{name}) "
            f"union all (select * {exclude} from verify.{name} except all select * {exclude} from {table}))"
        ).fetchone()[0]
        rows = con.execute(f"select count(*) from {table}").fetchone()[0]
        if diff:
            raise SystemExit(f"{table}: incremental result differs from full refresh in {diff} rows")
        log.info("%s: incremental equals full refresh (%d rows)", table, rows)
    con.execute("drop schema verify cascade")
    con.close()


def cmd_run_local(a: argparse.Namespace) -> None:
    """Full local run: fresh data, two load batches, incremental dbt runs, then validation."""
    import shutil

    from bankdp.generate import BankDataGenerator, GenConfig
    from bankdp.local_load import load_landing

    if a.db.exists():
        a.db.unlink()
    shutil.rmtree(a.data, ignore_errors=True)
    BankDataGenerator(GenConfig(customers=a.customers, days=a.days, out_dir=a.data)).run()

    cutoff = GenConfig().start + timedelta(days=int(a.days * 0.7))
    load_landing(a.db, a.data / "landing", through=cutoff)
    run_dbt(["seed"], db_path=a.db)
    run_dbt(["build"], db_path=a.db)

    load_landing(a.db, a.data / "landing")
    run_dbt(["build"], db_path=a.db)
    check_incremental_matches_full_refresh(a.db)

    a.target = "local"
    a.out = ROOT / "docs" / "validation_report.md"
    cmd_validate(a)


def cmd_load_snowflake(a: argparse.Namespace) -> None:
    from bankdp.snowflake_load import load_landing_to_snowflake

    load_landing_to_snowflake(a.data / "landing", entities=a.entities, through=a.through)


def cmd_benchmark(a: argparse.Namespace) -> None:
    from bankdp.benchmark import run_benchmarks

    run_benchmarks(ROOT / "snowflake" / "benchmarks", ROOT / "docs" / "benchmark_results.md", repeats=a.repeats)


def cmd_cost_report(a: argparse.Namespace) -> None:
    from bankdp.cost_report import build_cost_report

    build_cost_report(ROOT / "docs" / "cost_report.md", days=a.days)


def _make_sink(a: argparse.Namespace):
    from bankdp.streaming.sinks import DuckDBSink, SnowflakeSink

    return DuckDBSink(a.db) if a.sink == "duckdb" else SnowflakeSink()


def cmd_stream_produce(a: argparse.Namespace) -> None:
    from bankdp.streaming.producer import ensure_topic, produce, replay_rows

    ensure_topic(a.bootstrap, a.topic, a.partitions, fresh=a.fresh_topic)
    rows = replay_rows(a.data / "landing", getattr(a, "from"), a.through)
    produce(
        a.bootstrap,
        a.topic,
        rows,
        rate=a.rate,
        corrupt_every=a.corrupt_every,
        limit=a.limit,
        idempotent=not a.no_idempotence,
    )


def cmd_stream_consume(a: argparse.Namespace) -> None:
    from bankdp.streaming.consumer import run_consumer
    from bankdp.streaming.producer import ensure_topic

    ensure_topic(a.bootstrap, a.topic, a.partitions)
    run_consumer(
        a.bootstrap,
        a.group,
        a.topic,
        _make_sink(a),
        a.batch_size,
        a.max_wait,
        a.idle_exit,
        max_batches=a.max_batches,
        commit_offsets=not a.no_commit,
    )


def cmd_stream_check(a: argparse.Namespace) -> None:
    from bankdp.streaming.check import check_sink_matches_topic

    report = check_sink_matches_topic(a.bootstrap, a.topic, a.sink, a.db)
    print(report.to_markdown())
    if not report.passed:
        raise SystemExit("landed rows do not match the topic exactly")


def cmd_stream_run_local(a: argparse.Namespace) -> None:
    """Replay days from the extracts through Kafka into DuckDB, crash the consumer once, and verify exactly once."""
    from bankdp.streaming.check import check_sink_matches_topic
    from bankdp.streaming.consumer import run_consumer
    from bankdp.streaming.producer import ensure_topic, produce, replay_rows
    from bankdp.streaming.sinks import DuckDBSink

    if not a.db.exists():
        raise SystemExit(f"{a.db} not found; run `bankdp run-local` first")
    ensure_topic(a.bootstrap, a.topic, a.partitions, fresh=True)
    # a recreated topic starts again at offset 0, so rows landed from the old one would mislead the consumer
    sink = DuckDBSink(a.db)
    sink.con.execute("delete from raw.transactions_stream where _kafka_topic = ?", [a.topic])
    sink.close()
    produce(
        a.bootstrap,
        a.topic,
        replay_rows(a.data / "landing", getattr(a, "from"), a.through),
        corrupt_every=a.corrupt_every,
        idempotent=not a.no_idempotence,
    )

    group = f"bank-local-{date.today():%Y%m%d}"
    log.info("first consumer: lands 2 batches, never commits to Kafka, then stops as if it crashed")
    run_consumer(
        a.bootstrap,
        group,
        a.topic,
        DuckDBSink(a.db),
        batch_size=a.batch_size,
        max_wait_s=2,
        idle_exit_s=15,
        max_batches=2,
        commit_offsets=False,
    )
    log.info("second consumer: same group, resumes from what the table holds")
    run_consumer(a.bootstrap, group, a.topic, DuckDBSink(a.db), batch_size=a.batch_size, max_wait_s=2, idle_exit_s=10)

    report = check_sink_matches_topic(a.bootstrap, a.topic, "duckdb", a.db)
    print(report.to_markdown())
    if not report.passed:
        raise SystemExit("landed rows do not match the topic exactly")
    run_dbt(["build", "--select", "stg_bank__transactions_stream+"], db_path=a.db)

    import duckdb

    con = duckdb.connect(str(a.db))
    print(con.execute("select * from data_quality.dq_stream_vs_batch order by txn_date").df().to_string(index=False))
    con.close()


def _add_kafka_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="bank.transactions")


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(prog="bankdp", description="Banking data platform tooling")
    sub = parser.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="generate landing files and ground truth")
    g.add_argument("--customers", type=int, default=4000)
    g.add_argument("--days", type=int, default=365)
    g.add_argument("--start", type=date.fromisoformat, default=date(2025, 1, 1))
    g.add_argument("--seed", type=int, default=7)
    g.add_argument("--out", type=Path, default=DEFAULT_DATA)
    g.set_defaults(func=cmd_generate)

    ll = sub.add_parser("load-local", help="load new landing files into DuckDB raw tables")
    ll.add_argument("--db", type=Path, default=DEFAULT_DB)
    ll.add_argument("--data", type=Path, default=DEFAULT_DATA)
    ll.add_argument(
        "--through", type=date.fromisoformat, default=None, help="only load files dated on or before this day"
    )
    ll.set_defaults(func=cmd_load_local)

    rl = sub.add_parser("run-local", help="generate, load in two batches, build, validate")
    rl.add_argument("--customers", type=int, default=4000)
    rl.add_argument("--days", type=int, default=365)
    rl.add_argument("--db", type=Path, default=DEFAULT_DB)
    rl.add_argument("--data", type=Path, default=DEFAULT_DATA)
    rl.set_defaults(func=cmd_run_local)

    v = sub.add_parser("validate", help="compare pipeline output against injected ground truth")
    v.add_argument("--target", choices=["local", "snowflake"], default="local")
    v.add_argument("--db", type=Path, default=DEFAULT_DB)
    v.add_argument("--data", type=Path, default=DEFAULT_DATA)
    v.add_argument("--out", type=Path, default=None)
    v.set_defaults(func=cmd_validate)

    ls = sub.add_parser("load-snowflake", help="upload landing files to the Snowflake stage and run the load procedure")
    ls.add_argument("--data", type=Path, default=DEFAULT_DATA)
    ls.add_argument("--entities", nargs="*", default=None)
    ls.add_argument(
        "--through", type=date.fromisoformat, default=None, help="only upload files dated on or before this day"
    )
    ls.set_defaults(func=cmd_load_snowflake)

    b = sub.add_parser("benchmark", help="run Snowflake benchmark queries and write a report")
    b.add_argument("--repeats", type=int, default=3)
    b.set_defaults(func=cmd_benchmark)

    c = sub.add_parser("cost-report", help="summarise Snowflake credit usage by warehouse and query tag")
    c.add_argument("--days", type=int, default=14)
    c.set_defaults(func=cmd_cost_report)

    sp = sub.add_parser("stream-produce", help="replay daily extract rows to Kafka as transaction events")
    _add_kafka_args(sp)
    sp.add_argument("--data", type=Path, default=DEFAULT_DATA)
    sp.add_argument("--from", type=date.fromisoformat, required=True, help="first extract file date to replay")
    sp.add_argument("--through", type=date.fromisoformat, required=True, help="last extract file date to replay")
    sp.add_argument("--rate", type=float, default=500, help="messages per second, 0 for no limit")
    sp.add_argument("--limit", type=int, default=None)
    sp.add_argument("--partitions", type=int, default=3)
    sp.add_argument("--corrupt-every", type=int, default=0, help="truncate every Nth payload to test bad messages")
    sp.add_argument("--fresh-topic", action="store_true", help="delete and recreate the topic first")
    sp.add_argument("--no-idempotence", action="store_true")
    sp.set_defaults(func=cmd_stream_produce)

    sc = sub.add_parser("stream-consume", help="land Kafka transaction events in DuckDB or Snowflake")
    _add_kafka_args(sc)
    sc.add_argument("--sink", choices=["duckdb", "snowflake"], default="duckdb")
    sc.add_argument("--db", type=Path, default=DEFAULT_DB)
    sc.add_argument("--group", default="bank-warehouse-sink")
    sc.add_argument("--partitions", type=int, default=3, help="used only if the topic doesn't exist yet")
    sc.add_argument("--batch-size", type=int, default=5000)
    sc.add_argument("--max-wait", type=float, default=5.0, help="seconds before a partial batch is landed")
    sc.add_argument("--idle-exit", type=float, default=None, help="stop after this many seconds without messages")
    sc.add_argument("--max-batches", type=int, default=None, help="stop after N batches, as if the process died")
    sc.add_argument("--no-commit", action="store_true", help="never commit offsets to Kafka")
    sc.set_defaults(func=cmd_stream_consume)

    sk = sub.add_parser("stream-check", help="verify the landed table holds every message in the topic exactly once")
    _add_kafka_args(sk)
    sk.add_argument("--sink", choices=["duckdb", "snowflake"], default="duckdb")
    sk.add_argument("--db", type=Path, default=DEFAULT_DB)
    sk.set_defaults(func=cmd_stream_check)

    sr = sub.add_parser("stream-run-local", help="Kafka to DuckDB with a simulated consumer crash, then verify")
    _add_kafka_args(sr)
    sr.add_argument("--data", type=Path, default=DEFAULT_DATA)
    sr.add_argument("--db", type=Path, default=DEFAULT_DB)
    sr.add_argument("--from", type=date.fromisoformat, required=True)
    sr.add_argument("--through", type=date.fromisoformat, required=True)
    sr.add_argument("--partitions", type=int, default=3)
    sr.add_argument("--batch-size", type=int, default=2000)
    sr.add_argument("--corrupt-every", type=int, default=1000)
    sr.add_argument("--no-idempotence", action="store_true")
    sr.set_defaults(func=cmd_stream_run_local)

    args = parser.parse_args(argv)
    try:
        args.func(args)
    except SystemExit as exc:
        if exc.code not in (None, 0):
            log.error(exc.code)
            sys.exit(1)
        raise
