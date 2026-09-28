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

    args = parser.parse_args(argv)
    try:
        args.func(args)
    except SystemExit as exc:
        if exc.code not in (None, 0):
            log.error(exc.code)
            sys.exit(1)
        raise
