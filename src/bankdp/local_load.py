from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import duckdb

from bankdp.schema import RAW_COLUMNS

log = logging.getLogger(__name__)


def _file_date(path: Path) -> date:
    stamp = path.name.split("_")[-1].split(".")[0]
    return date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:8]))


def ensure_raw_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("create schema if not exists raw")
    for entity, columns in RAW_COLUMNS.items():
        cols = ", ".join(f"{c} varchar" for c in columns)
        con.execute(
            f"create table if not exists raw.{entity} "
            f"({cols}, _source_file varchar, _source_row bigint, _loaded_at timestamp)"
        )
    con.execute(
        "create table if not exists raw.load_audit ("
        "entity varchar, file_name varchar, rows_loaded bigint, status varchar, loaded_at timestamp)"
    )


def load_landing(db_path: Path, landing: Path, through: date | None = None) -> dict[str, int]:
    """Load new landing files into raw tables. Files already recorded in raw.load_audit are skipped."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    ensure_raw_schema(con)
    loaded = {row[0] for row in con.execute("select file_name from raw.load_audit where status = 'LOADED'").fetchall()}
    batch_ts = con.execute("select cast(current_timestamp as timestamp)").fetchone()[0]
    totals: dict[str, int] = {}

    con.execute("begin")
    try:
        for entity in RAW_COLUMNS:
            files = sorted((landing / entity).glob(f"{entity}_*.csv.gz"))
            pending = [f for f in files if f.name not in loaded and (through is None or _file_date(f) <= through)]
            count = 0
            for f in pending:
                before = con.execute(f"select count(*) from raw.{entity}").fetchone()[0]
                con.execute(
                    f"""
                    insert into raw.{entity} by name
                    select *,
                           ? as _source_file,
                           row_number() over () as _source_row,
                           ? as _loaded_at
                    from read_csv(?, header = true, all_varchar = true)
                    """,
                    [f"{entity}/{f.name}", batch_ts, str(f)],
                )
                rows = con.execute(f"select count(*) from raw.{entity}").fetchone()[0] - before
                con.execute(
                    "insert into raw.load_audit values (?, ?, ?, 'LOADED', ?)",
                    [entity, f.name, rows, batch_ts],
                )
                count += rows
            totals[entity] = count
            log.info("loaded %s: %d files, %d rows", entity, len(pending), count)
        con.execute("commit")
    except Exception:
        con.execute("rollback")
        raise
    finally:
        con.close()
    return totals
