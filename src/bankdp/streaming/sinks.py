from __future__ import annotations

import hashlib
import logging
import tempfile
from pathlib import Path

import duckdb
import pandas as pd

from bankdp.local_load import ensure_raw_schema
from bankdp.schema import STREAM_TABLE

log = logging.getLogger(__name__)


def _batch_name(topic: str, frame: pd.DataFrame) -> str:
    ranges = frame.groupby("_kafka_partition")["_kafka_offset"].agg(["min", "max"]).to_dict("index")
    digest = hashlib.sha1(repr(sorted(ranges.items())).encode()).hexdigest()[:16]
    return f"{topic.replace('.', '_')}_{digest}.csv.gz"


class DuckDBSink:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(db_path))
        ensure_raw_schema(self.con)

    def last_offsets(self, topic: str) -> dict[int, int]:
        rows = self.con.execute(
            f"select _kafka_partition, max(_kafka_offset) from raw.{STREAM_TABLE} where _kafka_topic = ? group by 1",
            [topic],
        ).fetchall()
        return {int(p): int(o) for p, o in rows}

    def write(self, frame: pd.DataFrame) -> None:
        self.con.register("batch_frame", frame)
        try:
            self.con.execute(
                f"insert into raw.{STREAM_TABLE} by name "
                "select *, cast(current_timestamp as timestamp) as _ingested_at from batch_frame"
            )
        finally:
            self.con.unregister("batch_frame")

    def close(self) -> None:
        self.con.close()


class SnowflakeSink:
    """
    Each micro batch becomes one gzip CSV that is PUT to the landing stage and loaded with a single COPY,
    so a batch is either fully landed or not at all. Uses the loader role's existing stage privileges.
    """

    table = "RAW.BANK.TRANSACTIONS_STREAM"
    stage_path = "@RAW.OPS.LANDING/stream/"

    def __init__(self):
        from bankdp.snowflake_io import connect

        self.con = connect(role="BANK_LOADER", warehouse="BANK_LOAD_WH", query_tag="bank_platform_stream")
        self.workdir = Path(tempfile.mkdtemp(prefix="bankdp_stream_"))

    def last_offsets(self, topic: str) -> dict[int, int]:
        cur = self.con.cursor()
        try:
            cur.execute(
                f"select _kafka_partition, max(_kafka_offset) from {self.table} where _kafka_topic = %s group by 1",
                (topic,),
            )
            return {int(p): int(o) for p, o in cur.fetchall()}
        finally:
            cur.close()

    def write(self, frame: pd.DataFrame) -> None:
        name = _batch_name(str(frame["_kafka_topic"].iloc[0]), frame)
        local = self.workdir / name
        frame.to_csv(local, index=False, compression={"method": "gzip", "compresslevel": 1})
        cur = self.con.cursor()
        try:
            cur.execute(
                f"put 'file://{local.as_posix()}' {self.stage_path} "
                "auto_compress = false source_compression = gzip overwrite = true"
            )
            cur.execute(
                f"""
                copy into {self.table}
                from {self.stage_path}
                files = ('{name}')
                file_format = (format_name = 'RAW.OPS.FF_LANDING_CSV')
                match_by_column_name = case_insensitive
                include_metadata = (_ingested_at = METADATA$START_SCAN_TIME)
                on_error = abort_statement
                """
            )
            result = cur.fetchall()
            loaded = sum(int(r[3]) for r in result if len(r) > 3 and str(r[1]).upper() == "LOADED")
            if loaded != len(frame):
                raise RuntimeError(f"COPY landed {loaded} of {len(frame)} rows for {name}: {result}")
            cur.execute(f"remove {self.stage_path}{name}")
        finally:
            cur.close()
            local.unlink(missing_ok=True)

    def close(self) -> None:
        self.con.close()
