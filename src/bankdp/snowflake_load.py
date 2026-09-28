from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

from bankdp.schema import RAW_COLUMNS
from bankdp.snowflake_io import connect

log = logging.getLogger(__name__)

STAGE = "@RAW.OPS.LANDING"
LOAD_ORDER = ["customers", "accounts", "merchants", "fx_rates", "transactions"]


def _file_date(path: Path) -> date:
    stamp = path.name.split("_")[-1].split(".")[0]
    return date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:8]))


def load_landing_to_snowflake(landing: Path, entities: list[str] | None = None, through: date | None = None) -> None:
    """Upload landing files to the internal stage, then load them with RAW.OPS.SP_LOAD_LANDING."""
    entities = entities or LOAD_ORDER
    unknown = set(entities) - set(RAW_COLUMNS)
    if unknown:
        raise SystemExit(f"unknown entities: {sorted(unknown)}")

    con = connect(role="BANK_LOADER", warehouse="BANK_LOAD_WH", query_tag="bank_platform_load")
    try:
        cur = con.cursor()
        for entity in [e for e in LOAD_ORDER if e in entities]:
            files = sorted((landing / entity).glob(f"{entity}_*.csv.gz"))
            if through:
                files = [f for f in files if _file_date(f) <= through]
            if not files:
                log.info("%s: nothing to upload", entity)
                continue

            if through is None:
                pattern = (landing / entity).resolve().as_posix() + f"/{entity}_*.csv.gz"
                cur.execute(
                    f"put 'file://{pattern}' {STAGE}/{entity}/ "
                    "auto_compress = false source_compression = gzip parallel = 8 overwrite = false"
                )
            else:
                for f in files:
                    cur.execute(
                        f"put 'file://{f.resolve().as_posix()}' {STAGE}/{entity}/ "
                        "auto_compress = false source_compression = gzip overwrite = false"
                    )
            log.info("%s: uploaded %d files", entity, len(files))

            cur.execute("call RAW.OPS.SP_LOAD_LANDING(%s)", (entity,))
            summary = json.loads(cur.fetchone()[0])
            log.info("%s: %s", entity, summary)
            if summary.get("files_with_errors"):
                log.warning(
                    "%s: %s files had parse errors, see RAW.OPS.LOAD_AUDIT", entity, summary["files_with_errors"]
                )
    finally:
        con.close()
