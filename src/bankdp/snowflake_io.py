from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)


def connect(role: str | None = None, warehouse: str | None = None, query_tag: str = "bank_platform_python"):
    """Connect with key pair auth from environment variables, or a named connection from connections.toml."""
    import snowflake.connector

    params: dict = {"session_parameters": {"QUERY_TAG": query_tag}}
    if role or os.getenv("SNOWFLAKE_ROLE"):
        params["role"] = role or os.environ["SNOWFLAKE_ROLE"]
    if warehouse or os.getenv("SNOWFLAKE_WAREHOUSE"):
        params["warehouse"] = warehouse or os.environ["SNOWFLAKE_WAREHOUSE"]

    if os.getenv("SNOWFLAKE_CONNECTION"):
        return snowflake.connector.connect(connection_name=os.environ["SNOWFLAKE_CONNECTION"], **params)

    missing = [v for v in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH") if not os.getenv(v)]
    if missing:
        raise SystemExit(f"missing environment variables: {', '.join(missing)} (see docs/snowflake_runbook.md)")
    params.update(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=str(Path(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]).expanduser()),
    )
    if os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
        params["private_key_file_pwd"] = os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"]
    return snowflake.connector.connect(**params)


def query_df(con, sql: str, params=None) -> pd.DataFrame:
    cur = con.cursor()
    try:
        cur.execute(sql, params)
        if not cur.description:
            return pd.DataFrame()
        try:
            return cur.fetch_pandas_all()
        except Exception:  # SHOW and CALL results are not Arrow backed
            return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    finally:
        cur.close()


def execute(con, sql: str, params=None) -> str:
    """Run a statement and return its query id."""
    cur = con.cursor()
    try:
        cur.execute(sql, params)
        return cur.sfqid
    finally:
        cur.close()
