from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from bankdp.snowflake_io import connect, query_df

log = logging.getLogger(__name__)

QUERIES = {
    "Warehouse credits by day": """
        select to_date(start_time) as day, warehouse_name,
               round(sum(credits_used_compute), 4) as compute_credits,
               round(sum(credits_used_cloud_services), 4) as cloud_services_credits
        from snowflake.account_usage.warehouse_metering_history
        where start_time >= dateadd(day, -%(days)s, current_timestamp())
          and startswith(warehouse_name, 'BANK_')
        group by 1, 2
        order by 1, 2
    """,
    "Compute credits by workload (query tag)": """
        select coalesce(nullif(query_tag, ''), '(untagged)') as query_tag, warehouse_name,
               count(*) as queries,
               round(sum(credits_attributed_compute), 4) as credits
        from snowflake.account_usage.query_attribution_history
        where start_time >= dateadd(day, -%(days)s, current_timestamp())
          and startswith(warehouse_name, 'BANK_')
        group by 1, 2
        order by credits desc
    """,
    "Most expensive queries": """
        select a.query_tag, a.warehouse_name, round(a.credits_attributed_compute, 5) as credits,
               round(q.total_elapsed_time / 1000, 1) as elapsed_s,
               round(q.bytes_scanned / 1e6, 0) as mb_scanned,
               q.partitions_scanned, q.partitions_total,
               left(regexp_replace(q.query_text, '\\\\s+', ' '), 90) as query_start
        from snowflake.account_usage.query_attribution_history a
        join snowflake.account_usage.query_history q on q.query_id = a.query_id
        where a.start_time >= dateadd(day, -%(days)s, current_timestamp())
          and startswith(a.warehouse_name, 'BANK_')
        order by a.credits_attributed_compute desc
        limit 10
    """,
    "Serverless task credits": """
        select task_name, round(sum(credits_used), 4) as credits, count(*) as runs
        from snowflake.account_usage.serverless_task_history
        where start_time >= dateadd(day, -%(days)s, current_timestamp())
          and database_name = 'RAW'
        group by 1
        order by credits desc
    """,
    "Storage by schema (MB)": """
        select table_catalog as database_name, table_schema,
               round(sum(active_bytes) / 1e6, 1) as active_mb,
               round(sum(time_travel_bytes) / 1e6, 1) as time_travel_mb,
               round(sum(failsafe_bytes) / 1e6, 1) as failsafe_mb
        from snowflake.account_usage.table_storage_metrics
        where table_catalog in ('RAW', 'ANALYTICS') and deleted = false
        group by 1, 2
        order by active_mb desc
    """,
}


def _to_markdown(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows yet. Account usage views can lag by up to a few hours._"
    df.columns = [c.lower() for c in df.columns]
    header = "| " + " | ".join(df.columns) + " |"
    sep = "|" + "|".join("---" for _ in df.columns) + "|"
    body = ["| " + " | ".join("" if pd.isna(v) else str(v) for v in row) + " |" for row in df.itertuples(index=False)]
    return "\n".join([header, sep, *body])


def build_cost_report(report_path: Path, days: int = 14) -> None:
    con = connect(role="BANK_ADMIN", warehouse="BANK_ANALYST_WH", query_tag="bank_platform_cost_report")
    sections = [
        "# Cost report",
        "",
        f"Last {days} days, generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC from SNOWFLAKE.ACCOUNT_USAGE.",
    ]
    try:
        for title, sql in QUERIES.items():
            try:
                df = query_df(con, sql, {"days": days})
                sections += ["", f"## {title}", "", _to_markdown(df)]
            except Exception as exc:
                log.warning("%s failed: %s", title, exc)
                sections += ["", f"## {title}", "", f"_Query failed: {exc}_"]
    finally:
        con.close()
    report_path.write_text("\n".join(sections) + "\n")
    log.info("wrote %s", report_path)
