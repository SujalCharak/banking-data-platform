from __future__ import annotations

import logging
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from bankdp.snowflake_io import connect, execute, query_df

log = logging.getLogger(__name__)

WAREHOUSE = "BANK_BENCH_WH"
BENCH = "ANALYTICS.BENCH"
CREDITS_PER_HOUR = {"XSMALL": 1, "SMALL": 2, "MEDIUM": 4, "LARGE": 8}

MONTH_REPORT = """
select txn_category, count(*) as txns, sum(amount_eur) as amount_eur
from {table}
where {predicate}
group by txn_category
"""

RUNNING_BALANCE_SELF_JOIN = f"""
select t.account_id, t.txn_id, sum(p.signed_amount_eur) as running_total_eur
from {BENCH}.TXN_CLUSTERED t
join {BENCH}.TXN_CLUSTERED p
  on p.account_id = t.account_id
 and p.txn_ts <= t.txn_ts
 and p.txn_date between '2024-01-01' and '2024-12-31'
where t.txn_date between '2024-01-01' and '2024-12-31'
  and t.is_posted and p.is_posted
group by t.account_id, t.txn_id
"""

RUNNING_BALANCE_WINDOW = f"""
select account_id, txn_id,
       sum(signed_amount_eur) over (partition by account_id order by txn_ts, txn_id
                                    rows between unbounded preceding and current row) as running_total_eur
from {BENCH}.TXN_CLUSTERED
where txn_date between '2024-01-01' and '2024-12-31'
  and is_posted
"""

DISTINCT_EXACT = f"""
select date_trunc('month', txn_date) as month, txn_category, count(distinct account_id) as active_accounts
from {BENCH}.TXN_CLUSTERED
group by 1, 2
"""

FULL_HISTORY = f"""
select date_trunc('month', txn_date) as month, channel, txn_category,
       count(*) as txns, sum(amount_eur) as amount_eur, avg(amount_eur) as avg_eur
from {BENCH}.TXN_UNCLUSTERED
group by 1, 2, 3
"""


@dataclass
class Variant:
    label: str
    sql: str
    size: str = "XSMALL"


@dataclass
class Experiment:
    name: str
    question: str
    variants: list[Variant]
    requires: str | None = None


@dataclass
class RunResult:
    experiment: str
    variant: str
    size: str
    cold_s: float
    warm_median_s: float
    partitions_scanned: int | None
    partitions_total: int | None
    query_ids: list[str] = field(default_factory=list)

    @property
    def est_credits(self) -> float:
        return self.warm_median_s / 3600 * CREDITS_PER_HOUR[self.size]


def experiments(sample_txn_id: str) -> list[Experiment]:
    march = "txn_date between '2024-03-01' and '2024-03-31'"
    return [
        Experiment(
            "clustering",
            "How much does clustering on txn_date reduce the data scanned by a one month report?",
            [
                Variant("unclustered", MONTH_REPORT.format(table=f"{BENCH}.TXN_UNCLUSTERED", predicate=march)),
                Variant("clustered by txn_date", MONTH_REPORT.format(table=f"{BENCH}.TXN_CLUSTERED", predicate=march)),
            ],
        ),
        Experiment(
            "sargable_predicate",
            "Does wrapping the filter column in a function defeat pruning on a clustered table?",
            [
                Variant(
                    "to_char(txn_date, 'YYYY-MM') = '2024-03'",
                    MONTH_REPORT.format(
                        table=f"{BENCH}.TXN_CLUSTERED", predicate="to_char(txn_date, 'YYYY-MM') = '2024-03'"
                    ),
                ),
                Variant("txn_date between ...", MONTH_REPORT.format(table=f"{BENCH}.TXN_CLUSTERED", predicate=march)),
            ],
        ),
        Experiment(
            "point_lookup",
            "How fast is a single transaction lookup by id with and without search optimization?",
            [
                Variant(
                    "no search optimization", f"select * from {BENCH}.TXN_UNCLUSTERED where txn_id = '{sample_txn_id}'"
                ),
                Variant(
                    "search optimization",
                    f"select * from {BENCH}.TXN_SEARCH_OPTIMIZED where txn_id = '{sample_txn_id}'",
                ),
            ],
            requires="search_optimization",
        ),
        Experiment(
            "running_balance_rewrite",
            "Running balance per account over one year: triangular self join versus a window function.",
            [
                Variant("self join", RUNNING_BALANCE_SELF_JOIN),
                Variant("window function", RUNNING_BALANCE_WINDOW),
            ],
        ),
        Experiment(
            "approximate_distinct",
            "Monthly active accounts: exact COUNT(DISTINCT) versus HyperLogLog.",
            [
                Variant("count(distinct)", DISTINCT_EXACT),
                Variant(
                    "approx_count_distinct",
                    DISTINCT_EXACT.replace("count(distinct account_id)", "approx_count_distinct(account_id)"),
                ),
            ],
        ),
        Experiment(
            "warehouse_sizing",
            "Full history scan on each warehouse size: does doubling the size halve the time at the same cost?",
            [Variant(size, FULL_HISTORY, size) for size in ("XSMALL", "SMALL", "MEDIUM")],
        ),
    ]


def _set_size(con, size: str) -> None:
    execute(con, f"alter warehouse {WAREHOUSE} set warehouse_size = {size} wait_for_completion = true")


def _cold_start(con) -> None:
    """Suspending drops the warehouse's local disk cache so the first run reads from storage."""
    try:
        execute(con, f"alter warehouse {WAREHOUSE} suspend")
    except Exception:  # already suspended
        pass
    execute(con, f"alter warehouse {WAREHOUSE} resume if suspended")


def _pruning(con, query_id: str) -> tuple[int | None, int | None]:
    try:
        ops = query_df(
            con,
            "select sum(operator_statistics:pruning:partitions_scanned::number), "
            "sum(operator_statistics:pruning:partitions_total::number) "
            "from table(get_query_operator_stats(%s)) where operator_type = 'TableScan'",
            (query_id,),
        )
        if not ops.empty and ops.iloc[0, 1] is not None:
            return int(ops.iloc[0, 0]), int(ops.iloc[0, 1])
    except Exception as exc:
        log.warning("operator stats unavailable for %s: %s", query_id, exc)
    return None, None


def _search_optimization_ready(con, timeout_s: int = 1200) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            rows = query_df(con, f"show tables like 'TXN_SEARCH_OPTIMIZED' in schema {BENCH}")
        except Exception:
            return False
        if rows.empty or "search_optimization_progress" not in rows.columns:
            return False
        progress = rows["search_optimization_progress"].iloc[0]
        if progress is not None and int(progress) >= 100:
            return True
        log.info("search optimization build at %s%%, waiting", progress)
        time.sleep(30)
    return False


def run_benchmarks(sql_dir: Path, report_path: Path, repeats: int = 3) -> list[RunResult]:
    con = connect(role="BANK_ADMIN", warehouse=WAREHOUSE, query_tag="bank_platform_benchmark")
    results: list[RunResult] = []
    try:
        execute(con, "alter session set use_cached_result = false")
        sample = query_df(con, f"select txn_id from {BENCH}.TXN_UNCLUSTERED limit 1").iloc[0, 0]
        depth_sql = "parse_json(system$clustering_information('{}', '(txn_date)')):average_depth::float"
        depth = query_df(
            con,
            f"select {depth_sql.format(BENCH + '.TXN_UNCLUSTERED')} as u, "
            f"{depth_sql.format(BENCH + '.TXN_CLUSTERED')} as c",
        )
        rows = int(query_df(con, f"select count(*) from {BENCH}.TXN_UNCLUSTERED").iloc[0, 0])
        search_ready = _search_optimization_ready(con)

        for exp in experiments(sample):
            if exp.requires == "search_optimization" and not search_ready:
                log.warning("skipping %s: search optimization not available (needs Enterprise edition)", exp.name)
                continue
            for v in exp.variants:
                _set_size(con, v.size)
                _cold_start(con)
                timings, qids = [], []
                for _ in range(repeats):
                    start = time.perf_counter()
                    qids.append(execute(con, v.sql))
                    timings.append(time.perf_counter() - start)
                scanned, total = _pruning(con, qids[0])
                result = RunResult(
                    exp.name,
                    v.label,
                    v.size,
                    timings[0],
                    statistics.median(timings[1:] or timings),
                    scanned,
                    total,
                    qids,
                )
                log.info("%s / %s: cold %.2fs warm %.2fs", exp.name, v.label, result.cold_s, result.warm_median_s)
                results.append(result)
        _set_size(con, "XSMALL")
        execute(con, f"alter warehouse {WAREHOUSE} suspend")
    finally:
        con.close()

    report_path.write_text(_render(results, experiments("x"), rows, depth.iloc[0].to_dict(), repeats))
    log.info("wrote %s", report_path)
    return results


def _render(results: list[RunResult], exps: list[Experiment], rows: int, depth: dict, repeats: int) -> str:
    fmt = lambda v, spec: "n/a" if v is None else format(v, spec)  # noqa: E731
    out = [
        "# Benchmark results",
        "",
        f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC on {rows:,} benchmark rows. "
        f"Result cache off; the warehouse is suspended before each variant, so the first run is cold. "
        f"Warm is the median of the remaining {repeats - 1} runs.",
        "",
        f"Clustering depth on txn_date: unclustered {fmt(depth.get('U'), '.1f')}, "
        f"clustered {fmt(depth.get('C'), '.1f')} (lower is better; 1.0 is perfect).",
    ]
    for exp in exps:
        rs = [r for r in results if r.experiment == exp.name]
        if not rs:
            continue
        out += [
            "",
            f"## {exp.name}",
            "",
            exp.question,
            "",
            "| Variant | Warehouse | Cold (s) | Warm (s) | Partitions scanned | Est. credits (warm) |",
            "|---|---|---:|---:|---:|---:|",
        ]
        for r in rs:
            parts = "n/a" if r.partitions_total is None else f"{r.partitions_scanned:,} / {r.partitions_total:,}"
            out.append(
                f"| {r.variant} | {r.size} | {r.cold_s:.2f} | {r.warm_median_s:.2f} | {parts} | {r.est_credits:.5f} |"
            )
    out += [
        "",
        "Estimated credits = warm seconds / 3600 x credits per hour for the size, "
        "ignoring the 60 second minimum per resume.",
        "",
    ]
    return "\n".join(out)
