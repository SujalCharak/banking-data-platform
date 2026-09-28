from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

Query = Callable[[str], pd.DataFrame]


@dataclass
class CheckResult:
    name: str
    expected: float
    actual: float
    passed: bool
    detail: str = ""


@dataclass
class ValidationReport:
    checks: list[CheckResult] = field(default_factory=list)
    aml: pd.DataFrame | None = None

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def to_markdown(self) -> str:
        lines = ["| Check | Expected | Actual | Result |", "|---|---:|---:|---|"]
        for c in self.checks:
            lines.append(
                f"| {c.name} | {c.expected:,.0f} | {c.actual:,.0f} | {'PASS' if c.passed else 'FAIL'} {c.detail} |"
            )
        if self.aml is not None:
            lines += [
                "",
                "| Rule | Injected cases | Detected | Recall | Alerts | Alerts on injected cases | Precision |",
                "|---|---:|---:|---:|---:|---:|---:|",
            ]
            for r in self.aml.itertuples():
                cells = [r.rule, r.cases, r.detected, f"{r.recall:.0%}", r.alerts, r.true_alerts, f"{r.precision:.0%}"]
                lines.append("| " + " | ".join(str(c) for c in cells) + " |")
        return "\n".join(lines)


def _lower_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.lower() for c in df.columns]
    return df


def validate(query: Query, truth_dir: Path, schemas: dict[str, str]) -> ValidationReport:
    """Compare pipeline output with what the generator deliberately injected."""
    q = lambda sql: _lower_columns(query(sql))  # noqa: E731
    dq, core, risk = schemas["data_quality"], schemas["core"], schemas["risk"]
    issues = pd.read_csv(truth_dir / "injected_issues.csv", dtype=str)
    txn_issues = issues[issues["entity"] == "transactions"]
    report = ValidationReport()

    rejected = q(f"select distinct txn_id, rejection_reason from {dq}.dq_rejected_transactions")
    for issue in ["INVALID_AMOUNT", "NEGATIVE_AMOUNT", "UNKNOWN_ACCOUNT", "MISSING_TIMESTAMP"]:
        want = set(txn_issues.loc[txn_issues["issue"] == issue, "key"])
        got = set(rejected.loc[rejected["rejection_reason"] == issue, "txn_id"])
        report.checks.append(
            CheckResult(
                f"rejected as {issue}",
                len(want),
                len(got),
                want == got,
                "" if want == got else f"(missing {len(want - got)}, extra {len(got - want)})",
            )
        )

    summary = q(
        f"select sum(duplicate_rows) as dup, sum(late_arriving_rows) as late from {dq}.dq_transaction_file_summary"
    )
    dup_expected = int((txn_issues["issue"] == "DUPLICATE_ROW").sum())
    report.checks.append(
        CheckResult(
            "duplicate rows detected", dup_expected, int(summary["dup"][0]), dup_expected == int(summary["dup"][0])
        )
    )

    hard = set(
        txn_issues.loc[
            txn_issues["issue"].isin(["INVALID_AMOUNT", "NEGATIVE_AMOUNT", "UNKNOWN_ACCOUNT", "MISSING_TIMESTAMP"]),
            "key",
        ]
    )
    late = set(txn_issues.loc[txn_issues["issue"] == "LATE_ARRIVAL", "key"]) - hard
    report.checks.append(
        CheckResult("late arrivals detected", len(late), int(summary["late"][0]), len(late) == int(summary["late"][0]))
    )

    warned = q(f"select count(*) as n from {core}.fct_transactions where warning_reason = 'MISSING_MERCHANT'")
    want_warn = len(set(txn_issues.loc[txn_issues["issue"] == "MISSING_MERCHANT", "key"]))
    report.checks.append(
        CheckResult("missing merchant warnings", want_warn, int(warned["n"][0]), want_warn == int(warned["n"][0]))
    )

    lower = q(f"select count(*) as n from {core}.fct_transactions where currency <> upper(currency)")
    report.checks.append(
        CheckResult("lowercase currency left after cleaning", 0, int(lower["n"][0]), int(lower["n"][0]) == 0)
    )

    report.aml = _aml_quality(q, truth_dir, risk, schemas["intermediate"])
    return report


def _aml_quality(q: Callable[[str], pd.DataFrame], truth_dir: Path, risk: str, intermediate: str) -> pd.DataFrame:
    truth = pd.read_csv(truth_dir / "injected_anomalies.csv", dtype=str)
    hits = q(f"select alert_id, rule_code, txn_id from {intermediate}.int_aml__alert_transactions")
    alerts = q(f"select alert_id, rule_code from {risk}.rpt_aml_alerts")
    rows = []
    for rule, cases in truth.groupby("rule"):
        rule_hits = hits[hits["rule_code"] == rule]
        joined = rule_hits.merge(cases, on="txn_id")
        detected_cases = joined["case_id"].nunique()
        true_alerts = joined["alert_id"].nunique()
        total_alerts = int((alerts["rule_code"] == rule).sum())
        rows.append(
            {
                "rule": rule,
                "cases": cases["case_id"].nunique(),
                "detected": detected_cases,
                "recall": detected_cases / cases["case_id"].nunique(),
                "alerts": total_alerts,
                "true_alerts": true_alerts,
                "precision": true_alerts / total_alerts if total_alerts else 0.0,
            }
        )
    return pd.DataFrame(rows)
