import re
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from bankdp.generate import BankDataGenerator, GenConfig, make_iban
from bankdp.local_load import load_landing
from bankdp.schema import RAW_COLUMNS

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    out = tmp_path_factory.mktemp("data")
    cfg = GenConfig(customers=200, days=150, out_dir=out)
    summary = BankDataGenerator(cfg).run()
    return cfg, summary


def read_all(folder: Path) -> pd.DataFrame:
    return pd.concat([pd.read_csv(f, dtype=str) for f in sorted(folder.glob("*.csv.gz"))], ignore_index=True)


def test_iban_check_digits_are_valid():
    iban = make_iban("DEMO", 42)
    rearranged = iban[4:] + iban[:4]
    assert int("".join(str(int(c, 36)) for c in rearranged)) % 97 == 1


def test_generation_is_deterministic(generated, tmp_path):
    cfg, summary = generated
    again = BankDataGenerator(GenConfig(customers=cfg.customers, days=cfg.days, out_dir=tmp_path)).run()
    assert again == summary


def test_every_entity_lands_files_with_expected_columns(generated):
    cfg, _ = generated
    for entity, columns in RAW_COLUMNS.items():
        files = sorted((cfg.out_dir / "landing" / entity).glob("*.csv.gz"))
        assert files, entity
        header = pd.read_csv(files[-1], nrows=0).columns.tolist()
        assert set(header) <= set(columns), entity


def test_device_id_column_appears_after_schema_drift_day(generated):
    cfg, _ = generated
    files = sorted((cfg.out_dir / "landing" / "transactions").glob("*.csv.gz"))
    first, last = pd.read_csv(files[0], nrows=0).columns, pd.read_csv(files[-1], nrows=0).columns
    assert "device_id" not in first
    assert "device_id" in last


def test_duplicates_are_exact_copies_of_real_transactions(generated):
    cfg, _ = generated
    tx = read_all(cfg.out_dir / "landing" / "transactions")
    issues = pd.read_csv(cfg.out_dir / "ground_truth" / "injected_issues.csv", dtype=str)
    dup_ids = set(issues.loc[issues["issue"] == "DUPLICATE_ROW", "key"])
    counts = tx["txn_id"].value_counts()
    assert set(counts[counts > 1].index) == dup_ids
    assert tx.drop_duplicates().shape[0] == tx.shape[0] - len(dup_ids)


def test_anomaly_ground_truth_points_at_real_rows(generated):
    cfg, _ = generated
    tx = read_all(cfg.out_dir / "landing" / "transactions")
    truth = pd.read_csv(cfg.out_dir / "ground_truth" / "injected_anomalies.csv", dtype=str)
    assert set(truth["rule"]) == {"STRUCTURING", "VELOCITY", "OUTLIER"}
    assert truth["txn_id"].isin(tx["txn_id"]).all()


def test_local_load_is_idempotent(generated, tmp_path):
    cfg, _ = generated
    db = tmp_path / "bank.duckdb"
    first = load_landing(db, cfg.out_dir / "landing")
    second = load_landing(db, cfg.out_dir / "landing")
    assert first["transactions"] > 0
    assert all(v == 0 for v in second.values())
    con = duckdb.connect(str(db))
    loaded = con.execute("select count(*) from raw.transactions").fetchone()[0]
    assert loaded == first["transactions"]


def test_raw_ddl_matches_python_schema():
    ddl = (ROOT / "snowflake" / "01_databases_and_raw.sql").read_text().lower()
    for entity, columns in RAW_COLUMNS.items():
        match = re.search(rf"create table if not exists raw\.bank\.{entity} \((.*?)\);", ddl, re.S)
        assert match, entity
        declared = [c.strip().split()[0] for c in match.group(1).split(",") if c.strip()]
        assert declared == columns + ["_source_file", "_source_row", "_loaded_at"], entity


def test_stream_ddl_matches_python_schema():
    from bankdp.schema import STREAM_COLUMNS

    ddl = (ROOT / "snowflake" / "06_streaming.sql").read_text().lower()
    match = re.search(r"create table if not exists raw\.bank\.transactions_stream \((.*?)\)\s*comment", ddl, re.DOTALL)
    assert match
    declared = [c.strip().split()[0] for c in match.group(1).split(",") if c.strip()]
    assert declared == STREAM_COLUMNS + ["_ingested_at"]
