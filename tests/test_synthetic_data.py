"""Phase 1: synthetic SAP-style dataset acceptance tests."""
import glob
import json
import os

import pandas as pd
import pytest

from dfp.synthetic import FORMATS, SyntheticConfig, SyntheticGenerator, easter
from dfp.synthetic_validation import run_checks


def test_all_acceptance_checks_pass(synthetic_small):
    checks = run_checks(synthetic_small["gen"], synthetic_small["bb"])
    failed = [c for c in checks if not c[1]]
    assert not failed, failed
    assert len(checks) >= 60


def test_files_written_and_readable(synthetic_small):
    man = synthetic_small["manifest"]
    assert set(man["source"]) == set(FORMATS)
    for _, row in man.sample(25, random_state=1).iterrows():
        fmt = FORMATS[row["source"]]
        df = {"csv": pd.read_csv, "parquet": pd.read_parquet,
              "json": lambda p: pd.read_json(p, lines=True)}[fmt](row["path"])
        assert len(df) == row["rows"]


def test_transactional_sources_split_monthly(synthetic_small):
    man = synthetic_small["manifest"]
    pos_b1 = man[(man.batch_id == "batch_001") & (man.source == "pos")]
    assert len(pos_b1) >= 30  # ~36 months of history less the batch-2 window


def test_alt_date_format_rows_isolated_not_lost(synthetic_small):
    man = synthetic_small["manifest"]
    misc = man[(man.source == "sales") & man.path.str.contains("_misc")]
    assert misc["rows"].sum() > 0
    df = pd.read_csv(misc.iloc[0]["path"])
    assert df["posting_date"].str.match(r"^\d{2}\.\d{2}\.\d{4}$").all()


def test_ground_truth_outputs(synthetic_small):
    root = synthetic_small["root"] / "_synthetic_ground_truth"
    for f in ("sku_demand_patterns.csv", "injected_issues.csv", "weekly_demand_truth.csv",
              "genuine_demand_spikes.csv", "generation_manifest.json", "README_SYNTHETIC.txt"):
        assert (root / f).exists(), f
    meta = json.loads((root / "generation_manifest.json").read_text())
    assert "SYNTHETIC" in meta["notice"]
    truth = pd.read_csv(root / "weekly_demand_truth.csv")
    assert truth["week_start"].nunique() == 156


def test_raw_batches_contain_no_ground_truth_columns(synthetic_small):
    for batch in synthetic_small["bb"].batches.values():
        for name, df in batch.items():
            leaked = [c for c in df.columns if c.startswith("_") or "pattern" in c]
            assert not leaked, (name, leaked)


def test_batch2_new_sku_and_scd_changes(synthetic_small):
    b1 = synthetic_small["bb"].batches["batch_001"]["products"]
    b2 = synthetic_small["bb"].batches["batch_002"]["products"]
    assert len(b2) == len(b1) + 1
    merged = b1.merge(b2, on="sku_id", suffixes=("_1", "_2"))
    changed = merged[(merged.lifecycle_status_1 != merged.lifecycle_status_2) | (merged.product_name_1 != merged.product_name_2)]
    assert len(changed) >= 2


def test_deterministic_with_seed():
    kw = dict(history_weeks=110, skus_per_category=2, stores_per_region=3, future_weeks=104)
    a = SyntheticGenerator(SyntheticConfig(seed=7, **kw)).generate()
    b = SyntheticGenerator(SyntheticConfig(seed=7, **kw)).generate()
    c = SyntheticGenerator(SyntheticConfig(seed=8, **kw)).generate()
    assert a.tables["pos"]["quantity"].sum() == b.tables["pos"]["quantity"].sum()
    assert a.tables["pos"]["quantity"].sum() != c.tables["pos"]["quantity"].sum()


def test_history_end_must_be_monday():
    with pytest.raises(AssertionError):
        SyntheticConfig(history_end_week_start="2026-11-18").history_start


@pytest.mark.parametrize("year,expected", [(2024, "2024-03-31"), (2025, "2025-04-20"), (2026, "2026-04-05")])
def test_easter(year, expected):
    assert easter(year).isoformat() == expected
