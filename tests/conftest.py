import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


@pytest.fixture(scope="session")
def config_dir():
    return ROOT / "config"


@pytest.fixture(scope="session")
def synthetic_small(tmp_path_factory):
    """Generate the default 'small' dataset once per test session (~20 s) and write it to disk."""
    from dfp.synthetic import SyntheticConfig, generate_all

    root = tmp_path_factory.mktemp("synthetic")
    cfg = SyntheticConfig.from_scale("small")
    gen, bb, manifest = generate_all(cfg, str(root / "staging"), str(root / "_synthetic_ground_truth"))
    return {"cfg": cfg, "gen": gen, "bb": bb, "manifest": manifest, "root": root}


# ---------------------------------------------------------------------------------------------
# Local Spark fixtures (Silver / Gold / EDA tests). Skipped automatically if pyspark is missing.
# ---------------------------------------------------------------------------------------------
TS = {"batch_001": "2026-11-24 10:00:00", "batch_002": "2026-11-24 11:00:00"}


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("dfp-tests")
         .config("spark.sql.ansi.enabled", "true").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timeZone", "UTC")
         .config("spark.driver.memory", "4g").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _to_bronze_pdf(df: pd.DataFrame, source: str, batch: str) -> pd.DataFrame:
    """Mimic Bronze: every delivered value as a string (CSV/JSON without type inference) + lineage."""
    out = pd.DataFrame({c: [None if pd.isna(v) else str(v) for v in df[c]] for c in df.columns})
    out["_source_file"] = f"/Volumes/x/raw/source_files/landing/{source}/{batch}/{source}.csv"
    out["_ingested_at"] = pd.Timestamp(TS[batch]).to_pydatetime()
    out["_ingestion_run_id"] = f"run_{batch}"
    out["_batch_id"] = batch
    return out


@pytest.fixture(scope="session")
def bronze(spark, synthetic_small):
    """{source: Spark DataFrame of batch_001 ∪ batch_002 in Bronze shape}"""
    from dfp import silver as sv
    out = {}
    for source in sv.SPECS:
        parts = [_to_bronze_pdf(b[source], source, name) for name, b in synthetic_small["bb"].batches.items() if source in b]
        pdf = pd.concat(parts, ignore_index=True)
        string_cols = [c for c in pdf.columns if c != "_ingested_at"]
        schema = ", ".join(f"`{c}` string" for c in string_cols) + ", `_ingested_at` timestamp"
        rows = [tuple(None if (v is None or (isinstance(v, float) and v != v)) else
                      (v.to_pydatetime() if isinstance(v, pd.Timestamp) else v) for v in r)
                for r in pdf[string_cols + ["_ingested_at"]].itertuples(index=False, name=None)]
        out[source] = spark.createDataFrame(rows, schema)
    return out


@pytest.fixture(scope="session")
def refs(spark, synthetic_small):
    g = synthetic_small["gen"]
    return {"sku": spark.createDataFrame(g.products[["sku_id"]]),
            "store": spark.createDataFrame(g.tables["stores"][["store_id"]]),
            "region": spark.createDataFrame(g.tables["regions"][["region_id"]])}


@pytest.fixture(scope="session")
def issues(synthetic_small):
    return pd.DataFrame(synthetic_small["bb"].issues)
