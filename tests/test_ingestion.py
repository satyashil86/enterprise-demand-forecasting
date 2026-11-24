"""Phase 2: ingestion framework (pure-Python parts; Auto Loader / COPY INTO run on Databricks)."""
import os

import pytest

from dfp import ingestion as ing
from dfp import uc
from dfp.config import load_config


# ---------------------------------------------------------------- batch release
def test_release_sequence_on_real_staged_batches(synthetic_small, tmp_path):
    staging = str(synthetic_small["root"] / "staging")
    landing = str(tmp_path / "landing")
    man = synthetic_small["manifest"]

    assert ing.list_batches(staging) == ["batch_001", "batch_002"]
    assert ing.next_unreleased_batch(staging, landing) == "batch_001"

    n1 = ing.release_batch(staging, landing, "batch_001")
    assert n1 == (man.batch_id == "batch_001").sum()
    assert ing.released_batches(landing) == ["batch_001"]
    assert ing.next_unreleased_batch(staging, landing) == "batch_002"

    # landing layout: landing/<source>/<batch>/<file>
    assert os.path.isdir(os.path.join(landing, "pos", "batch_001"))
    assert len(os.listdir(os.path.join(landing, "products", "batch_001"))) == 1

    # re-release rewrites the same paths (no new files)
    before = sorted(os.path.join(r, f) for r, _, fs in os.walk(landing) for f in fs)
    ing.release_batch(staging, landing, "batch_001")
    after = sorted(os.path.join(r, f) for r, _, fs in os.walk(landing) for f in fs)
    assert before == after

    ing.release_batch(staging, landing, "batch_002")
    assert ing.next_unreleased_batch(staging, landing) is None


def test_plan_release_missing_batch(tmp_path):
    with pytest.raises(FileNotFoundError):
        ing.plan_release(str(tmp_path), str(tmp_path / "l"), "batch_009")


def test_list_batches_ignores_other_folders(tmp_path):
    for d in ("batch_001", "batch_002", "tmp", "batch_x"):
        (tmp_path / d).mkdir()
    assert ing.list_batches(str(tmp_path)) == ["batch_001", "batch_002"]
    assert ing.list_batches(str(tmp_path / "missing")) == []


# ---------------------------------------------------------------- options and SQL
def test_autoloader_options_csv_and_parquet():
    o = ing.autoloader_options("csv", "/Volumes/c/raw/checkpoints/bronze/pos/_schema")
    assert o["cloudFiles.format"] == "csv" and o["header"] == "true"
    assert o["cloudFiles.schemaEvolutionMode"] == "addNewColumns"
    assert o["cloudFiles.inferColumnTypes"] == "false"
    assert o["pathGlobFilter"] == "*.csv"
    p = ing.autoloader_options("parquet", "/x")
    assert "header" not in p and p["pathGlobFilter"] == "*.parquet"


def test_copy_into_sql():
    sql = ing.copy_into_sql("`c`.`bronze`.`products_raw`", "/Volumes/c/raw/source_files/landing/products", "csv", "run_1")
    assert sql.startswith("COPY INTO `c`.`bronze`.`products_raw`")
    assert "FILEFORMAT = CSV" in sql and "PATTERN = 'batch_*/*.csv'" in sql
    assert "'mergeSchema' = 'true'" in sql and "_metadata.file_path" in sql
    assert "'run_1'" in sql and "SYNTHETIC_SAP" in sql


def test_batch_regex_matches_landing_paths():
    import re
    p = "/Volumes/demand_planning/raw/source_files/landing/pos/batch_002/pos_batch_002_202610.csv"
    assert re.search(ing.BATCH_IN_PATH_REGEX, p).group(1) == "batch_002"


def test_methods_from_config(config_dir):
    cfg = load_config("dev", config_dir)
    methods = {s: c["ingestion_method"] for s, c in cfg.sources.items()}
    assert {s for s, m in methods.items() if m == "autoloader"} == {
        "sales", "pos", "orders", "promotions", "pricing", "weather", "inventory"}
    assert {s for s, m in methods.items() if m == "copy_into"} == {"products", "stores", "regions", "suppliers", "calendar"}
    assert all(c["bronze_table"].startswith("bronze.") for c in cfg.sources.values())


# ---------------------------------------------------------------- schema management
def test_schema_drift_ignores_metadata():
    contract = ["a", "b", "c"]
    actual = ["a", "b", "d", "_rescued_data", *ing.METADATA_COLUMNS]
    d = ing.schema_drift(contract, actual)
    assert d == {"not_in_contract": ["d"], "missing_from_delivery": ["c"]}


def test_contract_matches_generated_files(synthetic_small, config_dir):
    """Every column delivered in batch_001 is in the documented contract (new batch_002 columns are not)."""
    cfg = load_config("dev", config_dir)
    for src, df in synthetic_small["bb"].batches["batch_001"].items():
        d = ing.schema_drift(cfg.expected_schemas[src], df.columns)
        assert d == {"not_in_contract": [], "missing_from_delivery": []}, (src, d)
    b2 = synthetic_small["bb"].batches["batch_002"]
    assert ing.schema_drift(cfg.expected_schemas["pos"], b2["pos"].columns)["not_in_contract"] == [
        "loyalty_member_flag", "basket_count"]


@pytest.mark.parametrize("msg,expected", [
    ("[UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_RECORD_WITH_FILE_PATH] Encountered unknown fields", True),
    ("org.apache.spark.sql.catalyst.util.UnknownFieldException: new fields", True),
    ("PERMISSION_DENIED: User does not have USE SCHEMA", False),
])
def test_schema_evolution_error_detection(msg, expected):
    assert ing.is_schema_evolution_error(RuntimeError(msg)) is expected


# ---------------------------------------------------------------- catalog resolution
class _FakeSQL:
    def __init__(self, catalogs):
        self.catalogs = catalogs

    def __call__(self, sql):
        name = sql.split("'")[1]
        rows = [name] if name in self.catalogs else []
        return type("R", (), {"collect": lambda self_: rows})()


def test_resolve_catalog_prefers_configured(config_dir):
    cfg = load_config("dev", config_dir)
    assert uc.resolve_catalog(cfg, _FakeSQL({"demand_planning", "workspace"})).catalog == "demand_planning"


def test_resolve_catalog_uses_fallback(config_dir):
    cfg = load_config("dev", config_dir)
    assert uc.resolve_catalog(cfg, _FakeSQL({"workspace"})).catalog == "workspace"


def test_resolve_catalog_raises_when_nothing_exists(config_dir):
    with pytest.raises(RuntimeError):
        uc.resolve_catalog(load_config("prod", config_dir), _FakeSQL(set()))


def test_schema_change_log_in_core_tables():
    assert "monitoring.schema_change_log" in uc.CORE_TABLE_DDL
