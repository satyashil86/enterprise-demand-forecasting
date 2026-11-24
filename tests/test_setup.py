"""Phase 0: configuration and Unity Catalog DDL generation."""
import pytest

from dfp import uc
from dfp.config import load_config

EXPECTED_SCHEMAS = {"raw", "bronze", "silver", "gold", "features", "ml", "forecast", "monitoring", "audit"}


def test_load_dev_config(config_dir):
    cfg = load_config("dev", config_dir)
    assert cfg.catalog == "demand_planning"
    assert set(cfg.schemas) == EXPECTED_SCHEMAS
    assert cfg.forecast["forecast_horizon_weeks"] == 104
    assert len(cfg.sources) == 12


def test_unknown_env_rejected(config_dir):
    with pytest.raises(ValueError):
        load_config("staging", config_dir)


def test_prod_has_no_fallback(config_dir):
    assert load_config("prod", config_dir).catalog_fallback is None


def test_paths_and_names(config_dir):
    cfg = load_config("dev", config_dir)
    assert cfg.table("bronze.sales_raw") == "`demand_planning`.`bronze`.`sales_raw`"
    assert cfg.landing_root == "/Volumes/demand_planning/raw/source_files/landing"
    assert cfg.checkpoint_root == "/Volumes/demand_planning/raw/checkpoints"


def test_setup_ddl_is_idempotent_and_complete(config_dir):
    cfg = load_config("dev", config_dir)
    stmts = uc.setup_statements(cfg)
    assert all("IF NOT EXISTS" in s for s in stmts)
    assert sum(s.startswith("CREATE SCHEMA") for s in stmts) == 9
    assert sum(s.startswith("CREATE VOLUME") for s in stmts) == 4
    assert sum(s.startswith("CREATE TABLE") for s in stmts) == len(uc.CORE_TABLE_DDL)
    # schemas must be created before volumes/tables that live in them
    first_vol = next(i for i, s in enumerate(stmts) if s.startswith("CREATE VOLUME"))
    assert all(s.startswith("CREATE SCHEMA") for s in stmts[:first_vol])


def test_catalog_fallback(config_dir):
    cfg = load_config("dev", config_dir)

    def deny(sql):
        raise PermissionError("CREATE CATALOG not permitted")

    new_cfg, msg = uc.ensure_catalog(cfg, deny)
    assert new_cfg.catalog == "workspace" and "Falling back" in msg
    assert new_cfg.table("gold.dim_product") == "`workspace`.`gold`.`dim_product`"


def test_prod_catalog_failure_raises(config_dir):
    cfg = load_config("prod", config_dir)
    with pytest.raises(PermissionError):
        uc.ensure_catalog(cfg, lambda s: (_ for _ in ()).throw(PermissionError("no")))
