"""Unity Catalog object management: catalog, schemas, volumes, core audit tables.

SQL generation is separated from execution so it can be unit-tested without Spark.
All DDL is idempotent (IF NOT EXISTS), so 00_setup can be re-run safely.
"""
from __future__ import annotations

from typing import Callable, List, Optional, Tuple

from .config import ProjectConfig

# Core audit / monitoring tables needed from Phase 1 onwards. Later phases add their own.
CORE_TABLE_DDL = {
    "audit.pipeline_run_log": """
        run_id STRING NOT NULL,
        env STRING,
        pipeline_step STRING,
        status STRING,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        duration_seconds DOUBLE,
        rows_processed BIGINT,
        details STRING,
        error_message STRING,
        executed_by STRING
    """,
    "audit.ingestion_log": """
        run_id STRING NOT NULL,
        source_name STRING,
        batch_id STRING,
        ingestion_method STRING,
        files_processed BIGINT,
        rows_ingested BIGINT,
        target_table STRING,
        ingested_at TIMESTAMP,
        details STRING
    """,
    "monitoring.data_quality_results": """
        run_id STRING NOT NULL,
        layer STRING,
        table_name STRING,
        rule_name STRING,
        rule_type STRING,
        severity STRING,
        failed_records BIGINT,
        total_records BIGINT,
        pass_flag BOOLEAN,
        checked_at TIMESTAMP,
        details STRING
    """,
    "monitoring.schema_change_log": """
        run_id STRING NOT NULL,
        table_name STRING,
        column_name STRING,
        change_type STRING,
        detected_at TIMESTAMP,
        details STRING
    """,
}


def catalog_ddl(catalog: str) -> List[str]:
    return [
        f"CREATE CATALOG IF NOT EXISTS `{catalog}` "
        f"COMMENT 'Enterprise demand forecasting platform (synthetic data)'"
    ]


def schema_ddl(cfg: ProjectConfig) -> List[str]:
    return [
        f"CREATE SCHEMA IF NOT EXISTS `{cfg.catalog}`.`{s}` COMMENT '{c.replace(chr(39), '')}'"
        for s, c in cfg.schemas.items()
    ]


def volume_ddl(cfg: ProjectConfig) -> List[str]:
    out = []
    for qualified, comment in cfg.volumes.items():
        schema, vol = qualified.split(".")
        out.append(
            f"CREATE VOLUME IF NOT EXISTS `{cfg.catalog}`.`{schema}`.`{vol}` "
            f"COMMENT '{comment.replace(chr(39), '')}'"
        )
    return out


def core_table_ddl(cfg: ProjectConfig) -> List[str]:
    return [
        f"CREATE TABLE IF NOT EXISTS {cfg.table(t)} ({cols.strip()}) USING DELTA "
        f"TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
        for t, cols in CORE_TABLE_DDL.items()
    ]


def ensure_catalog(cfg: ProjectConfig, run_sql: Callable[[str], object]) -> Tuple[ProjectConfig, str]:
    """Create the configured catalog, or fall back to an existing one if not permitted.

    Returns (possibly re-bound config, message). Free Edition normally allows
    CREATE CATALOG on its default storage; the fallback exists for locked-down workspaces.
    """
    try:
        for stmt in catalog_ddl(cfg.catalog):
            run_sql(stmt)
        return cfg, f"Catalog `{cfg.catalog}` ready."
    except Exception as exc:  # noqa: BLE001 - surface any permission/feature error
        if not cfg.catalog_fallback:
            raise
        msg = (f"CREATE CATALOG `{cfg.catalog}` failed ({type(exc).__name__}: {str(exc)[:200]}). "
               f"Falling back to existing catalog `{cfg.catalog_fallback}`.")
        return cfg.with_catalog(cfg.catalog_fallback), msg


def resolve_catalog(cfg: ProjectConfig, run_sql: Callable[[str], object]) -> ProjectConfig:
    """Pick the catalog 00_setup actually used: the configured one if it exists, else the fallback.

    Lets every notebook run interactively without depending on job task values.
    """
    def exists(name: str) -> bool:
        try:
            return len(run_sql(f"SHOW CATALOGS LIKE '{name}'").collect()) > 0
        except Exception:  # noqa: BLE001
            return False

    if exists(cfg.catalog):
        return cfg
    if cfg.catalog_fallback and exists(cfg.catalog_fallback):
        return cfg.with_catalog(cfg.catalog_fallback)
    raise RuntimeError(f"Neither `{cfg.catalog}` nor fallback `{cfg.catalog_fallback}` exists - run 00_setup first.")


def setup_statements(cfg: ProjectConfig) -> List[str]:
    """Everything after the catalog exists, in dependency order."""
    return schema_ddl(cfg) + volume_ddl(cfg) + core_table_ddl(cfg)


def run_all(statements: List[str], run_sql: Callable[[str], object],
            log: Optional[Callable[[str], None]] = print) -> int:
    for stmt in statements:
        run_sql(stmt)
        if log:
            log(f"OK  {stmt.split('(')[0][:110]}")
    return len(statements)
