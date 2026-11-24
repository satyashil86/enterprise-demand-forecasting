# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Project setup — Unity Catalog foundation
# MAGIC
# MAGIC **Business requirement:** one governed home for every dataset, model output and audit trail of the
# MAGIC demand forecasting platform, so planners, engineers and auditors work from the same source of truth.
# MAGIC
# MAGIC **What this notebook creates (idempotent — safe to re-run):**
# MAGIC
# MAGIC | Object | Name |
# MAGIC |---|---|
# MAGIC | Catalog | `demand_planning` (per env; falls back to `workspace` if CREATE CATALOG is not allowed) |
# MAGIC | Schemas | raw, bronze, silver, gold, features, ml, forecast, monitoring, audit |
# MAGIC | Volumes | raw.source_files, raw.checkpoints, ml.model_artifacts, audit.config |
# MAGIC | Tables | audit.pipeline_run_log, audit.ingestion_log, monitoring.data_quality_results |
# MAGIC
# MAGIC Compute: serverless (Databricks Free Edition compatible). No paid services are used.

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "test", "prod"], "Environment")
ENV = dbutils.widgets.get("env")

# COMMAND ----------

import os, sys, shutil

# Notebook CWD is src/ in a Git folder or bundle deployment -> dfp package is importable.
sys.path.insert(0, os.getcwd())

from dfp.config import load_config, find_config_dir, CONFIG_FILES
from dfp import uc
from dfp.audit import run_logged

cfg = load_config(ENV)
print(f"env={cfg.env}  requested catalog={cfg.catalog}")

# COMMAND ----------

# MAGIC %md ## 1. Catalog (with controlled fallback)

# COMMAND ----------

cfg, msg = uc.ensure_catalog(cfg, spark.sql)
print(msg)
CATALOG = cfg.catalog

# COMMAND ----------

# MAGIC %md ## 2. Schemas, volumes and core audit tables

# COMMAND ----------

n = uc.run_all(uc.setup_statements(cfg), spark.sql)
print(f"{n} DDL statements applied to catalog `{CATALOG}`")

# COMMAND ----------

# MAGIC %md ## 3. Snapshot configuration into the `audit.config` volume
# MAGIC Every run can later be traced to the exact config it used.

# COMMAND ----------

from datetime import datetime, timezone

cfg_dir = find_config_dir()
stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
target = cfg.volume_path("audit", "config", stamp)
os.makedirs(target, exist_ok=True)
for name in CONFIG_FILES:
    shutil.copy(cfg_dir / name, f"{target}/{name}")
print("Config snapshot:", target)

# COMMAND ----------

# MAGIC %md ## 4. Validation (acceptance checks for Phase 0)

# COMMAND ----------

expected_schemas = set(cfg.schemas)
found_schemas = {r["databaseName"] for r in spark.sql(f"SHOW SCHEMAS IN `{CATALOG}`").collect()}
missing_schemas = expected_schemas - found_schemas

expected_volumes = set(cfg.volumes)
found_volumes = set()
for s in {v.split(".")[0] for v in expected_volumes}:
    for r in spark.sql(f"SHOW VOLUMES IN `{CATALOG}`.`{s}`").collect():
        found_volumes.add(f"{s}.{r['volume_name']}")
missing_volumes = expected_volumes - found_volumes

missing_tables = [t for t in uc.CORE_TABLE_DDL if not spark.catalog.tableExists(cfg.table(t).replace("`", ""))]

# Volume write/read round-trip
probe = cfg.volume_path("raw", "source_files", "_probe.txt")
with open(probe, "w") as fh:
    fh.write("ok")
volume_rw_ok = open(probe).read() == "ok"
os.remove(probe)

results = {
    "missing_schemas": sorted(missing_schemas),
    "missing_volumes": sorted(missing_volumes),
    "missing_tables": missing_tables,
    "volume_read_write": volume_rw_ok,
}
display(spark.createDataFrame([(k, str(v)) for k, v in results.items()], "check string, result string"))
assert not missing_schemas and not missing_volumes and not missing_tables and volume_rw_ok, results

with run_logged(spark, cfg, "00_setup") as ctx:
    ctx["details"] = {"catalog": CATALOG, "fallback_message": msg, "config_snapshot": target, **results}
    ctx["rows_processed"] = 0
print("PHASE 0 ACCEPTANCE: PASSED")

# COMMAND ----------

dbutils.jobs.taskValues.set(key="catalog", value=CATALOG)
dbutils.notebook.exit(CATALOG)
