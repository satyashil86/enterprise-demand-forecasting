# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Synthetic SAP-style source data  — ⚠️ SYNTHETIC DATA
# MAGIC
# MAGIC **Business requirement:** without ERP access we still need realistic, multi-year, multi-source data to
# MAGIC build and prove the platform. Every record produced here is **synthetic**; brands, products, stores and
# MAGIC customers are invented.
# MAGIC
# MAGIC **What it produces** (to `/Volumes/<catalog>/raw/source_files/staging/batch_00N/<source>/`):
# MAGIC
# MAGIC | Source | Format | Grain |
# MAGIC |---|---|---|
# MAGIC | sales (billing) | CSV, monthly files | delivery line |
# MAGIC | pos | CSV, monthly files | store × SKU × day |
# MAGIC | orders | JSON lines, monthly | order line (customer × week × SKU) |
# MAGIC | products, stores, regions, suppliers, calendar | CSV | master |
# MAGIC | promotions | JSON lines | promotion (history + approved/draft future plans) |
# MAGIC | pricing | CSV | change-based price condition (history + planned) |
# MAGIC | weather | JSON lines, monthly | region × day (actuals + 14-day forecast) |
# MAGIC | inventory | Parquet, monthly | DC snapshot: SKU × region × week |
# MAGIC
# MAGIC `batch_001` = history up to the cutoff; `batch_002` = last 8 weeks plus late-arriving, re-sent and restated
# MAGIC records, master-data changes (SCD1/SCD2) and **new columns** (schema evolution). Phase 2 releases these
# MAGIC batches into the landing zone one at a time to demonstrate incremental ingestion.
# MAGIC
# MAGIC The `_synthetic_ground_truth/` folder is the answer key (demand patterns, injected issues). Pipelines
# MAGIC and models never read it; only validation does.

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "test", "prod"], "Environment")
dbutils.widgets.dropdown("overwrite", "false", ["false", "true"], "Regenerate if data exists")
dbutils.widgets.text("scale", "", "Scale override (small|medium, blank = env default)")
dbutils.widgets.text("seed", "42", "Random seed")
ENV = dbutils.widgets.get("env")
OVERWRITE = dbutils.widgets.get("overwrite") == "true"

# COMMAND ----------

import os, sys, shutil
sys.path.insert(0, os.getcwd())

import pandas as pd
from dfp.config import load_config
from dfp.audit import run_logged
from dfp.synthetic import SyntheticConfig, generate_all, SYNTHETIC_NOTICE
from dfp.synthetic_validation import run_checks, checks_frame

from dfp.uc import resolve_catalog

# use the catalog 00_setup actually created (configured one, or the fallback)
cfg = resolve_catalog(load_config(ENV), spark.sql)

scale = dbutils.widgets.get("scale") or cfg.env_setting("synthetic_data_scale", "small")
hist = cfg.forecast["history"]
syn_cfg = SyntheticConfig.from_scale(
    scale,
    seed=int(dbutils.widgets.get("seed")),
    history_weeks=int(hist["synthetic_history_weeks"]),
    history_end_week_start=hist["history_end_week_start"],
    future_weeks=int(cfg.forecast["forecast_horizon_weeks"]),
)
STAGING, TRUTH = cfg.staging_root, cfg.ground_truth_root
print(SYNTHETIC_NOTICE)
print(f"catalog={cfg.catalog} scale={scale} history={syn_cfg.history_start}..{syn_cfg.history_end_week_start}")
print("staging:", STAGING)

# COMMAND ----------

# MAGIC %md ## 1. Generate (idempotent: skipped when data exists unless `overwrite=true`)

# COMMAND ----------

already = os.path.exists(os.path.join(TRUTH, "generation_manifest.json"))
with run_logged(spark, cfg, "01_synthetic_data") as ctx:
    if already and not OVERWRITE:
        print("Synthetic data already exists - skipping generation (set overwrite=true to regenerate).")
        manifest = pd.read_csv(os.path.join(TRUTH, "file_manifest.csv"))
        gen = bb = None
        ctx["details"] = {"skipped": True}
    else:
        for p in (STAGING, TRUTH):
            if os.path.exists(p):
                shutil.rmtree(p)
        gen, bb, manifest = generate_all(syn_cfg, STAGING, TRUTH)
        ctx["details"] = {"skipped": False, "scale": scale, "n_skus": gen.S, "files": len(manifest)}
    ctx["rows_processed"] = int(manifest["rows"].sum())

display(manifest.groupby(["batch_id", "source"]).agg(files=("path", "count"), rows=("rows", "sum")).reset_index())

# COMMAND ----------

# MAGIC %md ## 2. Sample outputs (read back from the volume with Spark)

# COMMAND ----------

def staged(batch, source, fmt):
    path = f"{STAGING}/{batch}/{source}"
    reader = spark.read.option("header", True) if fmt == "csv" else spark.read
    return getattr(reader, fmt)(path)

display(staged("batch_001", "products", "csv"))
display(staged("batch_001", "pos", "csv").limit(20))
display(staged("batch_002", "pos", "csv").select("transaction_id", "transaction_date", "sku_id", "quantity",
                                                 "loyalty_member_flag", "basket_count").limit(10))
display(staged("batch_001", "orders", "json").limit(20))
display(staged("batch_002", "promotions", "json").groupBy("promotion_status").count())
display(staged("batch_001", "inventory", "parquet").limit(10))

# COMMAND ----------

# MAGIC %md ## 3. Acceptance checks (Phase 1)
# MAGIC Checks cover coverage (5 categories, 9 patterns, 12 sources, 156 weeks), key integrity of the clean
# MAGIC data, presence of each injected defect, schema evolution, late-arrival semantics, future-known inputs
# MAGIC and statistical signatures of each demand pattern.

# COMMAND ----------

if gen is None:
    print("Generation skipped; re-run with overwrite=true to re-validate in-memory checks.")
else:
    res = checks_frame(run_checks(gen, bb))
    display(res)
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    dq = spark.createDataFrame(pd.DataFrame({
        "run_id": ctx["run_id"], "layer": "raw_synthetic", "table_name": "staging",
        "rule_name": res["check"], "rule_type": "acceptance", "severity": "critical",
        "failed_records": (~res["passed"]).astype("int64"), "total_records": 1,
        "pass_flag": res["passed"], "checked_at": now, "details": res["observed"].str.slice(0, 500)}))
    dq.write.mode("append").saveAsTable(cfg.table("monitoring.data_quality_results"))
    failed = res[~res["passed"]]
    assert failed.empty, f"Phase 1 acceptance failed:\n{failed}"
    print(f"PHASE 1 ACCEPTANCE: PASSED ({len(res)} checks)")
