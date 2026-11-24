# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Raw ingestion → Bronze (Auto Loader + COPY INTO)  — ⚠️ SYNTHETIC DATA
# MAGIC
# MAGIC **Business requirement:** source files from SAP, POS, pricing, promotions, weather and inventory arrive
# MAGIC repeatedly, sometimes late, sometimes re-sent, sometimes with new columns. Every file must be loaded
# MAGIC exactly once, with full lineage, without losing or duplicating data.
# MAGIC
# MAGIC **Design (metadata-driven from `config/source_config.yml`):**
# MAGIC
# MAGIC | Sources | Method | Why |
# MAGIC |---|---|---|
# MAGIC | sales, pos, orders, promotions, pricing, weather, inventory | **Auto Loader** (`cloudFiles`, `trigger(availableNow=True)`) | Incremental file discovery, checkpointed exactly-once file processing, schema inference + evolution, `_rescued_data` |
# MAGIC | products, stores, regions, suppliers, calendar | **COPY INTO** | Small master files; simple idempotent batch load (already-loaded files are skipped) |
# MAGIC
# MAGIC Bronze stores records **as delivered** (CSV/JSON values as strings, duplicates and bad values kept) plus
# MAGIC lineage columns `_source_file, _source_file_modified_at, _ingested_at, _ingestion_run_id, _batch_id, _source_system`.
# MAGIC Record-level deduplication, typing, late-record handling (watermarks/merge) and validation happen in Silver (Phase 4).
# MAGIC
# MAGIC **Modes**
# MAGIC * `demo` (dev/test only) — resets Bronze, then proves: batch 1 load → re-run with no new files → re-drop of the
# MAGIC   same files → batch 2 with new columns, late, re-sent and restated records. Ends with acceptance checks.
# MAGIC * `incremental` — releases the next staged batch (simulating a new delivery) and ingests it. Used by the job.
# MAGIC * `ingest_only` — ingests whatever is new in landing.
# MAGIC
# MAGIC Serverless compute only supports `availableNow` triggers, so *scheduled micro-batches* = this notebook in a
# MAGIC Lakeflow Job on a schedule; each run picks up only files that arrived since the last run.

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "test", "prod"], "Environment")
dbutils.widgets.dropdown("mode", "demo", ["demo", "incremental", "ingest_only"], "Mode")
ENV = dbutils.widgets.get("env")
MODE = dbutils.widgets.get("mode")

# COMMAND ----------

import os, sys, json
sys.path.insert(0, os.getcwd())

import pandas as pd
from pyspark.sql import functions as F

from dfp.config import load_config
from dfp import uc, ingestion as ing
from dfp.audit import run_logged, new_run_id

cfg = uc.resolve_catalog(load_config(ENV), spark.sql)
uc.run_all(uc.core_table_ddl(cfg), spark.sql, log=None)   # ensures monitoring.schema_change_log exists
SOURCES = list(cfg.sources)
print(f"catalog={cfg.catalog} mode={MODE}")
print("staging:", cfg.staging_root)
print("landing:", cfg.landing_root)
print("staged batches:", ing.list_batches(cfg.staging_root), "| released:", ing.released_batches(cfg.landing_root))
assert ing.list_batches(cfg.staging_root), "No staged batches - run 01_synthetic_data first."

# COMMAND ----------

def bronze_counts():
    out = {}
    for s in SOURCES:
        t = cfg.table(cfg.sources[s]["bronze_table"])
        out[s] = spark.table(t).count() if spark.catalog.tableExists(t.replace("`", "")) else 0
    return out

def summary(results):
    return pd.DataFrame([{k: (", ".join(v) if isinstance(v, list) else v) for k, v in r.items() if k != "columns"}
                         for r in results])

def run_step(step_name):
    """Ingest everything new in landing, log it, and return per-source results."""
    rid = new_run_id("ingest")
    results = ing.ingest_all(spark, cfg, rid)
    ing.write_ingestion_log(spark, cfg, rid, results, step_name)
    n_changes = ing.write_schema_changes(spark, cfg, rid, results)
    print(f"[{step_name}] run_id={rid} rows={sum(r['rows_ingested'] for r in results):,} "
          f"files={sum(r['files_processed'] for r in results)} schema-change rows logged={n_changes}")
    display(summary(results))
    return rid, results

# COMMAND ----------

# MAGIC %md ## Incremental / ingest-only modes (used by the scheduled job)

# COMMAND ----------

if MODE in ("incremental", "ingest_only"):
    with run_logged(spark, cfg, f"02_raw_ingestion_{MODE}") as ctx:
        released = None
        if MODE == "incremental":
            released = ing.next_unreleased_batch(cfg.staging_root, cfg.landing_root)
            if released:
                n = ing.release_batch(cfg.staging_root, cfg.landing_root, released)
                print(f"Released {released}: {n} files")
            else:
                print("No new staged batch to release - ingesting anything new in landing.")
        rid, res = run_step(MODE)
        ctx["rows_processed"] = sum(r["rows_ingested"] for r in res)
        ctx["details"] = {"released_batch": released, "ingest_run_id": rid}
    dbutils.notebook.exit(json.dumps({"released": released, "rows": ctx["rows_processed"]}))

# COMMAND ----------

# MAGIC %md ## DEMO · Step 0 — reset (dev/test only)

# COMMAND ----------

assert cfg.env != "prod", "demo mode resets Bronze and is not allowed in prod"
ing.reset_bronze(spark, cfg)
print("Bronze tables, landing files and Bronze checkpoints removed.")
demo = {}

# COMMAND ----------

# MAGIC %md ## DEMO · Step 1 — first delivery (batch_001) and full initial load

# COMMAND ----------

n_files = ing.release_batch(cfg.staging_root, cfg.landing_root, "batch_001")
print(f"Released batch_001: {n_files} files")
demo["s1_run"], demo["s1"] = run_step("s1_batch_001")
demo["after_s1"] = bronze_counts()

# COMMAND ----------

# MAGIC %md ## DEMO · Step 2 — re-run with no new files (must ingest 0 rows)

# COMMAND ----------

demo["s2_run"], demo["s2"] = run_step("s2_rerun_no_new_files")
demo["after_s2"] = bronze_counts()

# COMMAND ----------

# MAGIC %md ## DEMO · Step 3 — the same batch_001 files are delivered again (must ingest 0 rows)
# MAGIC The files are rewritten at the same paths. Auto Loader's checkpoint and COPY INTO's load history both
# MAGIC recognise them as already processed.

# COMMAND ----------

ing.release_batch(cfg.staging_root, cfg.landing_root, "batch_001")
demo["s3_run"], demo["s3"] = run_step("s3_redrop_batch_001")
demo["after_s3"] = bronze_counts()

# COMMAND ----------

# MAGIC %md ## DEMO · Step 4 — second delivery (batch_002)
# MAGIC Contains: new columns (`pos.loyalty_member_flag`, `pos.basket_count`, `orders.order_source`,
# MAGIC `products.sustainability_tag`), late-arriving POS/orders rows, re-sent and restated POS rows,
# MAGIC and changed master data. Auto Loader stops once on the new columns, records them in its schema
# MAGIC location, and the restart continues with the evolved schema (`schema_restarts` column).

# COMMAND ----------

n_files = ing.release_batch(cfg.staging_root, cfg.landing_root, "batch_002")
print(f"Released batch_002: {n_files} files")
demo["s4_run"], demo["s4"] = run_step("s4_batch_002")
demo["after_s4"] = bronze_counts()

display(pd.DataFrame({k: demo[k] for k in ("after_s1", "after_s2", "after_s3", "after_s4")}).rename_axis("source").reset_index())

# COMMAND ----------

# MAGIC %md ## Sample Bronze output

# COMMAND ----------

pos = spark.table(cfg.table("bronze.pos_raw"))
display(pos.where("_batch_id = 'batch_002'").select(
    "transaction_id", "transaction_date", "sku_id", "quantity", "loyalty_member_flag", "basket_count",
    "_rescued_data", "_source_file", "_batch_id", "_ingested_at").limit(10))
display(spark.table(cfg.table("bronze.products_raw")).where("_batch_id = 'batch_002'").limit(5))
display(spark.sql(f"SELECT * FROM {cfg.table('monitoring.schema_change_log')} WHERE run_id = '{demo['s4_run']}'"))

# COMMAND ----------

# MAGIC %md ## Acceptance checks (Phase 2)

# COMMAND ----------

checks = []
def check(name, ok, observed, rule):
    checks.append((name, bool(ok), str(observed)[:500], rule))

# 1. initial load reconciles with landing files (rows and files) for every source
for r in demo["s1"]:
    s = r["source"]
    rows, files = ing.landing_counts(spark, cfg, s)  # landing currently holds batch_001 + batch_002
    check(f"reconcile_rows_{s}", demo["after_s4"][s] == rows, f"bronze={demo['after_s4'][s]} landing={rows}",
          "Bronze rows == rows in landing files (after both batches)")
    bfiles = spark.table(r["table"]).select("_source_file").distinct().count()
    check(f"reconcile_files_{s}", bfiles == files, f"bronze={bfiles} landing={files}", "every landing file ingested once")

# 2. idempotency
check("rerun_ingests_zero_rows", sum(r["rows_ingested"] for r in demo["s2"]) == 0,
      sum(r["rows_ingested"] for r in demo["s2"]), "re-run without new files adds 0 rows")
check("rerun_counts_unchanged", demo["after_s2"] == demo["after_s1"], "", "table counts unchanged")
check("redrop_ingests_zero_rows", sum(r["rows_ingested"] for r in demo["s3"]) == 0,
      sum(r["rows_ingested"] for r in demo["s3"]), "re-delivered files add 0 rows")
check("redrop_counts_unchanged", demo["after_s3"] == demo["after_s1"], "", "table counts unchanged")
check("batch2_added_rows", all(demo["after_s4"][s] >= demo["after_s1"][s] for s in SOURCES)
      and sum(demo["after_s4"].values()) > sum(demo["after_s1"].values()), "", "batch_002 appended")

# 3. schema evolution
cols = {r["source"]: r["columns"] for r in demo["s4"]}
for s, c in (("pos", "loyalty_member_flag"), ("pos", "basket_count"), ("orders", "order_source"),
             ("products", "sustainability_tag")):
    check(f"schema_evolution_{s}_{c}", c in cols[s], c, "new column added to Bronze table")
b1_new_vals = pos.where("_batch_id = 'batch_001' AND loyalty_member_flag IS NOT NULL").count()
check("batch1_rows_null_in_new_columns", b1_new_vals == 0, b1_new_vals, "older rows have NULL in new columns")
for r in demo["s1"]:
    if r["method"] == "autoloader":
        check(f"rescued_data_column_{r['source']}", "_rescued_data" in cols[r["source"]], "", "_rescued_data present")

# 4. late-arriving and duplicate records are preserved in Bronze (handled in Silver)
b1_max = pos.where("_batch_id = 'batch_001'").agg(F.max(F.to_date("transaction_date"))).first()[0]
late = pos.where((F.col("_batch_id") == "batch_002") & (F.to_date("transaction_date") <= F.lit(b1_max))).count()
check("late_or_resent_rows_landed_in_batch2", late > 0, late, "batch_002 rows dated inside the batch_001 window")
dup_keys = pos.groupBy("transaction_id").count().where("count > 1").count()
check("record_duplicates_preserved_in_bronze", dup_keys > 0, dup_keys, "Bronze keeps raw duplicates; Silver removes them")

# 5. lineage metadata on every row
for r in demo["s4"]:
    t = spark.table(r["table"])
    nulls = t.where(" OR ".join(f"{c} IS NULL" for c in ["_source_file", "_ingested_at", "_ingestion_run_id",
                                                          "_source_system"]) + " OR _batch_id = ''").count()
    check(f"lineage_complete_{r['source']}", nulls == 0, nulls, "no missing lineage metadata")

res = pd.DataFrame(checks, columns=["check", "passed", "observed", "rule"])
display(res)

# COMMAND ----------

from datetime import datetime, timezone
with run_logged(spark, cfg, "02_raw_ingestion_demo") as ctx:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    spark.createDataFrame(pd.DataFrame({
        "run_id": ctx["run_id"], "layer": "bronze", "table_name": "bronze.*", "rule_name": res["check"],
        "rule_type": "acceptance", "severity": "critical", "failed_records": (~res["passed"]).astype("int64"),
        "total_records": 1, "pass_flag": res["passed"], "checked_at": now, "details": res["observed"]})
    ).write.mode("append").saveAsTable(cfg.table("monitoring.data_quality_results"))
    ctx["rows_processed"] = int(sum(demo["after_s4"].values()))
    ctx["details"] = {"bronze_counts": demo["after_s4"], "checks": len(res), "failed": int((~res["passed"]).sum())}
    failed = res[~res["passed"]]
    assert failed.empty, f"Phase 2 acceptance failed:\n{failed}"
print(f"PHASE 2 ACCEPTANCE: PASSED ({len(res)} checks)")
