# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver — cleansing, validation, quarantine, MERGE, SCD1/SCD2  — ⚠️ SYNTHETIC DATA
# MAGIC
# MAGIC **Business requirement:** planners and models need one trustworthy version of every record: typed, valid,
# MAGIC de-duplicated, with corrections applied and master-data history kept. Bad records must be explainable,
# MAGIC never silently lost.
# MAGIC
# MAGIC | Step | What happens | Where |
# MAGIC |---|---|---|
# MAGIC | Incremental read | Only Bronze rows newer than the source's high-watermark (`_ingested_at`) | `audit.silver_watermark` |
# MAGIC | Standardise | Cast to the Silver contract (ANSI-safe `try_cast`), multi-format dates → `DATE`, trim strings | `dfp/silver.py: SPECS` |
# MAGIC | Validate | Not-null keys, value ranges, allowed values, referential integrity vs product/store/region masters | rules per source |
# MAGIC | Quarantine | Failing rows + rule names + reason + raw record JSON + lineage | `silver.quarantine_records` |
# MAGIC | De-duplicate | One row per business key; **latest ingested wins** (restatements), exact re-sends collapse | window `row_number()` |
# MAGIC | MERGE | Upsert on business key, update only if newer *and* changed → idempotent | `silver.pos`, `silver.orders`, … |
# MAGIC | SCD Type 2 | Product master history (`effective_from/to`, `is_current`, surrogate `product_sk`) | `silver.products_scd2` |
# MAGIC | SCD Type 1 | Stores, regions, suppliers, calendar overwritten in place | `silver.stores`, … |
# MAGIC
# MAGIC Also covered here (Bronze extras): Bronze tables made **append-only**, Delta **history / time travel**,
# MAGIC **liquid clustering + OPTIMIZE** on Silver facts.
# MAGIC
# MAGIC Modes: `run` = incremental (first run = full load) · `rebuild` = drop Silver + watermarks, then full load (dev/test).

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "test", "prod"], "Environment")
dbutils.widgets.dropdown("mode", "run", ["run", "rebuild"], "Mode")
ENV = dbutils.widgets.get("env")
MODE = dbutils.widgets.get("mode")

# COMMAND ----------

import os, sys, json
sys.path.insert(0, os.getcwd())

import pandas as pd
from pyspark.sql import functions as F

from dfp.config import load_config
from dfp import uc, silver as sv
from dfp.audit import run_logged, new_run_id

cfg = uc.resolve_catalog(load_config(ENV), spark.sql)
T = cfg.table
print(f"catalog={cfg.catalog} mode={MODE}")

def stats_frame(stats):
    return pd.DataFrame(stats)[["source", "bronze_rows_read", "valid_rows", "quarantined_rows", "silver_rows_after", "skipped"]]

# COMMAND ----------

# MAGIC %md ## 1. Bronze hardening — append-only + documentation
# MAGIC `delta.appendOnly = true` makes Delta reject UPDATE/DELETE/MERGE on Bronze: it stays an immutable record of
# MAGIC what each source delivered (replay and audit), while Auto Loader / COPY INTO appends keep working.

# COMMAND ----------

hardened = sv.harden_bronze(spark, cfg)
print(f"{len(hardened)} Bronze tables set to append-only")

# COMMAND ----------

if MODE == "rebuild":
    sv.reset_silver(spark, cfg)
    print("Silver tables, quarantine and watermarks dropped - full reload follows.")

# COMMAND ----------

# MAGIC %md ## 2. Run 1 — Bronze → Silver (incremental; a full load when Silver is empty)

# COMMAND ----------

with run_logged(spark, cfg, "03_silver_run1") as ctx:
    RUN1 = ctx["run_id"]
    stats1 = sv.process_all(spark, cfg, RUN1)
    ctx["rows_processed"] = sum(s["bronze_rows_read"] for s in stats1)
    ctx["details"] = {"stats": stats1}
display(stats_frame(stats1))

# COMMAND ----------

# MAGIC %md ## 3. Run 2 — immediately again (must read 0 Bronze rows and change nothing)

# COMMAND ----------

counts_before = {s: spark.table(T(sv.SPECS[s].target)).count() for s in sv.PROCESS_ORDER}
q_before = spark.table(T(sv.QUARANTINE_TABLE)).count()
with run_logged(spark, cfg, "03_silver_run2_idempotency") as ctx:
    stats2 = sv.process_all(spark, cfg, ctx["run_id"])
    ctx["rows_processed"] = sum(s["bronze_rows_read"] for s in stats2)
display(stats_frame(stats2))
counts_after = {s: spark.table(T(sv.SPECS[s].target)).count() for s in sv.PROCESS_ORDER}
q_after = spark.table(T(sv.QUARANTINE_TABLE)).count()

# COMMAND ----------

# MAGIC %md ## 4. Look at the results

# COMMAND ----------

display(spark.sql(f"""SELECT source, rule_name, count(*) AS records, first(rejection_reason) AS example_reason
                      FROM {T(sv.QUARANTINE_TABLE)} GROUP BY source, rule_name ORDER BY source, records DESC"""))

# COMMAND ----------

display(spark.sql(f"""SELECT sku_id, product_name, lifecycle_status, effective_from, effective_to, is_current, product_sk
                      FROM {T('silver.products_scd2')}
                      WHERE sku_id IN (SELECT sku_id FROM {T('silver.products_scd2')} GROUP BY sku_id HAVING count(*) > 1)
                      ORDER BY sku_id, effective_from"""))

# COMMAND ----------

display(spark.table(T("silver.pos")).limit(10))
display(spark.table(T(sv.WATERMARK_TABLE)))

# COMMAND ----------

# MAGIC %md ## 5. Delta features — history, time travel, liquid clustering

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {T('silver.pos')}").select("version", "timestamp", "operation", "operationMetrics"))
first_version = spark.sql(f"DESCRIBE HISTORY {T('silver.pos')}").agg(F.min("version")).first()[0]
print("rows in silver.pos at first version:", spark.sql(f"SELECT count(*) FROM {T('silver.pos')} VERSION AS OF {first_version}").first()[0],
      "| now:", spark.table(T("silver.pos")).count())

for s in ("pos", "orders", "sales", "inventory"):
    spark.sql(f"OPTIMIZE {T(sv.SPECS[s].target)}")
display(spark.sql(f"DESCRIBE DETAIL {T('silver.pos')}").select("name", "clusteringColumns", "numFiles", "sizeInBytes"))

# COMMAND ----------

# MAGIC %md ## 6. Acceptance checks (Phase 3)
# MAGIC Silver results are compared with an independent batch recomputation over the whole of Bronze, and with the
# MAGIC synthetic answer key (`_synthetic_ground_truth`, read here for **validation only**).

# COMMAND ----------

checks = []
def check(name, ok, observed, rule):
    checks.append((name, bool(ok), str(observed)[:500], rule))

refs = sv.reference_frames(spark, cfg)
quar = spark.table(T(sv.QUARANTINE_TABLE))

# 6.1 every source processed, reconciliation, unique keys
for s in sv.PROCESS_ORDER:
    spec = sv.SPECS[s]
    st = next(x for x in stats1 if x["source"] == s)
    check(f"processed_{s}", st["skipped"] in (None, "no new bronze rows") and st["silver_rows_after"], st["skipped"], "source processed")
    bronze = spark.table(T(spec.bronze))
    checked = sv.validate(sv.standardise(bronze, spec), spec, refs)
    valid, invalid = sv.split_valid_invalid(checked)
    nb, nv, ni = bronze.count(), valid.count(), invalid.count()
    nq = quar.where(F.col("source") == s).count()
    distinct_valid = valid.select(*spec.pk).distinct().count()
    tgt = spark.table(T(spec.target))
    n_silver = tgt.where("is_current").count() if spec.kind == "scd2" else tgt.count()
    check(f"reconcile_{s}", nb == nv + ni and nq == ni and n_silver == distinct_valid,
          f"bronze={nb} valid={nv} invalid={ni} quarantine={nq} silver={n_silver} distinct_valid_keys={distinct_valid} "
          f"collapsed_duplicates={nv - distinct_valid}", "bronze = valid + quarantined; silver = distinct valid keys")
    key_df = tgt.where("is_current") if spec.kind == "scd2" else tgt
    dups = key_df.groupBy(*spec.pk).count().where("count > 1").count()
    check(f"pk_unique_{s}", dups == 0, dups, "one row per business key (current row for SCD2)")

# 6.2 idempotency
check("rerun_reads_zero_rows", sum(s["bronze_rows_read"] for s in stats2) == 0,
      sum(s["bronze_rows_read"] for s in stats2), "second run reads nothing new")
check("rerun_counts_unchanged", counts_before == counts_after and q_before == q_after, "", "Silver and quarantine unchanged")

# 6.3 answer-key checks (validation only)
iss = pd.read_csv(os.path.join(cfg.ground_truth_root, "injected_issues.csv"))
def ids(t):  # injected record keys of an issue type
    return set(iss.loc[iss.issue_type == t, "record_key"].astype(str))
qids = {}
for s in ("pos", "orders", "weather", "inventory"):
    rows = quar.where(F.col("source") == s).select("record_id").distinct().toPandas()["record_id"]
    qids[s] = {"|".join(r.split("|")[:2]) for r in rows} if s == "weather" else set(rows)
for s, t in (("pos", "invalid_sku"), ("pos", "null_store_id"), ("pos", "negative_quantity"), ("pos", "null_selling_price"),
             ("orders", "null_ordered_quantity"), ("weather", "sensor_error_99.9"), ("inventory", "negative_on_hand")):
    missing = ids(t) - qids[s]
    check(f"quarantined_{t}", len(ids(t)) > 0 and not missing, f"injected={len(ids(t))} missing={len(missing)}",
          "every injected defect is in quarantine")

alt = list(ids("alt_date_format_dd.MM.yyyy"))
sales_alt = spark.table(T("silver.sales")).where(F.col("sales_doc_id").isin(alt))
check("alt_date_formats_standardised", sales_alt.count() == len(alt) and sales_alt.where("posting_date IS NULL").count() == 0,
      f"{sales_alt.count()}/{len(alt)}", "dd.MM.yyyy dates parsed into Silver, none rejected")

restated = list(ids("restatement_latest_wins"))
pos_spec = sv.SPECS["pos"]
pos_valid, _ = sv.split_valid_invalid(sv.validate(sv.standardise(spark.table(T(pos_spec.bronze)), pos_spec), pos_spec, refs))
b2 = (pos_valid.where((F.col("_batch_id") == "batch_002") & F.col("transaction_id").isin(restated))
      .select("transaction_id", F.col("quantity").alias("q2")).dropDuplicates(["transaction_id"]))
wrong = spark.table(T("silver.pos")).join(b2, "transaction_id").where("quantity <> q2").count()
check("restatements_applied", b2.count() > 0 and wrong == 0, f"restated={b2.count()} wrong={wrong}", "latest correction wins")

pos_ids = spark.table(T("silver.pos")).select("transaction_id")
late = ids("late_arriving") - ids("invalid_sku") - ids("null_store_id") - ids("negative_quantity") - ids("null_selling_price")
late_pos = [i for i in late if i.startswith("POS-")]
found = pos_ids.where(F.col("transaction_id").isin(late_pos)).count()
check("late_arriving_rows_in_silver", found == len(late_pos), f"{found}/{len(late_pos)}", "late rows merged, not dropped")

# 6.4 SCD2
p = spark.table(T("silver.products_scd2"))
cur_per_sku = p.groupBy("sku_id").agg(F.sum(F.col("is_current").cast("int")).alias("c"))
check("scd2_one_current_per_sku", cur_per_sku.where("c <> 1").count() == 0, cur_per_sku.where("c <> 1").count(), "exactly one current row")
changed = ids("scd2_lifecycle_change") | ids("scd2_attribute_change_product_name")
versions = {r["sku_id"]: r["n"] for r in p.groupBy("sku_id").agg(F.count("*").alias("n")).collect()}
check("scd2_history_for_changed_skus", all(versions.get(k, 0) >= 2 for k in changed) and sum(v > 1 for v in versions.values()) == len(changed),
      f"changed={sorted(changed)} multi_version={sum(v > 1 for v in versions.values())}", "history kept only where attributes changed")
from pyspark.sql import Window
w = Window.partitionBy("sku_id").orderBy("effective_from")
overlap = p.withColumn("nxt", F.lead("effective_from").over(w)).where("nxt IS NOT NULL AND effective_to >= nxt").count()
check("scd2_no_overlapping_versions", overlap == 0, overlap, "effective_to < next effective_from")
open_cur = p.where("is_current AND effective_to <> DATE'9999-12-31'").count()
check("scd2_current_rows_open_ended", open_cur == 0, open_cur, "current rows end 9999-12-31")
new_sku = ids("new_sku_in_batch_002")
check("scd2_new_sku_single_version", all(versions.get(k) == 1 for k in new_sku), sorted(new_sku), "new SKU inserted once")

# 6.5 SCD1
sid = list(ids("scd1_attribute_change"))
b2s = (spark.table(T("bronze.stores_raw")).where((F.col("_batch_id") == "batch_002") & F.col("store_id").isin(sid))
       .select("store_id", F.expr("try_cast(size_index AS DOUBLE)").alias("b2")))
s1 = spark.table(T("silver.stores")).join(b2s, "store_id")
check("scd1_store_overwritten", s1.count() == len(sid) and s1.where("size_index <> b2").count() == 0,
      s1.select("store_id", "size_index", "b2").collect(), "store attributes overwritten with latest")

# 6.6 quarantine completeness, Bronze append-only
bad_q = quar.where("rule_name IS NULL OR rule_name = '' OR rejection_reason IS NULL OR record_json IS NULL").count()
check("quarantine_rows_explained", bad_q == 0, bad_q, "every quarantined row has rule, reason and raw record")
not_ao = [t for t in hardened if spark.sql(f"SHOW TBLPROPERTIES {t} ('delta.appendOnly')").first()["value"] != "true"]
check("bronze_append_only", len(hardened) == 12 and not not_ao, not_ao, "all 12 Bronze tables append-only")

res = pd.DataFrame(checks, columns=["check", "passed", "observed", "rule"])
display(res)

# COMMAND ----------

from datetime import datetime, timezone
with run_logged(spark, cfg, "03_silver_acceptance") as ctx:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    spark.createDataFrame(pd.DataFrame({
        "run_id": ctx["run_id"], "layer": "silver", "table_name": "silver.*", "rule_name": res["check"],
        "rule_type": "acceptance", "severity": "critical", "failed_records": (~res["passed"]).astype("int64"),
        "total_records": 1, "pass_flag": res["passed"], "checked_at": now, "details": res["observed"]})
    ).write.mode("append").saveAsTable(T("monitoring.data_quality_results"))
    ctx["details"] = {"checks": len(res), "failed": int((~res["passed"]).sum())}
    failed = res[~res["passed"]]
    assert failed.empty, f"Phase 3 acceptance failed:\n{failed}"
print(f"PHASE 3 ACCEPTANCE: PASSED ({len(res)} checks)")
