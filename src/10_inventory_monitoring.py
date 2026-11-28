# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Inventory risk + platform monitoring  ⚠️ SYNTHETIC DATA
# MAGIC
# MAGIC **Part A – Inventory:** combine the 104-week forecast with today's stock to answer, per SKU and region:
# MAGIC how many days will current stock last? will we run out before a new order can arrive? do we hold too much?
# MAGIC how much should we order?
# MAGIC
# MAGIC **Part B – Monitoring:** one health table that says whether data, pipelines, models and the business are OK.
# MAGIC
# MAGIC Assumptions (clearly labelled, because a recommendation is only as good as its inputs):
# MAGIC * stock in transit arrives next week; open purchase orders arrive after the supplier lead time
# MAGIC * service level 95% → safety-stock factor z = 1.65; forecast uncertainty from the P50–P90 band
# MAGIC * "excess" = more than 120 days of supply

# COMMAND ----------

import os, sys
sys.path.insert(0, os.getcwd())
import matplotlib.pyplot as plt
from pyspark.sql import functions as F, Window
from dfp.config import load_config
from dfp.uc import resolve_catalog

CATALOG = resolve_catalog(load_config("dev"), spark.sql).catalog
def T(name):
    return f"{CATALOG}.{name}"
def exists(name):
    return spark.catalog.tableExists(T(name))

Z, EXCESS_DAYS, PROJECT_WEEKS = 1.65, 120, 26

# COMMAND ----------

# MAGIC %md ## A1. Weekly forecast per SKU × region (the warehouse serves all channels of a region)

# COMMAND ----------

fc = (spark.table(T("forecast.weekly_demand_forecast"))
      .groupBy("sku_id", "category", "region", "forecast_week", "forecast_horizon")
      .agg(F.sum("p50").alias("p50"), F.sum("p90").alias("p90"))
      .withColumnRenamed("region", "region_id"))

# today's stock = the latest weekly inventory snapshot
latest = Window.partitionBy("sku_id", "region_id").orderBy(F.desc("snapshot_date"))
stock = (spark.table(T("silver.inventory"))
         .withColumn("rn", F.row_number().over(latest)).where("rn = 1")
         .select("sku_id", "region_id", "snapshot_date", "on_hand_qty", "in_transit_qty", "open_po_qty",
                 "lead_time_days", "moq", "replenishment_frequency_days", "unit_cost"))

# COMMAND ----------

# MAGIC %md ## A2. Projected stock week by week
# MAGIC projected stock(week h) = on hand + in transit + open POs (once arrived) − total forecast up to week h

# COMMAND ----------

upto = Window.partitionBy("sku_id", "region_id").orderBy("forecast_horizon").rowsBetween(Window.unboundedPreceding, 0)
proj = (fc.where(f"forecast_horizon <= {PROJECT_WEEKS}").join(stock, ["sku_id", "region_id"])
        .withColumn("lead_time_weeks", F.ceil(F.col("lead_time_days") / 7))
        .withColumn("cum_forecast", F.sum("p50").over(upto))
        .withColumn("projected_stock", F.col("on_hand_qty") + F.col("in_transit_qty")
                    + F.when(F.col("forecast_horizon") >= F.col("lead_time_weeks"), F.col("open_po_qty")).otherwise(0)
                    - F.col("cum_forecast")))
proj.select("sku_id", "region_id", "category", "forecast_week", "forecast_horizon", "p50", "projected_stock") \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("forecast.inventory_projection"))

# COMMAND ----------

# MAGIC %md ## A3. Risk metrics and replenishment suggestion per SKU × region

# COMMAND ----------

summary = (proj.groupBy("sku_id", "region_id", "category", "on_hand_qty", "in_transit_qty", "open_po_qty",
                        "lead_time_weeks", "moq", "replenishment_frequency_days", "unit_cost")
           .agg(F.avg(F.when(F.col("forecast_horizon") <= 13, F.col("p50"))).alias("avg_weekly_forecast"),
                F.sum(F.when(F.col("forecast_horizon") <= F.col("lead_time_weeks"), F.col("p50"))).alias("lead_time_demand"),
                F.avg(F.when(F.col("forecast_horizon") <= 13, (F.col("p90") - F.col("p50")) / 1.2816)).alias("sigma_week"),
                F.min(F.when(F.col("projected_stock") < 0, F.col("forecast_horizon"))).alias("first_stockout_week")))

risk = (summary
        .withColumn("review_weeks", F.ceil(F.col("replenishment_frequency_days") / 7))
        .withColumn("safety_stock", Z * F.col("sigma_week") * F.sqrt("lead_time_weeks"))
        .withColumn("reorder_point", F.col("lead_time_demand") + F.col("safety_stock"))
        .withColumn("inventory_position", F.col("on_hand_qty") + F.col("in_transit_qty") + F.col("open_po_qty"))
        .withColumn("days_of_supply", F.try_divide(F.col("on_hand_qty") + F.col("in_transit_qty"), F.col("avg_weekly_forecast") / 7))
        .withColumn("stockout_risk",
                    F.when(F.col("first_stockout_week") <= F.col("lead_time_weeks"), "High")      # too late to fix with a new order
                     .when(F.col("inventory_position") < F.col("reorder_point"), "Medium")        # order now
                     .otherwise("Low"))
        .withColumn("excess_flag", F.col("days_of_supply") > EXCESS_DAYS)
        .withColumn("excess_units", F.greatest(F.lit(0.0), F.col("on_hand_qty") - F.col("avg_weekly_forecast") * EXCESS_DAYS / 7))
        .withColumn("excess_value", F.col("excess_units") * F.col("unit_cost"))
        .withColumn("order_up_to", F.col("avg_weekly_forecast") * (F.col("lead_time_weeks") + F.col("review_weeks")) + F.col("safety_stock"))
        .withColumn("suggested_order_qty",
                    F.ceil(F.greatest(F.lit(0.0), F.col("order_up_to") - F.col("inventory_position")) / F.col("moq")) * F.col("moq"))
        .withColumn("assumptions", F.lit("in-transit next week; open PO at lead time; 95% service; excess > 120 days")))
risk.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("forecast.inventory_risk"))

r = spark.table(T("forecast.inventory_risk"))
display(r.groupBy("stockout_risk").agg(F.count("*").alias("sku_regions"), F.sum(F.col("excess_flag").cast("int")).alias("with_excess")))
display(r.where("stockout_risk <> 'Low' OR excess_flag")
         .select("sku_id", "region_id", "category", "days_of_supply", "first_stockout_week", "stockout_risk",
                 "excess_units", "suggested_order_qty")
         .orderBy(F.desc(F.col("stockout_risk") == "High"), "days_of_supply").limit(15))

# COMMAND ----------

ex = r.orderBy("days_of_supply").first()
p = (spark.table(T("forecast.inventory_projection")).where((F.col("sku_id") == ex.sku_id) & (F.col("region_id") == ex.region_id))
     .orderBy("forecast_horizon").toPandas())
plt.figure(figsize=(12, 4)); plt.plot(p["forecast_week"], p["projected_stock"], marker="o"); plt.axhline(0, color="red")
plt.title(f"Projected stock – {ex.sku_id} {ex.region_id} (risk: {ex.stockout_risk})"); plt.show()

# COMMAND ----------

# MAGIC %md ## B. Monitoring — one table of health checks (status OK / WARN / ALERT)

# COMMAND ----------

checks = []
def add(group, name, value, status):
    checks.append((group, name, str(value), status))

# DATA: freshness and quarantine
for t in ["silver.orders", "silver.pos", "silver.inventory", "silver.pricing", "silver.weather"]:
    last = spark.table(T(t)).agg(F.max("_ingested_at")).first()[0]
    hours = spark.sql(f"SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(TIMESTAMP'{last}')) / 3600").first()[0]
    add("data", f"hours_since_last_load_{t}", round(hours, 1), "OK" if hours < 24 * 7 else "WARN")
if exists("silver.quarantine_records"):
    for row in spark.table(T("silver.quarantine_records")).groupBy("source").count().collect():
        total = spark.table(T(f"bronze.{row.source}_raw")).count() if exists(f"bronze.{row.source}_raw") else 0
        share = 100 * row["count"] / total if total else 0
        add("data", f"quarantine_pct_{row.source}", round(share, 2), "OK" if share < 2 else "WARN")
if exists("monitoring.schema_change_log"):
    add("data", "schema_changes_logged", spark.table(T("monitoring.schema_change_log")).count(), "OK")

# PIPELINES: latest status of every step
if exists("audit.pipeline_run_log"):
    w = Window.partitionBy("pipeline_step").orderBy(F.desc("started_at"))
    for row in spark.table(T("audit.pipeline_run_log")).withColumn("rn", F.row_number().over(w)).where("rn = 1").collect():
        add("pipeline", f"last_run_{row.pipeline_step}", f"{row.status} ({row.duration_seconds}s)",
            "OK" if row.status == "SUCCEEDED" else "ALERT")

# MODELS: accuracy drift (latest backtest origin vs earlier ones), coverage, bias
bt = spark.table(T("ml.backtest_results")).where("forecast IS NOT NULL")
last_origin = bt.agg(F.max("fold_origin")).first()[0]
def wape(df):
    r_ = df.agg(F.sum(F.abs(F.col("actual") - F.col("forecast"))).alias("e"), F.sum("actual").alias("a")).first()
    return 100 * r_.e / r_.a
for m in [x.selected_model for x in spark.table(T("ml.model_selection")).select("selected_model").distinct().collect()]:
    recent, before = wape(bt.where((F.col("model") == m) & (F.col("fold_origin") == last_origin))), \
                     wape(bt.where((F.col("model") == m) & (F.col("fold_origin") < last_origin)))
    add("model", f"wape_drift_{m}", f"{before:.1f}% -> {recent:.1f}%", "OK" if recent <= 1.2 * before else "WARN")
fcst = spark.table(T("forecast.weekly_demand_forecast"))
n_fc = fcst.select("sku_id", "region", "channel").distinct().count()
n_active = (spark.table(T("gold.weekly_demand_clean")).select("sku_id", "region_id", "channel").distinct()
            .join(spark.table(T("gold.dim_product")).where("lifecycle_status <> 'Discontinued'"), "sku_id").count())
add("model", "forecast_coverage_pct", round(100 * n_fc / n_active, 1), "OK" if n_fc == n_active else "ALERT")
add("model", "model_version", fcst.select("model_version").first()[0], "OK")
add("model", "fallback_series", fcst.where("selected_model LIKE '%fallback%'").select("sku_id", "region", "channel").distinct().count(), "OK")

# BUSINESS
high = r.where("stockout_risk = 'High'").count()
add("business", "sku_regions_high_stockout_risk", high, "OK" if high == 0 else "WARN")
add("business", "sku_regions_with_excess", r.where("excess_flag").count(), "OK")
if exists("monitoring.outlier_audit"):
    add("business", "data_errors_corrected", spark.table(T("monitoring.outlier_audit"))
        .where("outlier_reason = 'suspected_data_error'").count(), "OK")

health = (spark.createDataFrame(checks, "check_group string, check_name string, value string, status string")
          .withColumn("checked_at", F.current_timestamp()))
health.write.mode("append").saveAsTable(T("monitoring.health_checks"))       # append = history of every run
display(health.orderBy(F.desc(F.col("status") == "ALERT"), "check_group"))

# COMMAND ----------

# MAGIC %md ## Checks

# COMMAND ----------

assert r.count() > 0 and r.where("days_of_supply IS NULL AND avg_weekly_forecast > 0").count() == 0
assert r.where("suggested_order_qty < 0").count() == 0
assert set(x.stockout_risk for x in r.select("stockout_risk").distinct().collect()) <= {"High", "Medium", "Low"}
assert len(checks) >= 10
print(f"PHASE 8 INVENTORY + MONITORING: PASSED  |  {r.count()} SKU-regions, {len(checks)} health checks")
