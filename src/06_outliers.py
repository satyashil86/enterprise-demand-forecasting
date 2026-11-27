# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Outlier detection and treatment  ⚠️ SYNTHETIC DATA
# MAGIC
# MAGIC A sudden spike in weekly demand can be:
# MAGIC * **genuine** (a promotion, a festival, a heatwave, or shoppers really bought more) → **keep it**, the model must learn it
# MAGIC * **a data error** (someone typed 1000 instead of 100) → **fix it**, otherwise the model learns nonsense
# MAGIC
# MAGIC Steps
# MAGIC 1. **Detect** unusual weeks per series with three simple statistics (robust z-score, IQR, rolling median)
# MAGIC 2. **Explain** each spike with business data: promotion? festival? extreme weather? did shop sales (POS) also jump?
# MAGIC 3. **Treat** only unexplained spikes: replace with the rolling median. Keep the original value next to the cleaned one.
# MAGIC 4. Write `gold.weekly_demand_clean` and an audit table `monitoring.outlier_audit`
# MAGIC
# MAGIC We never delete rows and never touch genuine demand.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.getcwd())
import pandas as pd
from pyspark.sql import functions as F, Window
from dfp.config import load_config
from dfp.uc import resolve_catalog

cfg = resolve_catalog(load_config("dev"), spark.sql)
CATALOG = cfg.catalog
TRUTH = cfg.ground_truth_root            # synthetic answer key - used only in the validation cell at the end
def T(name):
    return f"{CATALOG}.{name}"

KEYS = ["sku_id", "region_id", "channel"]
df = spark.table(T("gold.weekly_demand_features"))

# COMMAND ----------

# MAGIC %md ## 1. Detect — three ways of saying "this week is unusual for this series"
# MAGIC | Method | Idea | Flag when |
# MAGIC |---|---|---|
# MAGIC | **Robust z-score (MAD)** | distance from the series median, measured in "typical deviations" (not fooled by the outliers themselves like a normal z-score) | robust_z > 4 |
# MAGIC | **IQR** | above the upper fence Q3 + 3×(Q3−Q1) | demand > upper fence |
# MAGIC | **Rolling median** | compare with the median of the surrounding ±4 weeks that had orders (handles trend, season, fortnightly ordering) | demand > 3 × rolling median |
# MAGIC
# MAGIC A week is a **spike** when all three agree (fewer false alarms). We are cleaning *history*, so looking at weeks on
# MAGIC both sides is fine here (this is not a forecast feature).

# COMMAND ----------

# series-level statistics
stats = (df.groupBy(*KEYS)
           .agg(F.percentile_approx("demand", 0.5).alias("series_median"),
                F.percentile_approx("demand", 0.25).alias("q1"),
                F.percentile_approx("demand", 0.75).alias("q3")))
mad = (df.join(stats, KEYS)
         .groupBy(*KEYS)
         .agg(F.percentile_approx(F.abs(F.col("demand") - F.col("series_median")), 0.5).alias("mad")))

around = Window.partitionBy(*KEYS).orderBy("week_start").rowsBetween(-4, 4)

scored = (df.join(stats, KEYS).join(mad, KEYS)
            # median of the surrounding weeks that HAD orders: retailers that order every 2nd week
            # (zero weeks in between) must not make their normal orders look like spikes
            .withColumn("rolling_median", F.coalesce(
                F.percentile_approx(F.when(F.col("demand") > 0, F.col("demand")), 0.5).over(around), F.lit(0.0)))
            .withColumn("robust_z", 0.6745 * (F.col("demand") - F.col("series_median")) / F.greatest(F.col("mad"), F.lit(1)))   # MAD can be 0 for sparse series
            .withColumn("iqr_upper_fence", F.col("q3") + 3 * (F.col("q3") - F.col("q1")))
            .withColumn("is_spike", F.coalesce((F.col("robust_z") > 4)
                                    & (F.col("demand") > F.col("iqr_upper_fence"))
                                    & (F.col("demand") > 3 * F.greatest(F.col("rolling_median"), F.lit(1))), F.lit(False)))
            # drops are only flagged (zero weeks can be genuine, e.g. stock-outs or delisting)
            .withColumn("is_drop", F.coalesce((F.col("rolling_median") >= 20) & (F.col("demand") < 0.2 * F.col("rolling_median")), F.lit(False))))

# COMMAND ----------

# MAGIC %md ## 2. Explain — is there a business reason for the spike?
# MAGIC **Key test: do shop sales (POS) support the order?** Orders are what retailers buy from us; POS is what shoppers buy.
# MAGIC A retailer may order 2 weeks of stock at once, so we compare the order with **all POS in the surrounding 3 weeks**.
# MAGIC * order > 2.5 × POS of the 3 weeks around it → shoppers did not buy that much → **suspected data error** (fix it)
# MAGIC * otherwise the spike is genuine; we record why: **promotion**, **festival/holiday**, **extreme weather**,
# MAGIC   or simply **confirmed by POS**

# COMMAND ----------

three_weeks = Window.partitionBy(*KEYS).orderBy("week_start").rowsBetween(-1, 1)
scored = scored.withColumn("pos_3_weeks", F.sum("pos_units").over(three_weeks))
pos_supports_order = F.col("demand") <= 2.5 * F.greatest(F.col("pos_3_weeks"), F.lit(1))

reason = (F.when(~F.col("is_spike"), F.lit(None))
           .when(~pos_supports_order, "suspected_data_error")
           .when(F.col("promo_flag") == 1, "promotion")
           .when((F.col("festival_days") > 0) | (F.col("holiday_days") > 0), "festival_or_holiday")
           .when(F.col("extreme_weather_days") > 0, "extreme_weather")
           .otherwise("confirmed_by_pos"))

clean = (scored.withColumn("outlier_reason", reason)
               .withColumn("outlier_flag", F.col("is_spike"))
               .withColumn("treatment_method", F.when(F.col("outlier_reason") == "suspected_data_error", "replaced_with_rolling_median")
                                                .when(F.col("is_spike"), "kept_genuine_demand"))
               .withColumn("actual_demand_raw", F.col("demand"))
               .withColumn("actual_demand_clean", F.when(F.col("outlier_reason") == "suspected_data_error", F.col("rolling_median"))
                                                   .otherwise(F.col("demand")))
               .withColumn("treatment_timestamp", F.when(F.col("is_spike"), F.current_timestamp())))

# COMMAND ----------

# MAGIC %md ## 3. Price anomalies (business rule)
# MAGIC A shelf price more than 2× the regular price is impossible in a normal week → typing error. Use the regular price instead.

# COMMAND ----------

clean = (clean.withColumn("price_anomaly", F.coalesce(F.col("current_price") > 2 * F.col("regular_price"), F.lit(False)))
              .withColumn("current_price", F.when(F.col("price_anomaly"), F.col("regular_price")).otherwise(F.col("current_price"))))

# COMMAND ----------

# MAGIC %md ## 4. Save the cleaned table and the audit trail

# COMMAND ----------

helper_cols = ["demand", "series_median", "q1", "q3", "mad", "iqr_upper_fence", "pos_3_weeks"]
clean.drop(*helper_cols).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("gold.weekly_demand_clean"))

audit = (spark.table(T("gold.weekly_demand_clean"))
         .where("outlier_flag OR is_drop OR price_anomaly")
         .select(*KEYS, "week_start", "category", "actual_demand_raw", "actual_demand_clean", "rolling_median", "robust_z",
                 "outlier_flag", "is_drop", "price_anomaly", "outlier_reason", "treatment_method", "treatment_timestamp"))
audit.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("monitoring.outlier_audit"))

a = spark.table(T("monitoring.outlier_audit"))
display(a.groupBy("outlier_reason", "treatment_method").count().orderBy(F.desc("count")))
display(a.where("outlier_reason = 'suspected_data_error'").orderBy(F.desc("actual_demand_raw")).limit(10))

# COMMAND ----------

# MAGIC %md ## 5. Example — one series before and after cleaning

# COMMAND ----------

import matplotlib.pyplot as plt
ex = a.where("outlier_reason = 'suspected_data_error'").select(*KEYS).first()
if ex:
    s = (spark.table(T("gold.weekly_demand_clean"))
         .where((F.col("sku_id") == ex.sku_id) & (F.col("region_id") == ex.region_id) & (F.col("channel") == ex.channel))
         .orderBy("week_start").select("week_start", "actual_demand_raw", "actual_demand_clean").toPandas())
    plt.figure(figsize=(12, 4))
    plt.plot(s["week_start"], s["actual_demand_raw"], label="raw", color="#E45756")
    plt.plot(s["week_start"], s["actual_demand_clean"], label="clean", color="#4C78A8")
    plt.title(f"{ex.sku_id} · {ex.region_id} · {ex.channel}"); plt.legend(); plt.show()

# COMMAND ----------

# MAGIC %md ## 6. Checks
# MAGIC The synthetic answer key tells us which weeks contain injected typing errors (×10) and which spikes were genuine,
# MAGIC so we can measure whether the method makes the right call. (Answer key = validation only.)

# COMMAND ----------

c = spark.table(T("gold.weekly_demand_clean"))
assert c.count() == df.count(), "rows were lost"
assert c.where("actual_demand_clean IS NULL OR actual_demand_clean < 0").count() == 0
assert c.where("outlier_reason <> 'suspected_data_error' AND actual_demand_clean <> actual_demand_raw").count() == 0, \
    "genuine demand must never be changed"

issues = pd.read_csv(os.path.join(TRUTH, "injected_issues.csv"))
keys = issues.loc[issues.issue_type == "data_entry_spike_x10", "record_key"].str.split("|", expand=True)
injected = spark.createDataFrame(keys.rename(columns={0: "order_id", 1: "order_item"}).astype(str))
error_weeks = (spark.table(T("silver.orders"))
               .join(injected.withColumn("order_item", F.col("order_item").cast("int")), ["order_id", "order_item"])
               .select(*KEYS, F.to_date(F.date_trunc("week", "order_date")).alias("week_start")).distinct())
caught = error_weeks.join(c.where("outlier_reason = 'suspected_data_error'"), KEYS + ["week_start"]).count()
n_err = error_weeks.count()

genuine = spark.createDataFrame(pd.read_csv(os.path.join(TRUTH, "genuine_demand_spikes.csv"))[["sku_id", "region_id", "week_start"]])
genuine = genuine.withColumn("week_start", F.to_date("week_start"))
g_flag = c.join(genuine, ["sku_id", "region_id", "week_start"]).where("outlier_flag")
wrongly_fixed = g_flag.where("outlier_reason = 'suspected_data_error'").count()

treated = c.where("outlier_reason = 'suspected_data_error'").count()
print(f"injected typing errors caught : {caught}/{n_err}")
print(f"weeks treated as errors       : {treated} (of which real errors: {caught})")
print(f"genuine spikes flagged        : {g_flag.count()} (wrongly 'fixed': {wrongly_fixed})")
assert caught >= 0.8 * n_err, "too few typing errors caught"
assert treated - caught <= max(3, 0.25 * treated), "too many false alarms"
assert wrongly_fixed <= max(2, 0.1 * g_flag.count()), "too many genuine spikes treated as errors"
print("PHASE 5 OUTLIERS: PASSED")
