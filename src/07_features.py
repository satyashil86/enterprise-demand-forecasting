# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Feature engineering (leakage-safe)  ⚠️ SYNTHETIC DATA
# MAGIC
# MAGIC Models learn from **features**: numbers that describe each week. The golden rule:
# MAGIC
# MAGIC > **A feature may only use information that would have been known when the forecast was made.**
# MAGIC
# MAGIC If a feature secretly contains the answer (e.g. this week's demand, or this week's shop sales), the model looks
# MAGIC brilliant in testing and fails in real life. That is called **data leakage**.
# MAGIC
# MAGIC | Group | Examples | Safe because |
# MAGIC |---|---|---|
# MAGIC | Demand lags | demand 1, 2, 3, 4, 8, 13, 26, 52, 104 weeks ago | strictly from the past (`F.lag`) |
# MAGIC | Rolling stats | average / std of the **previous** 4–52 weeks | window ends at the previous week (`rowsBetween(-n, -1)`) |
# MAGIC | Seasonality | week of year, month, holidays, last year's same week | calendar is known in advance |
# MAGIC | Promotion | promo this week, discount, promos in last 13 weeks, weeks since last promo | promotions are planned in advance |
# MAGIC | Price | price, price change, discount, our price vs competitor | prices are planned in advance |
# MAGIC | POS | shop sales **last** week and last 4 weeks | this week's POS is NOT known yet → only lags |
# MAGIC | Weather | temperature, normal temperature for the week, anomaly | from weather forecasts / normals in the future |
# MAGIC
# MAGIC Input: `gold.weekly_demand_clean` (cleaned demand from notebook 06) → output: `features.weekly_features`.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.getcwd())
from pyspark.sql import functions as F, Window
from dfp.config import load_config
from dfp.uc import resolve_catalog

CATALOG = resolve_catalog(load_config("dev"), spark.sql).catalog
def T(name):
    return f"{CATALOG}.{name}"

KEYS = ["sku_id", "region_id", "channel"]
df = spark.table(T("gold.weekly_demand_clean"))
y = F.col("actual_demand_clean")                            # the target we will forecast

w = Window.partitionBy(*KEYS).orderBy("week_start")         # one series, in time order
def past(n):                                                # the n weeks BEFORE the current week
    return w.rowsBetween(-n, -1)
all_past = w.rowsBetween(Window.unboundedPreceding, -1)     # every week before the current week

# COMMAND ----------

# MAGIC %md ## 1. Demand lags and rolling statistics

# COMMAND ----------

for k in [1, 2, 3, 4, 8, 13, 26, 52, 104]:
    df = df.withColumn(f"lag_{k}", F.lag(y, k).over(w))

for n in [4, 8, 13, 26, 52]:
    df = df.withColumn(f"roll_mean_{n}", F.avg(y).over(past(n)))

df = (df.withColumn("roll_std_13", F.stddev(y).over(past(13)))
        .withColumn("roll_growth_4_vs_13", F.try_divide("roll_mean_4", "roll_mean_13") - 1)   # recent momentum
        .withColumn("roll_volatility_13", F.try_divide("roll_std_13", "roll_mean_13"))        # how jumpy
        .withColumn("weeks_of_history", F.row_number().over(w) - 1))

# COMMAND ----------

# MAGIC %md ## 2. Seasonality

# COMMAND ----------

df = (df.withColumn("quarter", F.quarter("week_start"))
        # last year's same week compared with last year's average (1.2 = that week was 20% above normal)
        .withColumn("seasonal_index_52", F.try_divide("lag_52", F.lag("roll_mean_52", 52).over(w)))
        # are we selling more than at the same time last year?
        .withColumn("yoy_growth_13", F.try_divide("roll_mean_13", F.lag("roll_mean_13", 52).over(w)) - 1))

# COMMAND ----------

# MAGIC %md ## 3. Promotion features

# COMMAND ----------

last_promo_week = F.max(F.when(F.col("promo_flag") == 1, F.col("week_start"))).over(all_past)
df = (df.withColumn("promo_weeks_last_13", F.sum("promo_flag").over(past(13)))
        .withColumn("weeks_since_last_promo", F.datediff("week_start", last_promo_week) / 7)
        # how much this series usually sells in promo weeks vs normal weeks (from the past only)
        .withColumn("historical_promo_uplift",
                    F.try_divide(F.avg(F.when(F.col("promo_flag") == 1, y)).over(all_past),
                                 F.avg(F.when(F.col("promo_flag") == 0, y)).over(all_past))))

# COMMAND ----------

# MAGIC %md ## 4. Price features

# COMMAND ----------

df = (df.withColumn("price_change_pct", F.try_divide("regular_price", F.lag("regular_price", 1).over(w)) - 1)
        .withColumn("shelf_discount_pct", 1 - F.try_divide("current_price", "regular_price"))
        .withColumn("relative_price_index", F.try_divide("current_price", "competitor_price")))   # > 1 = we are dearer

# COMMAND ----------

# MAGIC %md ## 5. POS (only past weeks!) and weather

# COMMAND ----------

df = (df.withColumn("pos_lag_1", F.lag("pos_units", 1).over(w))
        .withColumn("pos_roll_mean_4", F.avg("pos_units").over(past(4)))
        .withColumn("stores_selling_lag_1", F.lag("stores_selling", 1).over(w))
        .withColumn("sales_velocity_lag_1", F.try_divide("pos_lag_1", "stores_selling_lag_1"))
        # "normal" temperature for this week = average of the same week 1 and 2 years ago
        .withColumn("temperature_normal", (F.lag("temperature_avg", 52).over(w) + F.lag("temperature_avg", 104).over(w)) / 2)
        .withColumn("temperature_anomaly", F.col("temperature_avg") - F.col("temperature_normal")))

# COMMAND ----------

# MAGIC %md ## 6. Choose the features and save

# COMMAND ----------

# (name, group, known for future weeks without forecasting it?, description)
CATALOG_ROWS = [
    *[(f"lag_{k}", "demand_lag", False, f"demand {k} weeks ago") for k in [1, 2, 3, 4, 8, 13, 26, 52, 104]],
    *[(f"roll_mean_{n}", "rolling", False, f"average demand of the previous {n} weeks") for n in [4, 8, 13, 26, 52]],
    ("roll_std_13", "rolling", False, "std of demand, previous 13 weeks"),
    ("roll_growth_4_vs_13", "rolling", False, "last 4 weeks vs last 13 weeks"),
    ("roll_volatility_13", "rolling", False, "std / mean, previous 13 weeks"),
    ("weeks_of_history", "rolling", True, "weeks since the series started"),
    ("week_of_year", "seasonality", True, "ISO week number"),
    ("month", "seasonality", True, "month"),
    ("quarter", "seasonality", True, "quarter"),
    ("holiday_days", "seasonality", True, "public holidays in the week"),
    ("festival_days", "seasonality", True, "festival days in the week"),
    ("seasonal_index_52", "seasonality", True, "same week last year vs last year's average"),
    ("yoy_growth_13", "seasonality", False, "last 13 weeks vs same period last year"),
    ("promo_flag", "promotion", True, "promotion planned this week"),
    ("discount_pct", "promotion", True, "planned discount %"),
    ("display_flag", "promotion", True, "in-store display planned"),
    ("promo_weeks_last_13", "promotion", True, "promotion weeks in the previous 13 weeks"),
    ("weeks_since_last_promo", "promotion", True, "weeks since the previous promotion"),
    ("historical_promo_uplift", "promotion", False, "past promo-week demand / normal-week demand"),
    ("regular_price", "price", True, "list price"),
    ("current_price", "price", True, "shelf price this week"),
    ("price_change_pct", "price", True, "list price change vs last week"),
    ("shelf_discount_pct", "price", True, "1 - shelf price / list price"),
    ("relative_price_index", "price", True, "our shelf price / competitor price"),
    ("pos_lag_1", "pos", False, "shop units sold last week"),
    ("pos_roll_mean_4", "pos", False, "average shop units, previous 4 weeks"),
    ("stores_selling_lag_1", "pos", False, "stores that sold the SKU last week"),
    ("sales_velocity_lag_1", "pos", False, "units per selling store last week"),
    ("temperature_avg", "weather", True, "weekly average temperature (forecast / normal in future)"),
    ("temperature_normal", "weather", True, "average of the same week 1 and 2 years ago"),
    ("temperature_anomaly", "weather", True, "temperature minus normal"),
    ("rainfall_total", "weather", True, "weekly rainfall"),
    ("extreme_weather_days", "weather", True, "heatwave / storm days"),
]
FEATURES = [r[0] for r in CATALOG_ROWS]
ID_COLS = KEYS + ["week_start", "category", "brand", "country_code", "lifecycle_status"]

features = df.select(*ID_COLS, "actual_demand_raw", "actual_demand_clean", "outlier_flag", *FEATURES)
features.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("features.weekly_features"))
spark.createDataFrame(CATALOG_ROWS, "feature string, feature_group string, known_in_future boolean, description string") \
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("features.feature_catalog"))

fw = spark.table(T("features.weekly_features"))
print(f"features.weekly_features: {fw.count():,} rows, {len(FEATURES)} features")
display(fw.where("lag_52 IS NOT NULL").orderBy(*KEYS, "week_start").limit(10))

# COMMAND ----------

# MAGIC %md ## 7. Leakage checks

# COMMAND ----------

# 1) no "answer" columns among the features (this week's demand, POS, stock...)
forbidden = {"actual_demand_raw", "actual_demand_clean", "demand", "pos_units", "pos_revenue", "stores_selling",
             "confirmed_quantity", "on_hand_qty", "in_transit_qty", "rolling_median"}
assert not forbidden & set(FEATURES), forbidden & set(FEATURES)

# 2) lag_1 really is LAST week's demand
prev = fw.select(*KEYS, F.date_add("week_start", 7).alias("week_start"), F.col("actual_demand_clean").alias("prev_week"))
bad_lag = fw.join(prev, KEYS + ["week_start"]).where("abs(lag_1 - prev_week) > 1e-9").count()
assert bad_lag == 0, f"{bad_lag} rows where lag_1 is not last week's demand"

# 3) roll_mean_4 is the average of the 4 previous weeks (it does NOT include this week)
bad_roll = fw.where("lag_4 IS NOT NULL AND abs(roll_mean_4 - (lag_1 + lag_2 + lag_3 + lag_4) / 4) > 1e-6").count()
assert bad_roll == 0, f"{bad_roll} rows where roll_mean_4 includes the current week"

# 4) no feature is (almost) identical to the target - a classic sign of leakage
def safe_corr(a, b):
    return F.try_divide(F.covar_samp(a, b), F.stddev_samp(a) * F.stddev_samp(b))
corrs = fw.agg(*[safe_corr(F.col(c).cast("double"), "actual_demand_clean").alias(c) for c in FEATURES]).first().asDict()
too_high = {c: round(v, 3) for c, v in corrs.items() if v is not None and abs(v) > 0.99}
assert not too_high, f"suspiciously perfect features: {too_high}"

top = sorted(((c, v) for c, v in corrs.items() if v is not None), key=lambda x: -abs(x[1]))[:10]
display(spark.createDataFrame([(c, float(round(v, 3))) for c, v in top], "feature string, corr_with_demand double"))
print("PHASE 5 FEATURES: PASSED")
