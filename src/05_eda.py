# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Exploratory Data Analysis (EDA)  ⚠️ SYNTHETIC DATA
# MAGIC
# MAGIC Before building models we look at the data and answer simple questions:
# MAGIC 1. Is demand growing? 2. Is it seasonal? 3. How many products sell only now and then (intermittent)?
# MAGIC 4. Do promotions, price and weather change demand? 5. Do shop sales (POS) and orders match?
# MAGIC
# MAGIC The answers tell us which models and features to use. Heavy work runs in Spark; only small results go to
# MAGIC pandas for charts.

# COMMAND ----------

import os, sys
sys.path.insert(0, os.getcwd())
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from dfp.config import load_config
from dfp.uc import resolve_catalog

CATALOG = resolve_catalog(load_config("dev"), spark.sql).catalog
def T(name):
    return f"{CATALOG}.{name}"

KEYS = ["sku_id", "region_id", "channel"]

def safe_corr(a, b):
    """Correlation that returns NULL (instead of failing) when a column never changes."""
    return F.try_divide(F.covar_samp(a, b), F.stddev_samp(a) * F.stddev_samp(b))

df = spark.table(T("gold.weekly_demand_features"))
print(f"{df.count():,} rows | {df.select(*KEYS).distinct().count()} series | {df.select('sku_id').distinct().count()} SKUs")

# COMMAND ----------

# MAGIC %md ## 1. Overview — size per category and missing values

# COMMAND ----------

display(df.groupBy("category")
          .agg(F.countDistinct("sku_id").alias("skus"),
               F.countDistinct(*KEYS).alias("series"),
               F.sum("demand").alias("total_demand"))
          .orderBy("category"))

# % of empty values per column (POS can be empty in weeks without shop sales)
total = df.count()
display(df.select([(F.sum(F.col(c).isNull().cast("int")) * 100 / total).alias(c) for c in df.columns]))

# COMMAND ----------

# MAGIC %md ## 2. Trend — total weekly demand by category

# COMMAND ----------

trend = df.groupBy("category", "week_start").agg(F.sum("demand").alias("demand")).toPandas()
trend.pivot(index="week_start", columns="category", values="demand").plot(figsize=(12, 4), title="Weekly demand by category")
plt.show()

# COMMAND ----------

# MAGIC %md ## 3. Seasonality — which weeks of the year are above / below normal?
# MAGIC Seasonal index = average demand in that week ÷ average week. 1.3 means 30% above normal.

# COMMAND ----------

season = df.groupBy("category", "week_of_year").agg(F.avg("demand").alias("avg_demand")).toPandas()
season["seasonal_index"] = season["avg_demand"] / season.groupby("category")["avg_demand"].transform("mean")
season.pivot(index="week_of_year", columns="category", values="seasonal_index").plot(figsize=(12, 4), title="Seasonal index")
plt.axhline(1, color="grey"); plt.show()

# COMMAND ----------

# MAGIC %md ## 4. Demand pattern of every series (smooth / erratic / intermittent / lumpy)
# MAGIC Two simple numbers per series (Syntetos–Boylan method):
# MAGIC * **ADI** = weeks ÷ weeks-with-demand → 1.0 means it sells every week, 3.0 means once every 3 weeks
# MAGIC * **CV²** = how much the size of non-zero weeks varies
# MAGIC
# MAGIC | | CV² < 0.49 | CV² ≥ 0.49 |
# MAGIC |---|---|---|
# MAGIC | **ADI < 1.32** | smooth | erratic |
# MAGIC | **ADI ≥ 1.32** | intermittent | lumpy |

# COMMAND ----------

profile = (df.groupBy(*KEYS, "category")
             .agg(F.count("*").alias("weeks"),
                  F.sum((F.col("demand") > 0).cast("int")).alias("weeks_with_demand"),
                  F.avg("demand").alias("avg_demand"),
                  F.avg(F.when(F.col("demand") > 0, F.col("demand"))).alias("avg_nonzero"),
                  F.stddev(F.when(F.col("demand") > 0, F.col("demand"))).alias("std_nonzero"),
                  safe_corr("demand", "temperature_avg").alias("temperature_corr"))
             .withColumn("adi", F.col("weeks") / F.greatest("weeks_with_demand", F.lit(1)))
             .withColumn("cv2", F.pow(F.try_divide("std_nonzero", "avg_nonzero"), 2))
             .withColumn("demand_class",
                         F.when((F.col("adi") < 1.32) & (F.col("cv2") < 0.49), "smooth")
                          .when(F.col("adi") < 1.32, "erratic")
                          .when(F.col("cv2") < 0.49, "intermittent")
                          .otherwise("lumpy")))
profile.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("monitoring.eda_series_profile"))

p = spark.table(T("monitoring.eda_series_profile")).toPandas()
p["demand_class"].value_counts().plot(kind="bar", title="Series by demand pattern"); plt.show()
display(p.groupby(["category", "demand_class"]).size().unstack(fill_value=0).reset_index())

# COMMAND ----------

# MAGIC %md ## 5. Promotions — how much more do we sell in promotion weeks?

# COMMAND ----------

uplift = (df.groupBy("category")
            .agg(F.avg(F.when(F.col("promo_flag") == 1, F.col("demand"))).alias("promo_weeks"),
                 F.avg(F.when(F.col("promo_flag") == 0, F.col("demand"))).alias("normal_weeks"))
            .withColumn("uplift", F.round(F.try_divide("promo_weeks", "normal_weeks"), 2))
            .orderBy(F.desc("uplift")))
display(uplift)

# COMMAND ----------

# MAGIC %md ## 6. Price — do we sell less when we are more expensive than the competitor?
# MAGIC Prices and demand both rise over time (inflation, growth), so comparing raw price with demand is misleading.
# MAGIC Instead we use the **relative price** = our price ÷ competitor price, per series, in normal (non-promo) weeks.
# MAGIC Negative correlation = when we are relatively more expensive, demand drops.

# COMMAND ----------

price_corr = (df.where("promo_flag = 0 AND demand > 0 AND competitor_price > 0")
                .withColumn("relative_price", F.try_divide("current_price", "competitor_price"))
                .groupBy(*KEYS, "category")
                .agg(safe_corr(F.log("relative_price"), F.log("demand")).alias("corr")))
display(price_corr.groupBy("category")
                  .agg(F.round(F.percentile_approx("corr", 0.5), 2).alias("median_relative_price_corr"))
                  .orderBy("median_relative_price_corr"))

# COMMAND ----------

# MAGIC %md ## 7. Weather — does temperature drive demand?

# COMMAND ----------

display(p.groupby("category")["temperature_corr"].median().round(2).reset_index(name="median_temperature_corr"))

ice = (df.where("category = 'Ice Cream'").groupBy("region_id", "week_start")
         .agg(F.sum("demand").alias("demand"), F.avg("temperature_avg").alias("temperature")).toPandas())
for region, d in ice.groupby("region_id"):
    plt.scatter(d["temperature"], d["demand"], s=8, label=region)
plt.xlabel("weekly average temperature (°C)"); plt.ylabel("units"); plt.title("Ice Cream demand vs temperature")
plt.legend(); plt.show()

# COMMAND ----------

# MAGIC %md ## 8. POS (shop sales) vs orders — do they match?
# MAGIC Orders are what retailers buy from us; POS is what shoppers buy from retailers. A ratio near 1.0 is normal.
# MAGIC Big swings point to ordering habits (e.g. ordering every 2 weeks) rather than real consumer demand.

# COMMAND ----------

display(df.groupBy("channel", "region_id")
          .agg(F.sum("demand").alias("orders"), F.sum("pos_units").alias("pos"),
               F.round(F.try_divide(F.sum("demand"), F.sum("pos_units")), 3).alias("orders_to_pos"),
               F.round(F.avg(F.col("no_orders").cast("int")), 3).alias("share_of_weeks_without_orders"))
          .orderBy("channel", "region_id"))

# COMMAND ----------

# MAGIC %md ## 9. Key findings

# COMMAND ----------

up = uplift.toPandas()
print("Series by pattern     :", p["demand_class"].value_counts().to_dict())
print("Biggest promo uplift  :", up.iloc[0]["category"], up.iloc[0]["uplift"], "x")
print("Ice Cream temp. corr. :", round(p.loc[p.category == "Ice Cream", "temperature_corr"].median(), 2))
assert len(p) > 0 and p["demand_class"].notna().all()
print("PHASE 4 EDA: PASSED")
