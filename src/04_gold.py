# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold layer — weekly star schema  ⚠️ SYNTHETIC DATA
# MAGIC
# MAGIC Silver has clean **daily / transactional** data. Forecasting works on **weekly** numbers, so Gold:
# MAGIC 1. builds **dimensions** (who / where / when): product, region, calendar
# MAGIC 2. builds **weekly facts** (how much): demand, POS, promotions, price, weather, inventory
# MAGIC 3. joins them into one table for modelling: **`gold.weekly_demand_features`**
# MAGIC
# MAGIC ```
# MAGIC   dim_product   dim_region   dim_calendar
# MAGIC         \           |           /
# MAGIC   fact_weekly_demand · fact_weekly_pos · fact_weekly_promotions
# MAGIC   fact_weekly_price  · fact_weekly_weather · fact_weekly_inventory
# MAGIC                       |
# MAGIC          gold.weekly_demand_features   (1 row = SKU × region × channel × week)
# MAGIC ```
# MAGIC **Definitions:** week = Monday-start week · demand = `ordered_quantity` (what customers asked for).

# COMMAND ----------

import os, sys
sys.path.insert(0, os.getcwd())
from pyspark.sql import functions as F, Window
from dfp.config import load_config
from dfp.uc import resolve_catalog

CATALOG = resolve_catalog(load_config("dev"), spark.sql).catalog
def T(name):                      # "silver.pos" -> "demand_planning.silver.pos"
    return f"{CATALOG}.{name}"

def save(df, name):               # write a Gold table (full refresh = safe to re-run)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T(name))
    print(f"{name:32s} {spark.table(T(name)).count():>8,} rows")

KEYS = ["sku_id", "region_id", "channel"]          # one forecasting series
def week(col):                                     # any date -> Monday of its week
    return F.to_date(F.date_trunc("week", F.col(col)))

# COMMAND ----------

# MAGIC %md ## 1. Dimensions

# COMMAND ----------

# Product: only the CURRENT version of each SKU from the SCD2 table
dim_product = (spark.table(T("silver.products_scd2")).where("is_current")
               .select("product_sk", "sku_id", "product_name", "brand", "category", "subcategory",
                       "pack_size", "launch_date", "lifecycle_status"))
save(dim_product, "gold.dim_product")

save(spark.table(T("silver.regions")), "gold.dim_region")

# Calendar: one row per country and week, with number of holiday / festival days
dim_calendar = (spark.table(T("silver.calendar"))
                .groupBy("country_code", "week_start")
                .agg(F.min("iso_week").alias("week_of_year"),
                     F.min("month").alias("month"),
                     F.sum(F.col("holiday_flag").cast("int")).alias("holiday_days"),
                     F.sum(F.col("festival_flag").cast("int")).alias("festival_days")))
save(dim_calendar, "gold.dim_calendar")

# COMMAND ----------

# MAGIC %md ## 2. Weekly demand (the forecasting target) — with explicit zero weeks
# MAGIC If a series has no order in a week, there is simply no row. We add those weeks with **demand = 0**,
# MAGIC otherwise averages and lags later would be wrong (very important for intermittent products).

# COMMAND ----------

orders_weekly = (spark.table(T("silver.orders"))
                 .withColumn("week_start", week("order_date"))
                 .groupBy(*KEYS, "week_start")
                 .agg(F.sum("ordered_quantity").alias("demand"),
                      F.sum("confirmed_quantity").alias("confirmed_quantity")))

first_week, last_week = orders_weekly.agg(F.min("week_start"), F.max("week_start")).first()
print("history:", first_week, "->", last_week)

all_weeks = spark.sql(f"SELECT explode(sequence(DATE'{first_week}', DATE'{last_week}', INTERVAL 1 WEEK)) AS week_start")

# every series, every week from its first order onwards
series_start = orders_weekly.groupBy(*KEYS).agg(F.min("week_start").alias("first_order_week"))
grid = series_start.join(all_weeks, all_weeks.week_start >= series_start.first_order_week).drop("first_order_week")

fact_weekly_demand = (grid.join(orders_weekly, KEYS + ["week_start"], "left")
                      .withColumn("no_orders", F.col("demand").isNull())
                      .fillna(0, ["demand", "confirmed_quantity"]))
save(fact_weekly_demand, "gold.fact_weekly_demand")

# COMMAND ----------

# MAGIC %md ## 3. Weekly POS, promotions, weather, inventory

# COMMAND ----------

fact_weekly_pos = (spark.table(T("silver.pos"))
                   .withColumn("week_start", week("transaction_date"))
                   .groupBy(*KEYS, "week_start")
                   .agg(F.sum("quantity").alias("pos_units"),
                        F.sum("revenue").alias("pos_revenue"),
                        F.countDistinct("store_id").alias("stores_selling")))
save(fact_weekly_pos, "gold.fact_weekly_pos")

# A promotion runs from start_date to end_date -> one row for every week it touches.
# Only promotions that really happened (Executed) are history.
fact_weekly_promotions = (spark.table(T("silver.promotions"))
                          .where("promotion_status = 'Executed'")
                          .withColumn("week_start", F.explode(F.expr(
                              "sequence(to_date(date_trunc('week', start_date)), to_date(date_trunc('week', end_date)), INTERVAL 1 WEEK)")))
                          .groupBy("sku_id", "region_id", "week_start")
                          .agg(F.lit(1).alias("promo_flag"),
                               F.max("discount_percentage").alias("discount_pct"),
                               F.max(F.col("display_flag").cast("int")).alias("display_flag")))
save(fact_weekly_promotions, "gold.fact_weekly_promotions")

fact_weekly_weather = (spark.table(T("silver.weather"))
                       .where("weather_forecast_type = 'actual'")
                       .withColumn("week_start", week("date"))
                       .groupBy("region_id", "week_start")
                       .agg(F.avg("temperature").alias("temperature_avg"),
                            F.sum("rainfall").alias("rainfall_total"),
                            F.sum(F.col("extreme_weather_flag").cast("int")).alias("extreme_weather_days")))
save(fact_weekly_weather, "gold.fact_weekly_weather")

fact_weekly_inventory = (spark.table(T("silver.inventory"))
                         .withColumn("week_start", week("snapshot_date"))
                         .select("sku_id", "region_id", "week_start", "on_hand_qty", "in_transit_qty",
                                 "open_po_qty", "safety_stock_qty", "lead_time_days"))
save(fact_weekly_inventory, "gold.fact_weekly_inventory")

# COMMAND ----------

# MAGIC %md ## 4. Weekly price — "as-of" price with forward fill
# MAGIC Price records only exist when a price **changes**. For every week we want the price that was valid
# MAGIC that week = the last known value. `last(..., ignorenulls=True)` over a window carries it forward.

# COMMAND ----------

price_changes = (spark.table(T("silver.pricing"))
                 .where("price_status = 'Active'")
                 .withColumn("week_start", week("effective_date"))
                 .groupBy("sku_id", "region_id", "week_start")          # if 2 changes in a week, keep the last one
                 .agg(F.max_by("regular_price", "effective_date").alias("regular_price"),
                      F.max_by("current_price", "effective_date").alias("current_price"),
                      F.max_by("competitor_price", "effective_date").alias("competitor_price")))

price_grid = price_changes.select("sku_id", "region_id").distinct().crossJoin(all_weeks)
carry = Window.partitionBy("sku_id", "region_id").orderBy("week_start").rowsBetween(Window.unboundedPreceding, 0)

fact_weekly_price = (price_grid.join(price_changes, ["sku_id", "region_id", "week_start"], "left")
                     .withColumn("regular_price", F.last("regular_price", ignorenulls=True).over(carry))
                     .withColumn("current_price", F.last("current_price", ignorenulls=True).over(carry))
                     .withColumn("competitor_price", F.last("competitor_price", ignorenulls=True).over(carry))
                     .where("regular_price IS NOT NULL"))
save(fact_weekly_price, "gold.fact_weekly_price")

# COMMAND ----------

# MAGIC %md ## 5. One table for modelling: `gold.weekly_demand_features`

# COMMAND ----------

features = (spark.table(T("gold.fact_weekly_demand"))
            .join(spark.table(T("gold.dim_product")).select("sku_id", "category", "brand", "lifecycle_status"), "sku_id", "left")
            .join(spark.table(T("gold.dim_region")).select("region_id", "country_code"), "region_id", "left")
            .join(spark.table(T("gold.fact_weekly_pos")), KEYS + ["week_start"], "left")
            .join(spark.table(T("gold.fact_weekly_promotions")), ["sku_id", "region_id", "week_start"], "left")
            .join(spark.table(T("gold.fact_weekly_price")), ["sku_id", "region_id", "week_start"], "left")
            .join(spark.table(T("gold.fact_weekly_weather")), ["region_id", "week_start"], "left")
            .join(spark.table(T("gold.fact_weekly_inventory")), ["sku_id", "region_id", "week_start"], "left")
            .join(spark.table(T("gold.dim_calendar")), ["country_code", "week_start"], "left")
            .fillna(0, ["promo_flag", "discount_pct", "display_flag", "pos_units", "pos_revenue", "stores_selling"]))
save(features, "gold.weekly_demand_features")
display(spark.table(T("gold.weekly_demand_features")).orderBy(*KEYS, "week_start").limit(20))

# COMMAND ----------

# MAGIC %md ## 6. Checks — if any `assert` fails, the notebook stops and tells you which one

# COMMAND ----------

f = spark.table(T("gold.weekly_demand_features"))

# 1) exactly one row per series and week
dups = f.groupBy(*KEYS, "week_start").count().where("count > 1").count()
assert dups == 0, f"{dups} duplicate series-weeks"

# 2) no demand lost or invented: Gold total = Silver total
silver_total = spark.table(T("silver.orders")).agg(F.sum("ordered_quantity")).first()[0]
gold_total = f.agg(F.sum("demand")).first()[0]
assert abs(silver_total - gold_total) < 1e-6, (silver_total, gold_total)

# 3) no missing weeks inside a series (zero weeks were added)
span = f.groupBy(*KEYS).agg(F.count("*").alias("rows"), F.min("week_start").alias("a"), F.max("week_start").alias("b"))
gaps = span.where(F.col("rows") != F.datediff("b", "a") / 7 + 1).count()
assert gaps == 0, f"{gaps} series with missing weeks"

# 4) every week starts on a Monday
assert f.where(F.dayofweek("week_start") != 2).count() == 0

# 5) every row has a price, weather and calendar
missing = f.where("regular_price IS NULL OR temperature_avg IS NULL OR week_of_year IS NULL").count()
assert missing == 0, f"{missing} rows without price/weather/calendar"

print(f"PHASE 4 GOLD: PASSED  |  {f.count():,} rows, {span.count()} series, history {first_week} -> {last_week}")
