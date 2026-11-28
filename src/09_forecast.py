# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Final 104-week forecast with P10 / P50 / P90  ⚠️ SYNTHETIC DATA
# MAGIC
# MAGIC 1. Build the **future**: for every active series, the next 104 weeks with what is already known – calendar,
# MAGIC    **approved** promotions, **planned** prices, normal temperature for the week.
# MAGIC 2. Retrain the winning models (from notebook 08) on **all** history and forecast from the last actual week.
# MAGIC 3. Each series uses the winner of its segment; if the winner cannot run for that series (e.g. too little history)
# MAGIC    the fallback model (LightGBM) is used and recorded.
# MAGIC 4. **Uncertainty**: P10 / P90 come from the errors the same model made in the backtest at the same horizon
# MAGIC    (empirical quantiles). We calibrate on the earlier backtest origins and check the coverage on the last one.
# MAGIC
# MAGIC Output: `forecast.weekly_demand_forecast` – exactly 104 weekly rows per active series.

# COMMAND ----------

# MAGIC %pip install lightgbm xgboost statsforecast nixtla --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, time
sys.path.insert(0, os.getcwd())
import numpy as np, pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import functions as F, Window
from dfp.config import load_config
from dfp.uc import resolve_catalog
from dfp import forecasting as fc

CATALOG = resolve_catalog(load_config("dev"), spark.sql).catalog
def T(name):
    return f"{CATALOG}.{name}"
RUN_ID = time.strftime("forecast_%Y%m%dT%H%M%S")
HORIZON = 104

# COMMAND ----------

# MAGIC %md ## 1. History (same preparation as notebook 08)

# COMMAND ----------

base = (spark.table(T("gold.weekly_demand_clean"))
        .join(spark.table(T("features.weekly_features")).select(*fc.KEYS, "week_start", "temperature_normal"),
              fc.KEYS + ["week_start"], "left")
        .select(*fc.KEYS, "week_start", "category", F.col("actual_demand_clean").alias("y"), "pos_units",
                *[c for c in fc.TARGET_KNOWN if c != "temperature_normal"], "temperature_normal"))
hist = base.toPandas()
hist["week_start"] = pd.to_datetime(hist["week_start"])
first_week, last_week = hist["week_start"].min(), hist["week_start"].max()
hist["wi"] = ((hist["week_start"] - first_week).dt.days // 7).astype(int)
LAST = int(hist["wi"].max())
hist = fc.add_level_features(hist)
CATEGORIES = {c: sorted(hist[c].unique()) for c in fc.CATS}
hist = fc.add_codes(hist, CATEGORIES)
print("forecast origin (last actual week):", last_week.date())

# COMMAND ----------

# MAGIC %md ## 2. The future 104 weeks — only information known today (built in Spark)

# COMMAND ----------

last = last_week.date().isoformat()
future_weeks = spark.sql(f"SELECT explode(sequence(DATE'{last}' + INTERVAL 7 DAYS, DATE'{last}' + INTERVAL {7 * HORIZON} DAYS, "
                         f"INTERVAL 1 WEEK)) AS week_start")
products = spark.table(T("gold.dim_product")).select("sku_id", "category", "brand", "lifecycle_status")
active = (spark.table(T("gold.weekly_demand_clean")).select(*fc.KEYS, "country_code").distinct()
          .join(products, "sku_id").where("lifecycle_status <> 'Discontinued'"))

calendar = spark.table(T("gold.dim_calendar"))                 # contains future weeks too
approved = (spark.table(T("silver.promotions")).where("promotion_status = 'Approved'")
            .withColumn("week_start", F.explode(F.expr(
                "sequence(to_date(date_trunc('week', start_date)), to_date(date_trunc('week', end_date)), INTERVAL 1 WEEK)")))
            .groupBy("sku_id", "region_id", "week_start")
            .agg(F.lit(1).alias("promo_flag"), F.max("discount_percentage").alias("discount_pct"),
                 F.max(F.col("display_flag").cast("int")).alias("display_flag")))

# price: last known list price, then planned list-price changes, carried forward week by week
last_price = (spark.table(T("gold.fact_weekly_price")).where(F.col("week_start") == F.lit(last).cast("date"))
              .select("sku_id", "region_id", F.lit(last).cast("date").alias("week_start"), "regular_price"))
planned = (spark.table(T("silver.pricing")).where("price_status = 'Planned'")
           .select("sku_id", "region_id", F.to_date(F.date_trunc("week", "effective_date")).alias("week_start"), "regular_price"))
carry = Window.partitionBy("sku_id", "region_id").orderBy("week_start").rowsBetween(Window.unboundedPreceding, 0)
future_price = (last_price.select("sku_id", "region_id").crossJoin(future_weeks.union(last_price.select("week_start")).distinct())
                .join(last_price.unionByName(planned), ["sku_id", "region_id", "week_start"], "left")
                .withColumn("regular_price", F.last("regular_price", ignorenulls=True).over(carry)))

# normal temperature for each region and week of year = average of the last 2 years
normal_temp = (spark.table(T("gold.fact_weekly_weather"))
               .where(F.col("week_start") > F.date_sub(F.lit(last).cast("date"), 7 * 104))
               .groupBy("region_id", F.weekofyear("week_start").alias("week_of_year"))
               .agg(F.avg("temperature_avg").alias("temperature_normal")))

future = (active.crossJoin(future_weeks)
          .join(calendar.select("country_code", "week_start", "week_of_year", "month", "holiday_days", "festival_days"),
                ["country_code", "week_start"], "left")
          .join(approved, ["sku_id", "region_id", "week_start"], "left")
          .join(future_price, ["sku_id", "region_id", "week_start"], "left")
          .join(normal_temp, ["region_id", "week_of_year"], "left")
          .fillna(0, ["promo_flag", "discount_pct", "display_flag", "holiday_days", "festival_days"])
          .withColumn("current_price", F.col("regular_price") * (1 - F.col("discount_pct") / 100)))
fut = future.toPandas()
fut["week_start"] = pd.to_datetime(fut["week_start"])
fut["wi"] = ((fut["week_start"] - first_week).dt.days // 7).astype(int)
fut = fc.add_codes(fut, CATEGORIES)
print(f"future grid: {len(fut):,} rows = {fut[fc.KEYS].drop_duplicates().shape[0]} active series × {HORIZON} weeks")

# COMMAND ----------

# MAGIC %md ## 3. Retrain the winners on all history and forecast

# COMMAND ----------

selection = spark.table(T("ml.model_selection")).toPandas()
segments = spark.table(T("monitoring.eda_series_profile")).select(*fc.KEYS, "demand_class").toPandas()
model_version = selection["run_id"].iloc[0]

rows = fc.build_rows(hist, fut, [LAST], HORIZON)
rows = rows.merge(segments, on=fc.KEYS, how="left")
rows["segment"] = rows["category"] + " | " + rows["demand_class"].fillna("unknown")
rows = rows.merge(selection[["segment", "selected_model", "validation_wape", "forecast_bias"]], on="segment", how="left")
rows["selected_model"] = rows["selected_model"].fillna("lightgbm")

needed = set(rows["selected_model"]) | {"lightgbm"}
preds = {}
train = fc.training_rows(hist, LAST)
for name, model in fc.make_models().items():
    if name in needed:
        preds[name], secs = fc.fit_predict(name, model, train, rows)
        print(f"trained {name} on {len(train):,} rows in {secs:.0f}s")
if "seasonal_naive" in needed:
    preds["seasonal_naive"] = rows["seasonal_lag"].to_numpy()
if "mstl_arima" in needed:
    ar = fc.mstl_arima(hist, LAST, HORIZON)
    preds["mstl_arima"] = rows[fc.KEYS + ["wi"]].merge(ar, on=fc.KEYS + ["wi"], how="left")["forecast"].to_numpy()

if "timegpt" in needed:                                  # 1 API call for all series that need it
    try:
        from nixtla import NixtlaClient
        key = dbutils.secrets.get("demand_planning_dev", "nixtla_api_key").strip().strip('"').strip("'")
        tg = fc.timegpt_forecast(NixtlaClient(api_key=key), hist, fut, LAST, HORIZON)
        preds["timegpt"] = rows[fc.KEYS + ["week_start"]].merge(tg, on=fc.KEYS + ["week_start"], how="left")["forecast"].to_numpy()
        print(f"TimeGPT (long-horizon) forecast {tg[fc.KEYS].drop_duplicates().shape[0]} series in one call")
    except Exception as e:
        print(f"TimeGPT unavailable ({type(e).__name__}) -> its segments use the fallback model")
        preds["timegpt"] = np.full(len(rows), np.nan)

chosen = np.array([preds[m][i] for i, m in enumerate(rows["selected_model"])], dtype=float)
use_fallback = np.isnan(chosen)
rows["p50"] = np.where(use_fallback, preds["lightgbm"], chosen)
rows["model_used"] = np.where(use_fallback, "lightgbm (fallback)", rows["selected_model"])
print(rows["model_used"].value_counts().to_dict())

# COMMAND ----------

# MAGIC %md ## 4. P10 / P90 from backtest errors (calibrate on early origins, check coverage on the last one)

# COMMAND ----------

bt = spark.table(T("ml.backtest_results")).toPandas().dropna(subset=["forecast"])
bt["rel_err"] = (bt["actual"] - bt["forecast"]) / np.maximum(bt["forecast"], 1)
last_origin = bt["fold_origin"].max()
cal, val = bt[bt["fold_origin"] < last_origin], bt[bt["fold_origin"] == last_origin]
q = cal.groupby(["model", "horizon_bucket"])["rel_err"].quantile([0.1, 0.9]).unstack()
q.columns = ["q10", "q90"]
# beyond 52 weeks there is no backtest -> assumption: 27-52-week errors widened by 50%
far = q.xs("27-52", level="horizon_bucket") * 1.5
q = pd.concat([q, far.assign(horizon_bucket="53-104").set_index("horizon_bucket", append=True)])

def add_band(df, model_col, f_col):
    d = df.merge(q.reset_index().rename(columns={"model": model_col}), on=[model_col, "horizon_bucket"], how="left")
    d["p10"] = np.clip(d[f_col] * (1 + d["q10"].fillna(-0.5)), 0, None)
    d["p90"] = np.maximum(d[f_col] * (1 + d["q90"].fillna(0.5)), d[f_col])
    d["p10"] = np.minimum(d["p10"], d[f_col])
    return d

v = add_band(val.assign(horizon_bucket=val["horizon_bucket"]), "model", "forecast")
coverage = ((v["actual"] >= v["p10"]) & (v["actual"] <= v["p90"])).mean()
print(f"P10-P90 coverage on the held-out origin: {coverage:.1%} (target ≈ 80%)")

rows["horizon_bucket"] = fc.horizon_bucket(rows["h"])
rows["band_model"] = rows["model_used"].str.replace(" (fallback)", "", regex=False)
rows = add_band(rows, "band_model", "p50")

# COMMAND ----------

# MAGIC %md ## 5. Save `forecast.weekly_demand_forecast`

# COMMAND ----------

out = rows                                                     # brand / category came with the future grid
out = pd.DataFrame({
    "forecast_run_id": RUN_ID, "forecast_generated_at": pd.Timestamp.utcnow().tz_localize(None),
    "sku_id": out["sku_id"], "category": out["category"], "brand": out["brand"], "region": out["region_id"],
    "channel": out["channel"], "forecast_week": out["week_start"].dt.date, "forecast_horizon": out["h"].astype(int),
    "forecast_quantity": out["p50"].round(2), "p10": out["p10"].round(2), "p50": out["p50"].round(2),
    "p90": out["p90"].round(2), "selected_model": out["model_used"], "model_version": model_version,
    "validation_wape": out["validation_wape"].round(2), "forecast_bias": out["forecast_bias"].round(2),
    "promo_flag": out["promo_flag"].astype(int)})
spark.createDataFrame(out.astype({"validation_wape": "float64", "forecast_bias": "float64"})) \
     .withColumn("validation_wape", F.when(F.isnan("validation_wape"), None).otherwise(F.col("validation_wape"))) \
     .withColumn("forecast_bias", F.when(F.isnan("forecast_bias"), None).otherwise(F.col("forecast_bias"))) \
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("forecast.weekly_demand_forecast"))
fcst = spark.table(T("forecast.weekly_demand_forecast"))
display(fcst.orderBy("sku_id", "region", "channel", "forecast_week").limit(20))

# COMMAND ----------

# MAGIC %md ## 6. Picture — history and forecast for one series

# COMMAND ----------

k = out.iloc[0]
h_s = hist[(hist.sku_id == k.sku_id) & (hist.region_id == k.region) & (hist.channel == k.channel)]
f_s = out[(out.sku_id == k.sku_id) & (out.region == k.region) & (out.channel == k.channel)]
plt.figure(figsize=(13, 4))
plt.plot(h_s["week_start"], h_s["y"], label="history (clean)", color="#4C78A8")
plt.plot(pd.to_datetime(f_s["forecast_week"]), f_s["p50"], label=f"forecast P50 ({f_s.selected_model.iloc[0]})", color="#F58518")
plt.fill_between(pd.to_datetime(f_s["forecast_week"]), f_s["p10"], f_s["p90"], alpha=0.25, color="#F58518", label="P10-P90")
plt.title(f"{k.sku_id} · {k.region} · {k.channel}"); plt.legend(); plt.show()

# COMMAND ----------

# MAGIC %md ## 7. Checks

# COMMAND ----------

n_series = active.count()
per_series = fcst.groupBy("sku_id", "region", "channel").count()
assert per_series.count() == n_series, "some active series have no forecast"
assert per_series.where(f"count <> {HORIZON}").count() == 0, "a series does not have exactly 104 weeks"
assert fcst.groupBy("sku_id", "region", "channel", "forecast_week").count().where("count > 1").count() == 0, "duplicates"
assert fcst.where("p10 < 0 OR p10 > p50 OR p50 > p90 OR p50 IS NULL").count() == 0, "bad interval / negative forecast"
assert fcst.where(F.dayofweek("forecast_week") != 2).count() == 0, "forecast weeks must be Mondays"
assert fcst.join(spark.table(T("gold.dim_product")), "sku_id", "left_anti").count() == 0, "unknown SKU"
assert 0.5 <= coverage <= 0.98, f"interval coverage {coverage:.0%} is not plausible"
print(f"PHASE 7 FORECAST: PASSED  |  {n_series} series × {HORIZON} weeks, coverage {coverage:.0%}")
