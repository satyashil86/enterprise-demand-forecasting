# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Models, rolling backtest and WAPE-based model selection  ⚠️ SYNTHETIC DATA
# MAGIC
# MAGIC **Question:** which forecasting method works best for which kind of product?
# MAGIC
# MAGIC | Model | Type | Why it is here |
# MAGIC |---|---|---|
# MAGIC | Seasonal Naive | baseline | "same week last year" – every model must beat this |
# MAGIC | MSTL + AutoARIMA | statistical | removes the yearly season, ARIMA on the rest (classic time-series benchmark) |
# MAGIC | LightGBM | machine learning | one global model for all series, uses promotions/price/calendar |
# MAGIC | XGBoost | machine learning | same features, different algorithm |
# MAGIC | Random Forest | machine learning | small add-on for comparison |
# MAGIC | TimeGPT | foundation model | all series, 1 API call per origin, with planned promotions/prices as exogenous inputs (needs a Nixtla key) |
# MAGIC
# MAGIC **Rolling-origin backtest:** pretend it is an earlier date, train only on data up to then, forecast the following
# MAGIC weeks, compare with what really happened. We do this for 3 origins, with **identical origins and actuals for every
# MAGIC model**. Horizons evaluated: 1–4, 5–13, 14–26 and 27–52 weeks (a 104-week backtest would need 4+ years of history).
# MAGIC
# MAGIC **WAPE** = Σ|actual − forecast| ÷ Σ actual × 100 (lower is better) · **Bias** = Σ(forecast − actual) ÷ Σ actual × 100
# MAGIC (positive = over-forecast). The winner is chosen per **segment = category × demand pattern**.

# COMMAND ----------

# MAGIC %pip install lightgbm xgboost statsforecast nixtla --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, json, time
sys.path.insert(0, os.getcwd())
import numpy as np, pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from dfp.config import load_config
from dfp.uc import resolve_catalog
from dfp import forecasting as fc

CATALOG = resolve_catalog(load_config("dev"), spark.sql).catalog
def T(name):
    return f"{CATALOG}.{name}"
RUN_ID = time.strftime("models_%Y%m%dT%H%M%S")

# COMMAND ----------

# MAGIC %md ## 1. Load the cleaned weekly history (Spark → pandas, it is small: ~93k rows)

# COMMAND ----------

base = (spark.table(T("gold.weekly_demand_clean"))
        .join(spark.table(T("features.weekly_features")).select(*fc.KEYS, "week_start", "temperature_normal"),
              fc.KEYS + ["week_start"], "left")
        .select(*fc.KEYS, "week_start", "category", F.col("actual_demand_clean").alias("y"), "pos_units",
                *[c for c in fc.TARGET_KNOWN if c != "temperature_normal"], "temperature_normal"))
hist = base.toPandas()
hist["week_start"] = pd.to_datetime(hist["week_start"])
first_week = hist["week_start"].min()
hist["wi"] = ((hist["week_start"] - first_week).dt.days // 7).astype(int)     # week index 0, 1, 2, ...
LAST = int(hist["wi"].max())

segments = spark.table(T("monitoring.eda_series_profile")).select(*fc.KEYS, "demand_class").toPandas()
hist = fc.add_level_features(hist)
CATEGORIES = {c: sorted(hist[c].unique()) for c in fc.CATS}
hist = fc.add_codes(hist, CATEGORIES)
print(f"{len(hist):,} rows, {hist[fc.KEYS].drop_duplicates().shape[0]} series, weeks 0..{LAST}")

# COMMAND ----------

# MAGIC %md ## 2. Tune LightGBM a little (on an origin *before* the backtest, so the test stays honest)

# COMMAND ----------

tune_origin = 90
tune_train = fc.training_rows(hist, tune_origin)
tune_test = fc.build_rows(hist, hist, [tune_origin], 13)
tuning = []
for params in [{"num_leaves": 31, "learning_rate": 0.05}, {"num_leaves": 63, "learning_rate": 0.05},
               {"num_leaves": 63, "learning_rate": 0.1}]:
    pred, _ = fc.fit_predict("lightgbm", fc.make_models(params)["lightgbm"], tune_train, tune_test)
    tuning.append({**params, "wape": fc.metrics(tune_test.assign(actual=tune_test["y"], forecast=pred))["wape"]})
tuning = pd.DataFrame(tuning).sort_values("wape")
BEST = tuning.iloc[0][["num_leaves", "learning_rate"]].to_dict()
BEST["num_leaves"] = int(BEST["num_leaves"])
display(tuning); print("chosen:", BEST)

# COMMAND ----------

# MAGIC %md ## 3. Rolling-origin backtest (3 origins × 5 local models; TimeGPT in step 4)

# COMMAND ----------

ORIGINS = [LAST - 52, LAST - 39, LAST - 26]          # e.g. weeks 103, 116, 129 -> forecast to the end of history
results, durations, outs = [], [], {}
for o in ORIGINS:
    h_max = LAST - o
    test = fc.build_rows(hist, hist, [o], h_max)
    train = fc.training_rows(hist, o)
    out = test[fc.KEYS + ["category", "wi", "week_start", "h"]].assign(fold_origin=o, actual=test["y"].to_numpy())
    outs[o] = out                                      # kept so TimeGPT is scored on exactly the same rows

    results.append(out.assign(model="seasonal_naive", forecast=test["seasonal_lag"].to_numpy()))
    for name, model in fc.make_models(BEST).items():
        pred, secs = fc.fit_predict(name, model, train, test)
        results.append(out.assign(model=name, forecast=pred)); durations.append((name, o, secs, len(train)))

    t0 = time.time()
    ar = fc.mstl_arima(hist, o, h_max)
    durations.append(("mstl_arima", o, time.time() - t0, 0))
    results.append(out.merge(ar, on=fc.KEYS + ["wi"], how="left").assign(model="mstl_arima"))
    print(f"origin {o}: {len(test):,} test rows, {len(train):,} training rows")

# COMMAND ----------

# MAGIC %md ## 4. TimeGPT (Nixtla foundation model) — all series, same origins
# MAGIC * **One API request per origin** carries all series → 3 calls for the backtest (cost is per call, not per series)
# MAGIC * **Exogenous inputs** (`X_df`): planned promotion, discount, shelf price, holidays, festivals for the forecast weeks.
# MAGIC   TimeGPT does not know about promotions unless we tell it – and we only pass information known in advance.
# MAGIC * **`model="timegpt-1-long-horizon"`** – the variant for long horizons (we forecast up to 104 weeks)
# MAGIC * **`level=[80]`** – TimeGPT returns its own 80% prediction interval; we check how often actuals fall inside
# MAGIC
# MAGIC Needs a key stored as a secret: `databricks secrets put-secret demand_planning_dev nixtla_api_key`.
# MAGIC Without a key (or if the call fails) TimeGPT is skipped and everything else continues.

# COMMAND ----------

timegpt_note, tg_parts = "skipped (no API key)", []
try:
    key = dbutils.secrets.get("demand_planning_dev", "nixtla_api_key").strip().strip('"').strip("'")
except Exception:
    key = None
if key:
    try:
        from nixtla import NixtlaClient
        client = NixtlaClient(api_key=key)
        for o in ORIGINS:
            t0 = time.time()
            tg = fc.timegpt_forecast(client, hist, hist, o, LAST - o)
            durations.append(("timegpt", o, time.time() - t0, 0))
            tg_parts.append(outs[o].merge(tg, on=fc.KEYS + ["week_start"], how="left").assign(model="timegpt"))
            print(f"origin {o}: TimeGPT forecast {tg[fc.KEYS].drop_duplicates().shape[0]} series in one call")
        timegpt_note = f"ran on all eligible series for {len(ORIGINS)} origins ({len(ORIGINS)} API calls)"
    except Exception as e:
        tg_parts, timegpt_note = [], f"skipped ({type(e).__name__}: {str(e)[:150]})"
print("TimeGPT:", timegpt_note)

if tg_parts:
    tg_bt = pd.concat(tg_parts, ignore_index=True).dropna(subset=["forecast"])
    inside = ((tg_bt["actual"] >= tg_bt["tg_lo"]) & (tg_bt["actual"] <= tg_bt["tg_hi"])).mean()
    print(f"TimeGPT's own 80% interval contained {inside:.1%} of actuals (well calibrated if close to 80%)")
    results.append(pd.concat(tg_parts, ignore_index=True).drop(columns=["tg_lo", "tg_hi"]))

# COMMAND ----------

bt = pd.concat(results, ignore_index=True)
bt["horizon_bucket"] = fc.horizon_bucket(bt["h"])
bt = bt.merge(segments, on=fc.KEYS, how="left")
bt["segment"] = bt["category"] + " | " + bt["demand_class"].fillna("unknown")
display(pd.DataFrame(durations, columns=["model", "fold_origin", "seconds", "training_rows"]))

# COMMAND ----------

# MAGIC %md ## 5. Fair comparison — only rows where **every** model produced a forecast
# MAGIC (MSTL-ARIMA needs 2 years of history and seasonal naive needs last year's value, so very new series are excluded
# MAGIC from the comparison and later use the fallback model.)

# COMMAND ----------

wide = bt.pivot_table(index=fc.KEYS + ["fold_origin", "wi"], columns="model", values="forecast", aggfunc="first")
common = wide.dropna().index
cmp = bt.set_index(fc.KEYS + ["fold_origin", "wi"]).loc[common].reset_index()
print(f"comparison rows per model: {len(common):,}  (of {len(wide):,})")

overall = cmp.groupby("model").apply(fc.metrics).sort_values("wape")
display(overall.reset_index())
by_h = cmp.groupby(["model", "horizon_bucket"]).apply(fc.metrics)["wape"].unstack()
display(by_h.reset_index())
by_cat = cmp.groupby(["category", "model"]).apply(fc.metrics)["wape"].unstack()
display(by_cat.round(1).reset_index())

fig, ax = plt.subplots(1, 2, figsize=(13, 4))
overall["wape"].plot(kind="bar", ax=ax[0], title="WAPE % by model (lower is better)", color="#4C78A8")
by_h.T.plot(ax=ax[1], marker="o", title="WAPE % by forecast horizon (weeks)")
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md ## 6. Choose the winner per segment
# MAGIC Rules: lowest WAPE · bias must be within ±20% (else the next best) · flag a winner whose WAPE jumps a lot between
# MAGIC origins (unstable) · **Forecast Value Add (FVA)** = Seasonal-Naive WAPE − winner WAPE (positive = the model adds value).

# COMMAND ----------

seg = cmp.groupby(["segment", "model"]).apply(fc.metrics).reset_index()
fold = cmp.groupby(["segment", "model", "fold_origin"]).apply(fc.metrics)["wape"].groupby(["segment", "model"]).std()
seg = seg.merge(fold.rename("wape_std_across_origins").reset_index(), on=["segment", "model"])

rows = []
for s, g in seg.groupby("segment"):
    g = g.dropna(subset=["wape"]).sort_values("wape")
    ok = g[g["bias"].abs() <= 20]
    win = (ok if len(ok) else g).iloc[0]
    sn = g[g["model"] == "seasonal_naive"]["wape"]
    rows.append({"segment": s, "selected_model": win["model"], "validation_wape": win["wape"], "forecast_bias": win["bias"],
                 "mae": win["mae"], "seasonal_naive_wape": float(sn.iloc[0]) if len(sn) else np.nan,
                 "review_flag": ("bias>20%" if not len(ok) else "") + (" unstable" if win["wape_std_across_origins"] > 15 else ""),
                 "series_points": int(win["n"])})
selection = pd.DataFrame(rows)
selection["forecast_value_add"] = selection["seasonal_naive_wape"] - selection["validation_wape"]
selection["fallback_model"] = "lightgbm"            # used where the winner cannot run (e.g. too little history)
display(selection.sort_values("segment"))
print("winners:", selection["selected_model"].value_counts().to_dict())

# COMMAND ----------

# MAGIC %md ## 7. Save backtest, comparison, selection and model registry

# COMMAND ----------

def to_spark(pdf):
    """pandas -> Spark, turning NaN (pandas 'missing') into proper SQL NULLs."""
    sdf = spark.createDataFrame(pdf)
    for c, t in sdf.dtypes:
        if t in ("double", "float"):
            sdf = sdf.withColumn(c, F.when(F.isnan(c), None).otherwise(F.col(c)))
    return sdf

now = pd.Timestamp.utcnow().tz_localize(None)
bt_out = bt.assign(run_id=RUN_ID, created_at=now, week_start=bt["week_start"].dt.date)
(to_spark(bt_out[fc.KEYS + ["category", "segment", "model", "fold_origin", "week_start", "h", "horizon_bucket",
                                        "actual", "forecast", "run_id", "created_at"]])
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("ml.backtest_results")))
to_spark(seg.assign(run_id=RUN_ID)).write.mode("overwrite").option("overwriteSchema", "true") \
     .saveAsTable(T("ml.model_comparison"))
to_spark(selection.assign(run_id=RUN_ID, selection_timestamp=now)).write.mode("overwrite") \
     .option("overwriteSchema", "true").saveAsTable(T("ml.model_selection"))
MODEL_PARAMS = {"lightgbm": BEST, "timegpt": {"model": "timegpt-1-long-horizon", "level": [80],
                                              "exogenous": fc.TIMEGPT_EXOG, "note": timegpt_note}}
registry = pd.DataFrame([{"model": m, "model_version": RUN_ID,
                          "params": json.dumps(MODEL_PARAMS.get(m, {})), "features": ",".join(fc.FEATURES),
                          "trained_at": now} for m in overall.index])
spark.createDataFrame(registry).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("ml.model_registry"))
print("saved: ml.backtest_results, ml.model_comparison, ml.model_selection, ml.model_registry")

# COMMAND ----------

# MAGIC %md ## 8. Checks

# COMMAND ----------

per_model = cmp.groupby("model").size()
assert per_model.nunique() == 1, "models were not compared on identical rows"
assert set(selection["segment"]) == set(cmp["segment"]), "a segment has no selected model"
assert selection["validation_wape"].notna().all()
best_ml = overall.drop(index=["seasonal_naive"]).iloc[0]
assert best_ml["wape"] < overall.loc["seasonal_naive", "wape"], "no model beats the seasonal naive baseline"
print(f"best model overall: {overall.index[0]} (WAPE {overall.iloc[0]['wape']:.1f}%) vs seasonal naive "
      f"{overall.loc['seasonal_naive', 'wape']:.1f}%")
print("PHASE 6 MODELS + BACKTEST: PASSED")
