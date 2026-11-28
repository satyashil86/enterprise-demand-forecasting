"""Shared forecasting helpers for 08_models_backtest and 09_forecast (pandas + scikit-learn style).

The one idea to understand
--------------------------
We stand at a FORECAST ORIGIN (the last week with known actuals) and predict each week up to
104 weeks ahead. Every training/prediction row is one (series, origin, target week):

    features = what we knew at the origin      (recent level: last value, 4/13/52-week means, ...)
             + what is planned for the target  (calendar, approved promotion, planned price, normal temperature)
             + the same week last year         (seasonal_lag: target-52 weeks if h <= 52, else target-104 weeks)
             + h = how many weeks ahead

Nothing from after the origin is used, so the backtest is honest (no leakage), and the same
code produces the real 104-week forecast. One "global" model is trained for all series.
"""
from __future__ import annotations

import time

import numpy as np
import pandas as pd

KEYS = ["sku_id", "region_id", "channel"]
TARGET_KNOWN = ["week_of_year", "month", "holiday_days", "festival_days", "promo_flag", "discount_pct",
                "display_flag", "regular_price", "current_price", "temperature_normal"]
LEVEL = ["last_value", "mean_4", "mean_13", "mean_52", "std_13", "pos_mean_4", "promo_uplift", "weeks_of_history"]
CATS = ["category", "region_id", "channel"]
FEATURES = ["h", "seasonal_lag", "seasonal_lag_is_104"] + TARGET_KNOWN + LEVEL + [f"{c}_code" for c in CATS]
HORIZON_BUCKETS = [(1, 4, "01-04"), (5, 13, "05-13"), (14, 26, "14-26"), (27, 52, "27-52"), (53, 104, "53-104")]


def horizon_bucket(h: pd.Series) -> pd.Series:
    out = pd.Series("53-104", index=h.index)
    for lo, hi, name in HORIZON_BUCKETS:
        out[(h >= lo) & (h <= hi)] = name
    return out


def add_level_features(hist: pd.DataFrame) -> pd.DataFrame:
    """Recent level of each series AT each week (inclusive) - used as 'what we knew at the origin'."""
    hist = hist.sort_values(KEYS + ["wi"]).copy()
    g = hist.groupby(KEYS, sort=False)
    hist["last_value"] = hist["y"]
    for n in (4, 13, 52):
        hist[f"mean_{n}"] = g["y"].transform(lambda s, n=n: s.rolling(n, min_periods=1).mean())
    hist["std_13"] = g["y"].transform(lambda s: s.rolling(13, min_periods=2).std())
    hist["pos_mean_4"] = g["pos_units"].transform(lambda s: s.rolling(4, min_periods=1).mean())
    promo_y = (hist["y"] * hist["promo_flag"]).groupby([hist[k] for k in KEYS]).cumsum()
    promo_n = hist.groupby(KEYS)["promo_flag"].cumsum()
    norm_y = (hist["y"] * (1 - hist["promo_flag"])).groupby([hist[k] for k in KEYS]).cumsum()
    norm_n = (1 - hist["promo_flag"]).groupby([hist[k] for k in KEYS]).cumsum()
    hist["promo_uplift"] = (promo_y / promo_n.replace(0, np.nan)) / (norm_y / norm_n.replace(0, np.nan)).replace(0, np.nan)
    hist["weeks_of_history"] = g.cumcount()
    return hist


def add_codes(df: pd.DataFrame, categories: dict) -> pd.DataFrame:
    for c in CATS:
        df[f"{c}_code"] = pd.Categorical(df[c], categories=categories[c]).codes
    return df


def build_rows(hist: pd.DataFrame, targets: pd.DataFrame, origins, max_h: int) -> pd.DataFrame:
    """One row per (series, origin, target week) for target weeks origin+1 .. origin+max_h present in `targets`."""
    level = hist[KEYS + ["wi"] + LEVEL]
    parts = []
    for o in origins:
        t = targets[(targets["wi"] > o) & (targets["wi"] <= o + max_h)]
        t = t.drop(columns=[c for c in LEVEL if c in t.columns])   # level comes from the ORIGIN week, not the target
        t["h"] = t["wi"] - o
        t["origin_wi"] = o
        lv = level[level["wi"] == o].drop(columns="wi")
        parts.append(t.merge(lv, on=KEYS, how="inner"))          # series must already exist at the origin
    rows = pd.concat(parts, ignore_index=True)
    y = hist[KEYS + ["wi", "y"]]
    s52 = y.assign(wi=y["wi"] + 52).rename(columns={"y": "y_lag52"})
    s104 = y.assign(wi=y["wi"] + 104).rename(columns={"y": "y_lag104"})
    rows = rows.merge(s52, on=KEYS + ["wi"], how="left").merge(s104, on=KEYS + ["wi"], how="left")
    rows["seasonal_lag_is_104"] = (rows["h"] > 52).astype(int)
    rows["seasonal_lag"] = np.where(rows["h"] <= 52, rows["y_lag52"], rows["y_lag104"])
    return rows


def training_rows(hist: pd.DataFrame, last_actual_wi: int, step: int = 4, max_h: int = 104) -> pd.DataFrame:
    """Many past origins; only targets whose actuals are known by `last_actual_wi`."""
    known = hist[hist["wi"] <= last_actual_wi]
    origins = range(13, last_actual_wi, step)
    rows = build_rows(known, known, origins, max_h)
    return rows[rows["wi"] <= last_actual_wi]


def make_models(params: dict | None = None) -> dict:
    from lightgbm import LGBMRegressor
    from sklearn.ensemble import RandomForestRegressor
    from xgboost import XGBRegressor
    p = {"num_leaves": 63, "learning_rate": 0.05, **(params or {})}
    return {
        "lightgbm": LGBMRegressor(objective="tweedie", n_estimators=400, learning_rate=p["learning_rate"],
                                  num_leaves=p["num_leaves"], min_child_samples=50, subsample=0.8, subsample_freq=1,
                                  colsample_bytree=0.8, random_state=42, verbose=-1),
        "xgboost": XGBRegressor(objective="reg:tweedie", n_estimators=400, learning_rate=0.05, max_depth=8,
                                subsample=0.8, colsample_bytree=0.8, tree_method="hist", random_state=42),
        "random_forest": RandomForestRegressor(n_estimators=60, max_depth=14, min_samples_leaf=20, max_samples=0.3,
                                               n_jobs=-1, random_state=42),
    }


def fit_predict(model_name: str, model, train: pd.DataFrame, test: pd.DataFrame):
    """Train on `train`, predict `test`; returns (predictions >= 0, seconds)."""
    t0 = time.time()
    X, Xt = train[FEATURES].astype("float32"), test[FEATURES].astype("float32")
    if model_name == "random_forest":                       # sklearn trees: no missing values
        X, Xt = X.fillna(-1), Xt.fillna(-1)
    model.fit(X, train["y"])
    return np.clip(model.predict(Xt), 0, None), time.time() - t0


def mstl_arima(hist: pd.DataFrame, origin_wi: int, h: int, min_weeks: int = 104) -> pd.DataFrame:
    """Statistical benchmark: remove the yearly season (MSTL, 52 weeks), fit AutoARIMA on the rest.
    Only for series with >= 2 years of history at the origin. Returns KEYS + wi + forecast."""
    from statsforecast import StatsForecast
    from statsforecast.models import MSTL, AutoARIMA
    known = hist[hist["wi"] <= origin_wi]
    counts = known.groupby(KEYS)["wi"].transform("count")
    known = known[counts >= min_weeks]
    if known.empty:
        return pd.DataFrame(columns=KEYS + ["wi", "forecast"])
    sf_df = pd.DataFrame({"unique_id": known[KEYS].astype(str).agg("|".join, axis=1),
                          "ds": pd.to_datetime(known["week_start"]), "y": known["y"].astype(float)})
    sf = StatsForecast(models=[MSTL(season_length=52, trend_forecaster=AutoARIMA())], freq="W-MON", n_jobs=-1)
    fc = sf.forecast(df=sf_df, h=h).reset_index()
    fc[KEYS] = fc["unique_id"].str.split("|", expand=True)
    fc["wi"] = origin_wi + ((pd.to_datetime(fc["ds"]) - pd.to_datetime(known["week_start"].max())).dt.days // 7)
    return fc[KEYS + ["wi"]].assign(forecast=np.clip(fc["MSTL"].to_numpy(), 0, None))


def metrics(df: pd.DataFrame, actual="actual", forecast="forecast") -> pd.Series:
    """WAPE % = sum|A-F| / sum A ; bias % = sum(F-A) / sum A (positive = over-forecast)."""
    a, f = df[actual], df[forecast]
    total = a.sum()
    return pd.Series({
        "wape": np.nan if total <= 0 else 100 * (a - f).abs().sum() / total,
        "bias": np.nan if total <= 0 else 100 * (f - a).sum() / total,
        "mae": (a - f).abs().mean(),
        "rmse": float(np.sqrt(((a - f) ** 2).mean())),
        "n": len(df), "actual_total": total,
    })
