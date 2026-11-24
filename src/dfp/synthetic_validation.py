"""Acceptance checks for the Phase 1 synthetic dataset.

Used by both the Databricks notebook (01_synthetic_data) and the local pytest suite, so the
same rules are enforced everywhere. Each check returns (name, passed, observed_value, rule).
"""
from __future__ import annotations

from datetime import timedelta
from typing import List, Tuple

import numpy as np
import pandas as pd

from .synthetic import CATEGORY_SPEC, PATTERNS, BatchBuilder, SyntheticGenerator

Check = Tuple[str, bool, str, str]


def _weekly_grid(gen: SyntheticGenerator) -> pd.DataFrame:
    wd = gen.truth["weekly_demand_truth"]
    keys = wd[["sku_id", "region_id", "channel"]].drop_duplicates()
    grid = keys.merge(pd.DataFrame({"_week": range(gen.H)}), how="cross")
    g = grid.merge(wd, on=["sku_id", "region_id", "channel", "_week"], how="left").fillna({"ordered_quantity": 0})
    pat = gen.truth["sku_demand_patterns"][["sku_id", "category", "synthetic_demand_pattern"]]
    return g.merge(pat, on="sku_id")


def run_checks(gen: SyntheticGenerator, bb: BatchBuilder) -> List[Check]:
    out: List[Check] = []
    add = lambda n, ok, v, rule: out.append((n, bool(ok), str(v), rule))  # noqa: E731
    prod = gen.products
    b1, b2 = bb.batches["batch_001"], bb.batches["batch_002"]

    # ---- coverage
    cats = sorted(prod["category"].unique())
    add("five_categories", cats == sorted(CATEGORY_SPEC), cats, "all 5 CPG categories present")
    pats = set(prod["_pattern"])
    add("nine_demand_patterns", pats == set(PATTERNS), sorted(pats), "all 9 demand patterns present")
    add("history_weeks", gen.H >= 104, gen.H, ">= 104 weeks of history")
    add("all_twelve_sources", len(set(b1) | set(b2)) == 12, sorted(set(b1) | set(b2)), "12 source datasets")
    add("multi_level_hierarchy", prod["brand"].nunique() >= 10 and prod["product_id"].nunique() >= 20
        and prod.groupby("category")["subcategory"].nunique().min() >= 2, f"brands={prod['brand'].nunique()} products={prod['product_id'].nunique()} "
        f"subcats={prod['subcategory'].nunique()}", ">=10 brands, >=20 products, >=2 subcategories per category")
    add("three_countries_six_regions", gen.tables["regions"]["country"].nunique() == 3 and len(gen.tables["regions"]) == 6,
        len(gen.tables["regions"]), "3 countries, 6 regions")
    add("three_channels", gen.tables["stores"]["channel"].nunique() == 3, gen.tables["stores"]["channel"].unique().tolist(), "3 channels")

    # ---- primary keys in the clean (pre-injection) data
    t = gen.tables
    pk = {"pos": ["transaction_id"], "orders": ["order_id", "order_item"], "sales": ["sales_doc_id", "sales_doc_item"],
          "pricing": ["price_record_id"], "promotions": ["promotion_id"], "inventory": ["snapshot_date", "sku_id", "region_id"],
          "weather": ["region_id", "date", "weather_forecast_type"], "stores": ["store_id"], "regions": ["region_id"],
          "suppliers": ["supplier_id"], "calendar": ["date", "country_code"]}
    for src, cols in pk.items():
        d = int(t[src].duplicated(cols).sum())
        add(f"pk_unique_clean_{src}", d == 0, d, f"{cols} unique before DQ injection")
    add("pk_unique_clean_products", not prod["sku_id"].duplicated().any(), 0, "sku_id unique")

    # ---- referential integrity in the clean data
    skus = set(prod["sku_id"])
    for src in ("pos", "orders", "sales", "pricing", "promotions", "inventory"):
        bad = int((~t[src]["sku_id"].isin(skus)).sum())
        add(f"ri_clean_{src}_sku", bad == 0, bad, "all sku_id exist in product master (clean data)")
    add("ri_clean_pos_store", t["pos"]["store_id"].isin(t["stores"]["store_id"]).all(), "", "store_id exists")

    # ---- injected issues are present and logged
    iss = pd.DataFrame(bb.issues)
    for issue in ("exact_duplicate_within_batch", "late_arriving", "invalid_sku", "negative_quantity", "null_store_id",
                  "missing_store_day", "data_entry_spike_x50", "resent_duplicate_cross_batch", "restatement_latest_wins",
                  "alt_date_format_dd.MM.yyyy", "scd2_lifecycle_change", "scd2_attribute_change_product_name",
                  "scd1_attribute_change", "new_sku_in_batch_002", "price_keyed_x10", "sensor_error_99.9"):
        n = int((iss["issue_type"] == issue).sum())
        add(f"injected_{issue}", n > 0, n, "issue injected and logged in ground truth")
    dups = int(b1["pos"].duplicated().sum())
    add("raw_pos_batch1_has_exact_duplicates", dups > 0, dups, "raw files intentionally contain duplicates")

    # ---- schema evolution
    for src, col in (("pos", "loyalty_member_flag"), ("pos", "basket_count"), ("orders", "order_source"),
                     ("products", "sustainability_tag")):
        add(f"schema_evolution_{src}_{col}", col in b2[src].columns and col not in b1[src].columns, col,
            "new column only in batch_002")

    # ---- late arrivals really are late
    late_ids = set(iss.loc[(iss["issue_type"] == "late_arriving") & (iss["source"] == "pos"), "record_key"])
    late_rows = b2["pos"][b2["pos"]["transaction_id"].isin(late_ids)]
    late_ok = (pd.to_datetime(late_rows["transaction_date"]).dt.date < gen.cutoff_date).all() and len(late_rows) > 0
    add("late_rows_have_pre_cutoff_event_dates", late_ok, len(late_rows), "event date < batch_002 window start")
    add("late_rows_absent_from_batch1", not b1["pos"]["transaction_id"].isin(late_ids).any(), "", "late rows not in batch_001")

    # ---- batch windows
    d1 = pd.to_datetime(b1["orders"]["order_date"]).dt.date.max()
    add("batch1_orders_before_cutoff", d1 < gen.cutoff_date, d1, f"< {gen.cutoff_date}")

    # ---- future-known exogenous inputs
    pr = t["promotions"]
    approved = pr[pr["promotion_status"] == "Approved"]
    add("future_approved_promotions", len(approved) > 0 and (pd.to_datetime(approved["start_date"]).dt.date > gen.hist_end.date()).all(),
        len(approved), "approved promotions exist only in the future")
    add("future_draft_promotions_exist", (pr["promotion_status"] == "Draft").any(), int((pr["promotion_status"] == "Draft").sum()),
        "unapproved (Draft) plans exist so Phase 9 can prove they are excluded")
    fut_cal = t["calendar"][t["calendar"]["is_future"]]
    n_future_weeks = fut_cal["week_start"].nunique()
    add("calendar_covers_104_future_weeks", n_future_weeks >= 104, n_future_weeks, ">= 104 future weeks in calendar")
    add("planned_future_prices", (t["pricing"]["price_status"] == "Planned").any(), int((t["pricing"]["price_status"] == "Planned").sum()),
        "planned list-price changes available as future-known inputs")
    add("weather_short_range_forecast", (t["weather"]["weather_forecast_type"] == "forecast").sum() == 14 * 6,
        int((t["weather"]["weather_forecast_type"] == "forecast").sum()), "14-day forecast per region")
    leak = t["weather"][(t["weather"]["weather_forecast_type"] == "actual")]["date"].max()
    add("no_future_weather_actuals", pd.Timestamp(leak).date() <= gen.hist_end.date() + timedelta(days=6), leak,
        "no weather actuals after history end")

    # ---- demand pattern signatures (realism)
    g = _weekly_grid(gen)
    ser = g.groupby(["sku_id", "region_id", "channel", "synthetic_demand_pattern"])["ordered_quantity"].agg(
        zero_share=lambda x: (x == 0).mean(), cv=lambda x: x.std() / max(x.mean(), 1e-9)).reset_index()
    med = ser.groupby("synthetic_demand_pattern")[["zero_share", "cv"]].median()
    add("intermittent_has_many_zero_weeks", med.loc["intermittent", "zero_share"] >= 0.3, round(med.loc["intermittent", "zero_share"], 2),
        "median zero-week share >= 30%")
    add("erratic_more_volatile_than_smooth", med.loc["erratic", "cv"] > 1.8 * med.loc["smooth", "cv"],
        f"erratic={med.loc['erratic', 'cv']:.2f} smooth={med.loc['smooth', 'cv']:.2f}", "CV(erratic) > 1.8 x CV(smooth)")

    # weather sensitivity: ice-cream weather-sensitive SKUs track temperature (region level, uses demand mean)
    ws = prod.index[(prod["_pattern"] == "weather_sensitive")]
    cors = []
    for s in ws:
        for r in range(len(gen.region_params)):
            m = gen.region_mean[s, r]
            if m.sum() > 0:
                cors.append(np.corrcoef(np.log1p(m), gen.weekly_temp[r, :gen.H])[0, 1])
    add("weather_sensitive_correlates_with_temperature", np.median(cors) > 0.5, round(float(np.median(cors)), 2),
        "median corr(log demand, temperature) > 0.5")

    # promotion uplift: promo-driven SKUs sell clearly more in promo weeks (region-level mean)
    pdx = prod.index[prod["_pattern"] == "promotion_driven"]
    ratios = []
    for s in pdx:
        for r in range(len(gen.region_params)):
            on = gen.promo_active[s, r, :gen.H]
            m = gen.region_mean[s, r]
            if on.any() and (~on).any() and m[~on].mean() > 0:
                ratios.append(m[on].mean() / m[~on].mean())
    add("promotion_driven_uplift", np.median(ratios) > 1.5, round(float(np.median(ratios)), 2), "median promo/non-promo ratio > 1.5")

    # lifecycle
    np_rows = prod[prod["_pattern"] == "new_product"]
    zero_before = all(gen.region_mean[i, :, :max(r["_launch_w"], 0)].sum() == 0 for i, r in np_rows.iterrows())
    add("new_product_zero_before_launch", zero_before, len(np_rows), "no demand before launch week")
    eol = prod[prod["_pattern"] == "end_of_life"]
    zero_after = all(gen.region_mean[i, :, r["_disc_w"]:].sum() == 0 for i, r in eol.iterrows())
    add("eol_zero_after_discontinuation", zero_after, len(eol), "no demand after discontinuation")
    cold = prod[(prod["_launch_w"] > gen.H - 26)]
    add("cold_start_sku_present", len(cold) >= 1, cold["sku_id"].tolist(), "at least one SKU with < 26 weeks history")

    # stockouts make shipments < orders sometimes (unconstrained vs constrained demand)
    o = t["orders"]
    fill = o["confirmed_quantity"].sum() / o["ordered_quantity"].sum()
    add("fill_rate_realistic", 0.85 <= fill < 1.0, round(fill, 4), "0.85 <= fill rate < 1.0")
    return out


def checks_frame(checks: List[Check]) -> pd.DataFrame:
    return pd.DataFrame(checks, columns=["check", "passed", "observed", "rule"])
