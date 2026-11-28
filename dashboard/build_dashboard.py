"""Builds dashboard/demand_planning.lvdash.json - the definition of the "Demand Planning" AI/BI dashboard.

Run:  python dashboard/build_dashboard.py      (then commit the JSON; notebook 12 deploys it)

The JSON format is the same as a dashboard exported from Databricks (.lvdash.json):
  datasets  = SQL queries (one per chart family), written against the views/tables of notebooks 08-11
  pages     = canvases on a 12-column grid with widgets (counter v2, table v2, bar/line v3)
  + one global-filters page (category, region, channel, SKU)
`__CATALOG__` is replaced with the real catalog by notebook 12.
"""
import json
from pathlib import Path

C = "__CATALOG__"

# ----------------------------------------------------------------------------------------------- datasets
DATASETS = {
    "ds_kpis": f"SELECT * FROM {C}.forecast.vw_kpis",
    "ds_trend": f"""
        SELECT week, category, region, channel, 'Actual' AS series, SUM(actual) AS units
        FROM {C}.forecast.vw_overview WHERE series_type = 'history' AND week >= DATE_SUB(CURRENT_DATE(), 800)
        GROUP BY ALL
        UNION ALL SELECT week, category, region, channel, 'Forecast P50', SUM(p50)
        FROM {C}.forecast.vw_overview WHERE series_type = 'forecast' GROUP BY ALL
        UNION ALL SELECT week, category, region, channel, 'P10 (low)', SUM(p10)
        FROM {C}.forecast.vw_overview WHERE series_type = 'forecast' GROUP BY ALL
        UNION ALL SELECT week, category, region, channel, 'P90 (high)', SUM(p90)
        FROM {C}.forecast.vw_overview WHERE series_type = 'forecast' GROUP BY ALL""",
    "ds_forecast": f"""
        SELECT forecast_week, forecast_horizon, sku_id, category, brand, region, channel, selected_model, p10, p50, p90,
               CONCAT(sku_id, '|', region, '|', channel) AS series_key
        FROM {C}.forecast.weekly_demand_forecast""",
    "ds_sku_forecast": f"""
        SELECT sku_id, category, brand, region, channel, selected_model,
               ROUND(SUM(CASE WHEN forecast_horizon <= 13 THEN p50 END)) AS next_13_weeks,
               ROUND(SUM(CASE WHEN forecast_horizon <= 52 THEN p50 END)) AS next_52_weeks,
               ROUND(SUM(p50)) AS next_104_weeks,
               ROUND(MAX(validation_wape), 1) AS segment_wape_pct
        FROM {C}.forecast.weekly_demand_forecast GROUP BY ALL""",
    "ds_inventory": f"""
        SELECT sku_id, region_id AS region, category, ROUND(days_of_supply, 1) AS days_of_supply, first_stockout_week,
               stockout_risk, excess_flag, ROUND(excess_units) AS excess_units, ROUND(safety_stock) AS safety_stock,
               ROUND(reorder_point) AS reorder_point, CAST(suggested_order_qty AS BIGINT) AS suggested_order_qty,
               CASE WHEN stockout_risk = 'High' THEN 1 ELSE 0 END AS is_high_risk,
               CASE WHEN stockout_risk = 'Medium' THEN 1 ELSE 0 END AS is_medium_risk,
               CASE WHEN excess_flag THEN 1 ELSE 0 END AS is_excess
        FROM {C}.forecast.inventory_risk""",
    "ds_projection": f"""
        SELECT forecast_week, sku_id, region_id AS region, category, p50 AS weekly_forecast, projected_stock
        FROM {C}.forecast.inventory_projection""",
    "ds_acc_model": f"""
        WITH b AS (SELECT * FROM {C}.ml.backtest_results WHERE forecast IS NOT NULL),
             n AS (SELECT COUNT(DISTINCT model) AS k FROM b),
             common AS (SELECT sku_id, region_id, channel, fold_origin, week_start FROM b
                        GROUP BY ALL HAVING COUNT(DISTINCT model) = (SELECT k FROM n))
        SELECT b.model, ROUND(100 * SUM(ABS(b.actual - b.forecast)) / SUM(b.actual), 1) AS wape_pct,
               ROUND(100 * SUM(b.forecast - b.actual) / SUM(b.actual), 1) AS bias_pct
        FROM b JOIN common USING (sku_id, region_id, channel, fold_origin, week_start) GROUP BY b.model""",
    "ds_acc_horizon": f"""
        WITH b AS (SELECT * FROM {C}.ml.backtest_results WHERE forecast IS NOT NULL),
             n AS (SELECT COUNT(DISTINCT model) AS k FROM b),
             common AS (SELECT sku_id, region_id, channel, fold_origin, week_start FROM b
                        GROUP BY ALL HAVING COUNT(DISTINCT model) = (SELECT k FROM n))
        SELECT b.model, b.horizon_bucket, ROUND(100 * SUM(ABS(b.actual - b.forecast)) / SUM(b.actual), 1) AS wape_pct
        FROM b JOIN common USING (sku_id, region_id, channel, fold_origin, week_start) GROUP BY b.model, b.horizon_bucket""",
    "ds_selection": f"""
        SELECT segment, selected_model, ROUND(validation_wape, 1) AS wape_pct,
               ROUND(seasonal_naive_wape, 1) AS naive_wape_pct, ROUND(forecast_value_add, 1) AS value_add_pts,
               ROUND(forecast_bias, 1) AS bias_pct, review_flag
        FROM {C}.ml.model_selection""",
    "ds_anomalies": f"""
        SELECT week, sku_id, region, channel, category, actual_demand_raw, actual_demand_clean, expected_demand,
               ROUND(robust_z, 1) AS robust_z, COALESCE(outlier_reason, 'drop / price rule') AS outlier_reason, treatment_method
        FROM {C}.forecast.vw_anomalies""",
    "ds_insights": f"SELECT insight, sku_id, region, metric, detail FROM {C}.forecast.vw_insights",
}


def dataset(name, sql):
    return {"name": name, "displayName": name.replace("ds_", "").replace("_", " ").title(),
            "queryLines": [line.strip() + " " for line in sql.strip().splitlines()]}


# ----------------------------------------------------------------------------------------------- widgets
def _q(ds, fields, disaggregated=False, name="main_query"):
    return [{"name": name, "query": {"datasetName": ds, "fields": [{"name": n, "expression": e} for n, e in fields],
                                       "disaggregated": disaggregated}}]


def pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}


def text(name, md, p):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [md]}}, "position": p}


def counter(name, ds, col, title, p):
    f = f"sum({col})"
    return {"widget": {"name": name, "queries": _q(ds, [(f, f"SUM(`{col}`)")]),
                       "spec": {"version": 2, "widgetType": "counter",
                                "encodings": {"value": {"fieldName": f, "displayName": title}},
                                "frame": {"title": title, "showTitle": True}}}, "position": p}


def bar(name, ds, x, y, title, p, x_type="categorical", color=None, agg="SUM"):
    yf = f"{agg.lower()}({y})"
    fields = [(x, f"`{x}`"), (yf, f"{agg}(`{y}`)")] + ([(color, f"`{color}`")] if color else [])
    enc = {"x": {"fieldName": x, "scale": {"type": x_type}, "displayName": x.replace("_", " ")},
           "y": {"fieldName": yf, "scale": {"type": "quantitative"}, "displayName": title}}
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color.replace("_", " ")}
    spec = {"version": 3, "widgetType": "bar", "encodings": enc, "frame": {"title": title, "showTitle": True}}
    if color:
        spec["mark"] = {"layout": "group"}
    return {"widget": {"name": name, "queries": _q(ds, fields), "spec": spec}, "position": p}


def line(name, ds, x, y, title, p, color=None):
    yf = f"sum({y})"
    fields = [(x, f"`{x}`"), (yf, f"SUM(`{y}`)")] + ([(color, f"`{color}`")] if color else [])
    enc = {"x": {"fieldName": x, "scale": {"type": "temporal"}, "displayName": x.replace("_", " ")},
           "y": {"fieldName": yf, "scale": {"type": "quantitative"}, "displayName": title}}
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color.replace("_", " ")}
    return {"widget": {"name": name, "queries": _q(ds, fields),
                       "spec": {"version": 3, "widgetType": "line", "encodings": enc,
                                "frame": {"title": title, "showTitle": True}}}, "position": p}


def table(name, ds, cols, title, p):
    return {"widget": {"name": name, "queries": _q(ds, [(c, f"`{c}`") for c in cols], disaggregated=True),
                       "spec": {"version": 2, "widgetType": "table",
                                "encodings": {"columns": [{"fieldName": c, "displayName": c.replace("_", " ")} for c in cols]},
                                "frame": {"title": title, "showTitle": True}}}, "position": p}


def multi_filter(name, field, title, datasets, p):
    queries = [{"name": f"{ds}_{field}", "query": {"datasetName": ds, "fields": [{"name": field, "expression": f"`{field}`"}],
                                                   "disaggregated": False}} for ds in datasets]
    return {"widget": {"name": name, "queries": queries,
                       "spec": {"version": 2, "widgetType": "filter-multi-select",
                                "encodings": {"fields": [{"fieldName": field, "displayName": title, "queryName": f"{ds}_{field}"}
                                                         for ds in datasets]},
                                "frame": {"title": title, "showTitle": True}}}, "position": p}


def page(name, title, layout, page_type="PAGE_TYPE_CANVAS"):
    return {"name": name, "displayName": title, "pageType": page_type, "layoutVersion": "GRID_V1", "layout": layout}


# ----------------------------------------------------------------------------------------------- pages (12-column grid)
overview = page("overview", "1 · Overview", [
    text("title_overview", "## Demand Planning – 104-week forecast  ·  ⚠️ SYNTHETIC DATA", pos(0, 0, 12, 1)),
    counter("kpi_wape", "ds_kpis", "wape_pct", "WAPE % (backtest)", pos(0, 1, 3, 3)),
    counter("kpi_bias", "ds_kpis", "bias_pct", "Bias % (+ = over-forecast)", pos(3, 1, 3, 3)),
    counter("kpi_fva", "ds_kpis", "forecast_value_add_pts", "Value add vs naive (pts)", pos(6, 1, 3, 3)),
    counter("kpi_total", "ds_kpis", "total_forecast_qty_104w", "Total forecast, 104 weeks", pos(9, 1, 3, 3)),
    counter("kpi_skus", "ds_kpis", "active_skus", "Active SKUs", pos(0, 4, 6, 3)),
    counter("kpi_action", "ds_kpis", "skus_needing_action", "SKUs needing action", pos(6, 4, 6, 3)),
    line("chart_trend", "ds_trend", "week", "units", "History vs forecast (P10 / P50 / P90)", pos(0, 7, 12, 6), color="series"),
    bar("chart_cat", "ds_forecast", "category", "p50", "104-week forecast by category", pos(0, 13, 6, 5)),
    bar("chart_region", "ds_forecast", "region", "p50", "104-week forecast by region", pos(6, 13, 6, 5)),
])
forecast = page("forecast", "2 · Forecast", [
    line("chart_fc_cat", "ds_forecast", "forecast_week", "p50", "Weekly forecast (P50) by category", pos(0, 0, 12, 6), color="category"),
    bar("chart_channel", "ds_forecast", "channel", "p50", "Forecast by channel", pos(0, 6, 6, 5)),
    bar("chart_models", "ds_forecast", "selected_model", "series_key", "Series per forecasting model", pos(6, 6, 6, 5),
        agg="COUNT"),
    table("tbl_sku_fc", "ds_sku_forecast", ["sku_id", "category", "brand", "region", "channel", "selected_model",
                                             "next_13_weeks", "next_52_weeks", "next_104_weeks", "segment_wape_pct"],
          "Forecast per SKU / region / channel", pos(0, 11, 12, 7)),
])
inventory = page("inventory", "3 · Inventory risk", [
    counter("kpi_high", "ds_inventory", "is_high_risk", "High stockout risk", pos(0, 0, 4, 3)),
    counter("kpi_medium", "ds_inventory", "is_medium_risk", "Medium risk – order now", pos(4, 0, 4, 3)),
    counter("kpi_excess", "ds_inventory", "is_excess", "Excess stock (> 120 days)", pos(8, 0, 4, 3)),
    bar("chart_dos", "ds_inventory", "category", "days_of_supply", "Average days of supply by category", pos(0, 3, 6, 5), agg="AVG"),
    bar("chart_order", "ds_inventory", "category", "suggested_order_qty", "Suggested order quantity by category", pos(6, 3, 6, 5)),
    line("chart_proj", "ds_projection", "forecast_week", "projected_stock",
         "Projected stock vs forecast (filter one SKU / region)", pos(0, 8, 12, 5)),
    table("tbl_risk", "ds_inventory", ["sku_id", "region", "category", "stockout_risk", "days_of_supply", "first_stockout_week",
                                        "safety_stock", "reorder_point", "suggested_order_qty", "excess_units"],
          "Stock position and replenishment suggestion", pos(0, 13, 12, 7)),
])
accuracy = page("accuracy", "4 · Accuracy & models", [
    bar("chart_wape_model", "ds_acc_model", "model", "wape_pct", "WAPE % by model (same backtest rows)", pos(0, 0, 6, 5)),
    bar("chart_bias_model", "ds_acc_model", "model", "bias_pct", "Bias % by model", pos(6, 0, 6, 5)),
    bar("chart_wape_h", "ds_acc_horizon", "horizon_bucket", "wape_pct", "WAPE % by horizon (weeks ahead)", pos(0, 5, 12, 5),
        color="model"),
    table("tbl_selection", "ds_selection", ["segment", "selected_model", "wape_pct", "naive_wape_pct", "value_add_pts",
                                            "bias_pct", "review_flag"], "Winning model per segment", pos(0, 10, 12, 7)),
])
anomalies = page("anomalies", "5 · Anomalies", [
    bar("chart_reasons", "ds_anomalies", "outlier_reason", "actual_demand_raw", "Flagged weeks by reason", pos(0, 0, 12, 5),
        agg="COUNT"),
    table("tbl_anomalies", "ds_anomalies", ["week", "sku_id", "region", "channel", "actual_demand_raw", "expected_demand",
                                            "actual_demand_clean", "robust_z", "outlier_reason", "treatment_method"],
          "Outliers – raw vs cleaned demand", pos(0, 5, 12, 7)),
])
insights = page("insights", "6 · Insights", [
    text("txt_insights", "Every insight below is calculated with SQL from the forecast, inventory and model tables – nothing is invented.",
         pos(0, 0, 12, 1)),
    bar("chart_insights", "ds_insights", "insight", "sku_id", "Insights by type", pos(0, 1, 12, 5), agg="COUNT"),
    table("tbl_insights", "ds_insights", ["insight", "sku_id", "region", "metric", "detail"], "Planner insights", pos(0, 6, 12, 8)),
])
filters = page("global_filters", "Filters", [
    multi_filter("f_category", "category", "Category",
                 ["ds_trend", "ds_forecast", "ds_sku_forecast", "ds_inventory", "ds_projection", "ds_anomalies"], pos(0, 0, 3, 2)),
    multi_filter("f_region", "region", "Region",
                 ["ds_trend", "ds_forecast", "ds_sku_forecast", "ds_inventory", "ds_projection", "ds_anomalies", "ds_insights"],
                 pos(3, 0, 3, 2)),
    multi_filter("f_channel", "channel", "Channel", ["ds_trend", "ds_forecast", "ds_sku_forecast", "ds_anomalies"], pos(6, 0, 3, 2)),
    multi_filter("f_sku", "sku_id", "SKU",
                 ["ds_forecast", "ds_sku_forecast", "ds_inventory", "ds_projection", "ds_anomalies", "ds_insights"], pos(9, 0, 3, 2)),
], page_type="PAGE_TYPE_GLOBAL_FILTERS")

DASHBOARD = {"datasets": [dataset(n, s) for n, s in DATASETS.items()],
             "pages": [overview, forecast, inventory, accuracy, anomalies, insights, filters]}


def validate(d):
    """Structural checks that catch the usual reasons a dashboard renders blank."""
    names = {ds["name"] for ds in d["datasets"]}
    versions = {"counter": 2, "table": 2, "bar": 3, "line": 3, "filter-multi-select": 2}
    for p in d["pages"]:
        assert p.get("pageType") and p.get("layoutVersion") == "GRID_V1", p["name"]
        rows = {}
        for item in p["layout"]:
            w, ps = item["widget"], item["position"]
            assert ps["x"] + ps["width"] <= 12, (w["name"], ps)
            rows.setdefault(ps["y"], 0)
            rows[ps["y"]] += ps["width"]
            if "spec" in w:
                assert w["spec"]["version"] == versions[w["spec"]["widgetType"]], w["name"]
                qnames = {q["name"] for q in w["queries"]}
                for q in w["queries"]:
                    assert q["query"]["datasetName"] in names, (w["name"], q["query"]["datasetName"])
                fields = [e.get("queryName") for e in w["spec"]["encodings"].get("fields", [])]
                assert all(f in qnames for f in fields if f), w["name"]
        assert all(v == 12 for v in rows.values()), (p["name"], rows)       # every row fills the grid
    return True


if __name__ == "__main__":
    validate(DASHBOARD)
    out = Path(__file__).with_name("demand_planning.lvdash.json")
    out.write_text(json.dumps(DASHBOARD, indent=2), encoding="utf-8")
    print(f"wrote {out} - {len(DASHBOARD['datasets'])} datasets, {len(DASHBOARD['pages'])} pages, "
          f"{sum(len(p['layout']) for p in DASHBOARD['pages'])} widgets")
