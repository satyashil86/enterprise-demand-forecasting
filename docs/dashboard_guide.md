# The Demand Planning AI/BI dashboard

**Automatic (recommended):** run `src/12_create_dashboard`. It creates and publishes the dashboard from
`dashboard/demand_planning.lvdash.json` and prints the link. To change the dashboard, edit
`dashboard/build_dashboard.py`, run `python dashboard/build_dashboard.py`, commit, and re-run notebook 12 (it updates
the existing dashboard).

**Manual alternative (for learning the tool, about 30 minutes, no code):**

All data comes from views made by `11_dashboard_governance` in `demand_planning.forecast`.

1. Left sidebar: **Dashboards → Create dashboard**. Name it "Demand Planning".
2. **Data** tab → **Create from SQL**: add one dataset per query below.
3. **Canvas** tab: add 5 pages (the + next to the page tabs) and build the widgets listed.
4. Add **global filters** (Filter widget, "apply to all pages"): category, brand, sku_id, region, channel,
   selected_model, week. Each filter is linked to the matching column in every dataset.
5. **Publish**. When sharing, choose **"Don't embed credentials"** (viewer's own permissions). Unity Catalog grants
   and the secure view then apply to every viewer: dashboard visibility is never the security layer.

| Dataset | SQL |
|---|---|
| overview | `SELECT * FROM demand_planning.forecast.vw_overview` |
| kpis | `SELECT * FROM demand_planning.forecast.vw_kpis` |
| accuracy | `SELECT * FROM demand_planning.forecast.vw_accuracy` |
| selection | `SELECT * FROM demand_planning.forecast.vw_model_selection` |
| inventory | `SELECT * FROM demand_planning.forecast.vw_inventory` |
| anomalies | `SELECT * FROM demand_planning.forecast.vw_anomalies` |
| insights | `SELECT * FROM demand_planning.forecast.vw_insights` |

## Page 1: Overview
- Counter tiles (kpis): `wape_pct`, `bias_pct`, `forecast_value_add_pts`, `total_forecast_qty_104w`, `active_skus`,
  `skus_needing_action`, `most_used_model`
- Line chart (overview): x = week, y = SUM(actual) and SUM(p50), plus SUM(p10) and SUM(p90) as the band
- Bar charts (overview, series_type = forecast): SUM(p50) by category; SUM(p50) by region; COUNT by selected_model

## Page 2: Forecast accuracy
- Bar: AVG(wape_pct) by model · Line: wape_pct by horizon_bucket, colour = model · Bar: bias_pct by model
- Table (selection): segment, selected_model, validation_wape, seasonal_naive_wape, forecast_value_add, review_flag
- Heatmap: category × model, value = wape_pct (to drill down by product family)

## Page 3: Inventory risk
- Counters: COUNT where stockout_risk = 'High' / 'Medium'; COUNT where excess_flag
- Table: sku_id, region, days_of_supply, first_stockout_week, stockout_risk, suggested_order_qty
- Line: projected_stock by forecast_week (filter on one sku_id); bar: weekly_forecast vs projected_stock

## Page 4: Anomalies
- Scatter: week vs actual_demand_raw, colour = outlier_reason · Bar: COUNT by outlier_reason
- Table: sku_id, region, week, actual_demand_raw, expected_demand, outlier_reason, treatment_method

## Page 5: Insights
- Table (insights): insight, sku_id, region, metric, detail, with a filter on insight
- Counter per insight type (Increasing, Declining, Promotion-driven, Weather-sensitive, Inventory risk, Model selection)

Every number on this page is calculated in SQL from the tables. You can switch on Genie/AI explanations for the
dashboard, but they only *explain* these numbers and never calculate forecasts or KPIs.
