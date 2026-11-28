# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Dashboard data + Unity Catalog governance  ⚠️ SYNTHETIC DATA
# MAGIC
# MAGIC **Part A – Dashboard views.** The Databricks AI/BI dashboard reads simple SQL views, one per page:
# MAGIC Overview · Accuracy · Inventory · Anomalies · Insights. (How to build the dashboard: `docs/dashboard_guide.md`.)
# MAGIC Every insight is calculated from data with SQL – nothing is invented.
# MAGIC
# MAGIC **Part B – Governance.** Who may see what is enforced **on the data** (Unity Catalog), not by hiding dashboard tiles:
# MAGIC * 5 groups with least-privilege GRANTs
# MAGIC * row-level security (planners see only their regions) and column masking (costs only for admins) with a dynamic view
# MAGIC * checks of grants, group membership, audit and lineage

# COMMAND ----------

import os, sys
sys.path.insert(0, os.getcwd())
import pandas as pd
from pyspark.sql import functions as F
from dfp.config import load_config
from dfp.uc import resolve_catalog

CATALOG = resolve_catalog(load_config("dev"), spark.sql).catalog
def T(name):
    return f"{CATALOG}.{name}"
def sql(statement):
    return spark.sql(statement)

# COMMAND ----------

# MAGIC %md ## A1. Overview page — history + forecast (P10/P50/P90) in one view, and the KPI cards

# COMMAND ----------

sql(f"""
CREATE OR REPLACE VIEW {T('forecast.vw_overview')} AS
SELECT 'history' AS series_type, h.week_start AS week, h.sku_id, h.category, h.brand, h.region_id AS region, h.channel,
       h.actual_demand_clean AS actual, CAST(NULL AS DOUBLE) AS p10, CAST(NULL AS DOUBLE) AS p50, CAST(NULL AS DOUBLE) AS p90,
       CAST(NULL AS STRING) AS selected_model
FROM {T('gold.weekly_demand_clean')} h
UNION ALL
SELECT 'forecast', f.forecast_week, f.sku_id, f.category, f.brand, f.region, f.channel,
       NULL, f.p10, f.p50, f.p90, f.selected_model
FROM {T('forecast.weekly_demand_forecast')} f
""")

sql(f"""
CREATE OR REPLACE VIEW {T('forecast.vw_kpis')} AS
WITH sel AS (SELECT segment, selected_model FROM {T('ml.model_selection')}),
     bt  AS (SELECT * FROM {T('ml.backtest_results')} WHERE forecast IS NOT NULL),
     -- selected model vs seasonal naive on exactly the same backtest weeks
     pairs AS (SELECT b.actual, b.forecast AS f_selected, n.forecast AS f_naive
               FROM bt b JOIN sel s ON b.segment = s.segment AND b.model = s.selected_model
               JOIN bt n ON n.model = 'seasonal_naive' AND n.sku_id = b.sku_id AND n.region_id = b.region_id
                        AND n.channel = b.channel AND n.fold_origin = b.fold_origin AND n.week_start = b.week_start),
     acc AS (SELECT 100 * SUM(ABS(actual - f_selected)) / SUM(actual) AS wape,
                    100 * SUM(f_selected - actual) / SUM(actual) AS bias,
                    100 * SUM(ABS(actual - f_naive)) / SUM(actual) AS naive_wape FROM pairs),
     fc  AS (SELECT SUM(p50) AS total_forecast_qty, COUNT(DISTINCT sku_id) AS active_skus,
                    MAX_BY(selected_model, cnt) AS most_used_model
             FROM (SELECT *, COUNT(*) OVER (PARTITION BY selected_model) AS cnt FROM {T('forecast.weekly_demand_forecast')})),
     act AS (SELECT COUNT(DISTINCT sku_id) AS skus_needing_action FROM {T('forecast.inventory_risk')}
             WHERE stockout_risk IN ('High', 'Medium') OR excess_flag)
SELECT ROUND(acc.wape, 1) AS wape_pct, ROUND(acc.bias, 1) AS bias_pct,
       ROUND(acc.naive_wape - acc.wape, 1) AS forecast_value_add_pts,
       ROUND(fc.total_forecast_qty) AS total_forecast_qty_104w, fc.active_skus, act.skus_needing_action, fc.most_used_model
FROM acc, fc, act
""")
display(sql(f"SELECT * FROM {T('forecast.vw_kpis')}"))

# COMMAND ----------

# MAGIC %md ## A2. Accuracy, Inventory and Anomalies pages

# COMMAND ----------

sql(f"""
CREATE OR REPLACE VIEW {T('forecast.vw_accuracy')} AS
SELECT model, category, segment, horizon_bucket, fold_origin,
       100 * SUM(ABS(actual - forecast)) / SUM(actual) AS wape_pct,
       100 * SUM(forecast - actual) / SUM(actual) AS bias_pct,
       AVG(ABS(actual - forecast)) AS mae, COUNT(*) AS points
FROM {T('ml.backtest_results')} WHERE forecast IS NOT NULL
GROUP BY model, category, segment, horizon_bucket, fold_origin
""")
sql(f"CREATE OR REPLACE VIEW {T('forecast.vw_model_selection')} AS SELECT * FROM {T('ml.model_selection')}")
sql(f"""
CREATE OR REPLACE VIEW {T('forecast.vw_inventory')} AS
SELECT r.sku_id, r.region_id AS region, r.category, r.days_of_supply, r.first_stockout_week, r.stockout_risk,
       r.excess_flag, r.excess_units, r.safety_stock, r.reorder_point, r.suggested_order_qty,
       p.forecast_week, p.p50 AS weekly_forecast, p.projected_stock
FROM {T('forecast.inventory_risk')} r
JOIN {T('forecast.inventory_projection')} p ON p.sku_id = r.sku_id AND p.region_id = r.region_id
""")
sql(f"""
CREATE OR REPLACE VIEW {T('forecast.vw_anomalies')} AS
SELECT sku_id, region_id AS region, channel, category, week_start AS week, actual_demand_raw, actual_demand_clean,
       rolling_median AS expected_demand, robust_z, outlier_reason, treatment_method, is_drop, price_anomaly
FROM {T('monitoring.outlier_audit')}
""")

# COMMAND ----------

# MAGIC %md ## A3. Insights page — rule-based, every number comes from the tables

# COMMAND ----------

sql(f"""
CREATE OR REPLACE VIEW {T('forecast.vw_insights')} AS
WITH last_year AS (SELECT sku_id, region_id AS region, SUM(actual_demand_clean) / 13 AS last_13w   -- same 13 weeks one year ago
                   FROM {T('gold.weekly_demand_clean')}, (SELECT MAX(week_start) AS m FROM {T('gold.weekly_demand_clean')}) x
                   WHERE week_start BETWEEN DATE_SUB(x.m, 357) AND DATE_SUB(x.m, 273)
                   GROUP BY sku_id, region_id),
     nxt AS (SELECT sku_id, region, category, SUM(p50) / 13 AS next_13w
             FROM {T('forecast.weekly_demand_forecast')} WHERE forecast_horizon <= 13 GROUP BY sku_id, region, category),
     trend AS (SELECT n.sku_id, n.region, n.category, n.next_13w, r.last_13w, n.next_13w / r.last_13w - 1 AS change
               FROM nxt n JOIN last_year r USING (sku_id, region) WHERE r.last_13w > 0),
     promo AS (SELECT sku_id, AVG(CASE WHEN promo_flag = 1 THEN actual_demand_clean END) /
                              AVG(CASE WHEN promo_flag = 0 THEN actual_demand_clean END) AS uplift
               FROM {T('gold.weekly_demand_clean')} GROUP BY sku_id),
     weather AS (SELECT sku_id, PERCENTILE_APPROX(temperature_corr, 0.5) AS corr
                 FROM {T('monitoring.eda_series_profile')} GROUP BY sku_id)
SELECT 'Increasing demand' AS insight, sku_id, region, ROUND(change * 100, 1) AS metric,
       CONCAT('Next 13 weeks ', ROUND(next_13w), '/wk vs ', ROUND(last_13w), '/wk same period last year (+', ROUND(change * 100), '%)') AS detail
FROM trend WHERE change > 0.25
UNION ALL
SELECT 'Declining demand', sku_id, region, ROUND(change * 100, 1),
       CONCAT('Next 13 weeks ', ROUND(next_13w), '/wk vs ', ROUND(last_13w), '/wk same period last year (', ROUND(change * 100), '%)')
FROM trend WHERE change < -0.25
UNION ALL
SELECT 'Promotion-driven', sku_id, 'ALL', ROUND(uplift, 2), CONCAT('Sells ', ROUND(uplift, 1), 'x more in promotion weeks')
FROM promo WHERE uplift > 1.8
UNION ALL
SELECT 'Weather-sensitive', sku_id, 'ALL', ROUND(corr, 2), CONCAT('Demand follows temperature (median correlation ', ROUND(corr, 2), ')')
FROM weather WHERE corr > 0.5
UNION ALL
SELECT CONCAT('Inventory risk: ', stockout_risk), sku_id, region_id, ROUND(days_of_supply, 1),
       CONCAT(ROUND(days_of_supply), ' days of supply; suggested order ', CAST(suggested_order_qty AS INT), ' units')
FROM {T('forecast.inventory_risk')} WHERE stockout_risk IN ('High', 'Medium')
UNION ALL
SELECT 'Excess stock', sku_id, region_id, ROUND(days_of_supply, 1),
       CONCAT(ROUND(days_of_supply), ' days of supply, ', ROUND(excess_units), ' units above 120 days')
FROM {T('forecast.inventory_risk')} WHERE excess_flag
UNION ALL
SELECT 'Model selection', segment, 'ALL', ROUND(forecast_value_add, 1),
       CONCAT(selected_model, ': WAPE ', ROUND(validation_wape, 1), '% vs seasonal naive ', ROUND(seasonal_naive_wape, 1),
              '% (value add ', ROUND(forecast_value_add, 1), ' pts)', CASE WHEN review_flag <> '' THEN CONCAT(' – review: ', review_flag) ELSE '' END)
FROM {T('ml.model_selection')}
""")
display(sql(f"SELECT insight, COUNT(*) AS items FROM {T('forecast.vw_insights')} GROUP BY insight ORDER BY items DESC"))

# COMMAND ----------

# MAGIC %md ## B1. Groups and least-privilege grants
# MAGIC Create the 5 groups once (workspace admin): **Settings → Identity and access → Groups → Add group**:
# MAGIC `demand_admin`, `data_engineering`, `demand_planners`, `business_analysts`, `management`. Add yourself to `demand_admin`.
# MAGIC If a group does not exist yet, its grants are reported as skipped (re-run after creating it).

# COMMAND ----------

C = f"`{CATALOG}`"
GRANTS = {
    "demand_admin":      [f"GRANT ALL PRIVILEGES ON CATALOG {C} TO `demand_admin`"],
    "data_engineering":  [f"GRANT USE CATALOG ON CATALOG {C} TO `data_engineering`"] +
                         [f"GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA {C}.{s} TO `data_engineering`"
                          for s in ("raw", "bronze", "silver", "gold", "features", "monitoring", "audit")] +
                         [f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {C}.raw.source_files TO `data_engineering`"],
    "demand_planners":   [f"GRANT USE CATALOG ON CATALOG {C} TO `demand_planners`"] +
                         [f"GRANT USE SCHEMA, SELECT ON SCHEMA {C}.{s} TO `demand_planners`" for s in ("gold", "forecast")],
    "business_analysts": [f"GRANT USE CATALOG ON CATALOG {C} TO `business_analysts`",
                          f"GRANT USE SCHEMA, SELECT ON SCHEMA {C}.gold TO `business_analysts`"],
    "management":        [f"GRANT USE CATALOG ON CATALOG {C} TO `management`",
                          f"GRANT USE SCHEMA ON SCHEMA {C}.forecast TO `management`",
                          f"GRANT SELECT ON VIEW {C}.forecast.vw_kpis TO `management`",
                          f"GRANT SELECT ON VIEW {C}.forecast.vw_overview TO `management`"],
}
log = []
for group, statements in GRANTS.items():
    for s in statements:
        try:
            sql(s); log.append((group, s, "applied", ""))
        except Exception as e:
            log.append((group, s, "skipped", str(e).split("\n")[0][:150]))
grant_log = pd.DataFrame(log, columns=["group", "statement", "status", "message"])
display(grant_log)

# COMMAND ----------

# MAGIC %md ## B2. Row-level security + column masking (dynamic view)
# MAGIC * a small mapping table says which group may see which region
# MAGIC * `vw_forecast_secure` returns only rows for regions of the caller's groups (admins see all)
# MAGIC * the accuracy column is masked (NULL) for everyone except admins and management
# MAGIC
# MAGIC Native alternative on tables: `ALTER TABLE ... SET ROW FILTER fn ON (region)` and
# MAGIC `ALTER TABLE ... ALTER COLUMN c SET MASK fn` with SQL UDFs – same idea, enforced on the table itself.

# COMMAND ----------

security_note = "created"
try:
    sql(f"CREATE TABLE IF NOT EXISTS {T('audit.region_access')} (group_name STRING, region_id STRING)")
    sql(f"""INSERT OVERWRITE {T('audit.region_access')} VALUES
            ('demand_planners', 'IN-N'), ('demand_planners', 'IN-S'), ('demand_planners', 'UK-ENG'),
            ('demand_planners', 'UK-SCT'), ('demand_planners', 'BR-SE'), ('demand_planners', 'BR-S')""")
    sql(f"""
    CREATE OR REPLACE VIEW {T('forecast.vw_forecast_secure')} AS
    SELECT f.sku_id, f.category, f.brand, f.region, f.channel, f.forecast_week, f.p10, f.p50, f.p90, f.selected_model,
           CASE WHEN is_account_group_member('demand_admin') OR is_account_group_member('management')
                THEN f.validation_wape END AS validation_wape                                 -- column mask
    FROM {T('forecast.weekly_demand_forecast')} f
    WHERE is_account_group_member('demand_admin')                                             -- row filter
       OR EXISTS (SELECT 1 FROM {T('audit.region_access')} a
                  WHERE a.region_id = f.region AND is_account_group_member(a.group_name))""")
    visible = sql(f"SELECT COUNT(*) AS rows_visible_to_me FROM {T('forecast.vw_forecast_secure')}").first()[0]
    security_note = f"created; you can see {visible:,} rows (0 = you are not in demand_admin or a region group yet)"
except Exception as e:
    security_note = f"not created here: {str(e).split(chr(10))[0][:150]}"
print("Secure view:", security_note)

# COMMAND ----------

# MAGIC %md ## B3. Test: who am I, which groups am I in, what is granted, audit and lineage

# COMMAND ----------

def try_display(label, statement):
    try:
        display(sql(statement))
    except Exception as e:
        print(f"{label}: not available here ({str(e).split(chr(10))[0][:120]})")

try_display("membership", "SELECT current_user() AS me, " + ", ".join(
    f"is_account_group_member('{g}') AS {g}" for g in GRANTS))
try_display("grants", f"SHOW GRANTS ON SCHEMA {C}.forecast")
try_display("audit log", "SELECT event_time, user_identity.email, action_name FROM system.access.audit "
                         "ORDER BY event_time DESC LIMIT 10")
try_display("lineage", f"SELECT source_table_full_name, target_table_full_name FROM system.access.table_lineage "
                       f"WHERE target_table_full_name LIKE '{CATALOG}.forecast.%' LIMIT 10")

# COMMAND ----------

# MAGIC %md ## Checks

# COMMAND ----------

for v in ["vw_overview", "vw_kpis", "vw_accuracy", "vw_model_selection", "vw_inventory", "vw_anomalies", "vw_insights"]:
    assert sql(f"SELECT COUNT(*) FROM {T('forecast.' + v)}").first()[0] > 0, f"{v} is empty"
k = sql(f"SELECT * FROM {T('forecast.vw_kpis')}").first()
assert k.wape_pct is not None and k.active_skus > 0
print(f"PHASE 9 DASHBOARD + GOVERNANCE: PASSED  |  grants applied: {(grant_log.status == 'applied').sum()}/{len(grant_log)}"
      f"  |  secure view: {security_note}")
