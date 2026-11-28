# Phases 6–10 in one page

**08_models_backtest:** rolling-origin backtest (3 origins, horizons 1–52) for Seasonal Naive, MSTL + AutoARIMA,
LightGBM, XGBoost and Random Forest (TimeGPT optional). Models are compared on exactly the same rows. Metrics are WAPE,
bias, MAE and RMSE, by model, horizon, category and segment. The winner per category × demand pattern is the lowest
WAPE with |bias| ≤ 20%; "unstable" is flagged when WAPE jumps between origins; FVA = naive WAPE − winner WAPE.
Tables: `ml.backtest_results`, `ml.model_comparison`, `ml.model_selection`, `ml.model_registry`.

**09_forecast:** the future grid uses only known information (calendar, approved promotions, planned prices, normal
temperature). Winners are retrained on all history; each series gets its segment's winner, or LightGBM as the
fallback. P10/P90 come from backtest error quantiles (calibrated on the early origins, coverage checked on the last
one). Output: `forecast.weekly_demand_forecast`, with 104 rows per active series.

**10_inventory_monitoring:** projected stock (on hand + in transit + open POs − cumulative forecast); days of supply;
safety stock = 1.65 × σ × √lead time; reorder point; High risk = stockout within the lead time; Medium = position
below the reorder point; excess = more than 120 days; order quantity rounded up to the MOQ. Health checks cover data
freshness, quarantine %, pipeline status, WAPE drift, forecast coverage and business risks, appended to
`monitoring.health_checks`.

**11_dashboard_governance:** 7 views for the 5 dashboard pages (KPIs, accuracy, inventory, anomalies, insights).
Least-privilege GRANTs for 5 groups. A dynamic view gives row-level security (regions per group) and masks a column
(accuracy only for admin/management). It also shows membership, grants, audit and lineage.

**Orchestration and CI/CD:** `resources/pipeline_job.yml` (12 tasks, dependencies, retries, paused weekly schedule)
and `.github/workflows/deploy.yml` (bundle validate and deploy; dev on push to main, prod on manual approval).
Azure: `docs/azure_integration.md`.

Interview points: why a rolling origin instead of a random split; why the same rows for every model; WAPE vs MAPE
(MAPE explodes on zero weeks); bias sign; FVA; global vs local models; direct multi-horizon features; empirical
prediction intervals; safety stock formula; dynamic views vs dashboard-only security; service principals for prod.
