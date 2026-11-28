# Project progress tracker (10-phase plan)

Status key: ✅ passed on Databricks · 🟡 built and passed end-to-end on local Spark with the synthetic data, needs a Databricks run · ⬜ not started

| # | Phase | Notebook(s) | Status | Evidence |
|---|---|---|---|---|
| 1 | Setup + synthetic data | `00_setup`, `01_synthetic_data` | ✅ | Phase 0 checks passed; Phase 1 ran with no errors (24-Nov-2026). |
| 2 | Ingestion → Bronze (Auto Loader, COPY INTO) | `02_raw_ingestion` | ✅ | `PHASE 2 ACCEPTANCE: PASSED (55 checks)` (25-Nov-2026). |
| 3 | Silver: quality, quarantine, MERGE, SCD1/SCD2 | `03_silver_cleansing` | ✅ | `PHASE 3 ACCEPTANCE: PASSED (56 checks)` (26-Nov-2026). |
| 4 | Gold star schema + EDA | `04_gold`, `05_eda` | ✅ | `PHASE 4 GOLD: PASSED`: 93,299 rows, 645 series (27-Nov-2026). |
| 5 | Outliers + features | `06_outliers`, `07_features` | ✅ | `PHASE 5 FEATURES: PASSED`: 45 features, leakage checks pass (28-Nov-2026). |
| 6 | Models + rolling backtest + WAPE selection | `08_models_backtest` | 🟡 | Local: LightGBM 34.7% WAPE, XGBoost 34.7%, RF 39.4%, MSTL-ARIMA 47.0%, Seasonal Naive 51.4% (FVA ≈ 17 pts); winners per segment: 9 LightGBM, 7 XGBoost, 3 RF, 1 SN. |
| 7 | 104-week forecast, P10/P50/P90 | `09_forecast` | 🟡 | Local: 594 active series × 104 weeks; P10–P90 coverage on the held-out origin 81% (target 80%). |
| 8 | Inventory risk + monitoring | `10_inventory_monitoring` | 🟡 | Local: 222 SKU-regions (10 high, 52 medium stockout risk, 14 excess); health-check table. |
| 9 | Dashboard views + governance + dashboard | `11_dashboard_governance`, `12_create_dashboard`, `dashboard/` | 🟡 | 11: `PHASE 9 ... PASSED` on Databricks (28-Nov-2026); grants pending group creation. 12: dashboard as code (6 pages, 34 widgets, 11 datasets, global filters); all 11 queries tested locally, needs a Databricks run. |
| 10 | Orchestration, CI/CD, Azure | `resources/pipeline_job.yml`, `.github/workflows/deploy.yml`, `docs/azure_integration.md` | 🟡 | One job with 12 tasks, dependencies and retries; deploy workflow (dev on push, prod with approval); Azure guide. |

## How to run phases 6–10 on Databricks
1. Copy the new files into `C:\demand-forecasting-platform`, then run `git add -A`, `git commit`, `git push`. In Databricks, discard any run-only changes, then click **Pull**.
2. Run in order: `08_models_backtest` (≈10–20 min), `09_forecast` (≈5–10 min), `10_inventory_monitoring`, `11_dashboard_governance`.
3. Create the 5 groups (Settings → Identity and access → Groups), add yourself to `demand_admin`, and re-run notebook 11.
4. Build the dashboard following `docs/dashboard_guide.md`.
5. Optional: Jobs & Pipelines → create the job from `resources/pipeline_job.yml` (or `databricks bundle deploy`), then GitHub secrets for `deploy.yml`.

## Decisions
- **Demand target:** `orders.ordered_quantity` (unconstrained), cleaned in notebook 06 (`actual_demand_clean`).
- **Silver is a curated contract**; Bronze evolves automatically.
- **Outliers:** an order spike is a data error only if POS over the surrounding 3 weeks doesn't support it.
- **Long horizons:** one global ML model per algorithm; features = level at the origin + planned drivers for the target week + the same week 52/104 weeks earlier. No lags after the origin, so the backtest and production use the same code.
- **ARIMA:** MSTL (52-week season) + AutoARIMA. Plain SARIMA with m=52 was 5× slower.
- **Backtest:** 3 origins (52, 39 and 26 weeks before the end), horizons 1–52. A 104-week backtest needs ≥ 4 years of history.
- **Intervals:** empirical quantiles of backtest errors per model and horizon; beyond 52 weeks widened by 50% (assumption).
- **Selection:** per category × demand pattern, lowest WAPE with |bias| ≤ 20%; fallback LightGBM.
- **Governance:** dynamic views (row filter and column mask); base tables are not masked, so pipelines keep running.

## Style from Phase 4 on
Plain PySpark notebooks with short explanations and `assert` checks. One small shared helper (`dfp/forecasting.py`) is used by notebooks 08 and 09.

## Open items
- TimeGPT: a full 6th model. It runs on all series with ≥ 52 weeks of history, one API call per origin (≈3 calls for the backtest, 1 for the forecast), with planned promotion/price/calendar as `X_df`, `timegpt-1-long-horizon` and native 80% intervals. Needs the `nixtla_api_key` secret. Databricks Free Edition reached the Nixtla API (a 401 came back for the first key, so outbound access works).
- Workspace groups must be created by an admin before the GRANTs apply.
