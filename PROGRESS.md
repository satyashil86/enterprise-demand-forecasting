# Project progress tracker (condensed 10-phase plan)

Status key: ✅ passed on Databricks · 🟡 code complete and tested locally, awaiting a Databricks run · ⬜ not started

| # | Phase | Notebook(s) | Status | Evidence |
|---|---|---|---|---|
| 1 | Setup + synthetic data | `00_setup`, `01_synthetic_data` | ✅ | Phase 0 checks passed; Phase 1 ran with no errors (24-Nov-2026). Locally: 64/64 data checks. |
| 2 | Ingestion → Bronze (Auto Loader, COPY INTO) | `02_raw_ingestion` | ✅ | `PHASE 2 ACCEPTANCE: PASSED (55 checks)` on Databricks (25-Nov-2026). |
| 3 | Silver: data quality, quarantine, MERGE, SCD1/SCD2 (+ Bronze append-only, time travel, clustering) | `03_silver_cleansing` | 🟡 | 17 local PySpark tests (ANSI mode) pass against the real synthetic batches. Needs a Databricks run → `PHASE 3 ACCEPTANCE: PASSED`. |
| 4 | Gold (star schema) + EDA | `04_gold_model`, `05_eda` | ⬜ | |
| 5 | Outliers + feature engineering (leakage-safe) | `06_outliers`, `07_features` | ⬜ | |
| 6 | Models: Seasonal Naive, AutoARIMA, XGBoost/LightGBM, (RF), TimeGPT (optional) | `08_models` | ⬜ | |
| 7 | Backtesting, WAPE model selection, 104-week forecast (P10/P50/P90) | `09_backtest_select`, `10_forecast` | ⬜ | |
| 8 | Inventory risk + monitoring | `11_inventory`, `12_monitoring` | ⬜ | |
| 9 | Dashboard (Databricks AI/BI, 5 pages) + Unity Catalog governance | `13_governance` | ⬜ | Needs `demand_planning_dashboard.html` as the reference |
| 10 | Orchestration (Lakeflow Job), CI/CD (Asset Bundles + GitHub Actions), Azure essentials | `databricks.yml`, `.github/` | 🟡 | Bundle and job skeleton (tasks t00–t03), CI runs the tests |

## Phase gate for 3
1. Copy the new files into `C:\demand-forecasting-platform`, then run `git add .`, `git commit`, `git push`.
2. In the Databricks Git folder, discard any run-only changes, then click **Pull**.
3. Run `src/03_silver_cleansing` (env=dev, mode=run). It should end with `PHASE 3 ACCEPTANCE: PASSED`.

## Decisions
- **Demand target:** `orders.ordered_quantity` (unconstrained). Shipments are capped by stock.
- **Silver is a curated contract:** Bronze evolves automatically, Silver only through a change to `dfp/silver.py: SPECS`.
- **Incremental Silver:** a high-watermark on Bronze `_ingested_at` plus idempotent MERGE (no streaming foreachBatch, so it runs reliably on serverless).
- **Keying spikes (×50 / ×10)** stay in Silver; they are separated from genuine spikes in the outlier phase.
- **Late-arriving dimension:** facts with an unknown SKU are quarantined. Re-processing them after the master arrives is a planned improvement.

## Open items
- Nixtla TimeGPT: a paid API, and Free Edition has limited outbound access. Check before the model phase.
- RBAC groups: Free Edition has no account console. Check workspace-local groups in phase 9.
