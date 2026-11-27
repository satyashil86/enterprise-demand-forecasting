# Project progress tracker (condensed 10-phase plan)

Status key: ✅ passed on Databricks · 🟡 code complete and tested locally, awaiting a Databricks run · ⬜ not started

| # | Phase | Notebook(s) | Status | Evidence |
|---|---|---|---|---|
| 1 | Setup + synthetic data | `00_setup`, `01_synthetic_data` | ✅ | Phase 0 checks passed; Phase 1 ran with no errors (24-Nov-2026). Locally: 64/64 data checks. |
| 2 | Ingestion → Bronze (Auto Loader, COPY INTO) | `02_raw_ingestion` | ✅ | `PHASE 2 ACCEPTANCE: PASSED (55 checks)` on Databricks (25-Nov-2026). |
| 3 | Silver: data quality, quarantine, MERGE, SCD1/SCD2 (+ Bronze append-only, time travel, clustering) | `03_silver_cleansing` | ✅ | `PHASE 3 ACCEPTANCE: PASSED (56 checks)` on Databricks (26-Nov-2026). |
| 4 | Gold (star schema) + EDA, simplified plain PySpark | `04_gold`, `05_eda` | ✅ | `PHASE 4 GOLD: PASSED` on Databricks (27-Nov-2026): 93,299 rows, 645 series, 2023-11-27 → 2026-11-16. |
| 5 | Outliers + feature engineering (leakage-safe) | `06_outliers`, `07_features` | 🟡 | Local Spark run: 15 weeks treated as errors, 14 of them real (14/16 injected ×10 typos caught), 0 genuine spikes changed; 45 features, all 4 leakage checks pass. Needs a Databricks run. |
| 6 | Models: Seasonal Naive, AutoARIMA, XGBoost/LightGBM, (RF), TimeGPT (optional) | `08_models` | ⬜ | |
| 7 | Backtesting, WAPE model selection, 104-week forecast (P10/P50/P90) | `09_backtest_select`, `10_forecast` | ⬜ | |
| 8 | Inventory risk + monitoring | `11_inventory`, `12_monitoring` | ⬜ | |
| 9 | Dashboard (Databricks AI/BI, 5 pages) + Unity Catalog governance | `13_governance` | ⬜ | Needs `demand_planning_dashboard.html` as the reference |
| 10 | Orchestration (Lakeflow Job), CI/CD (Asset Bundles + GitHub Actions), Azure essentials | `databricks.yml`, `.github/` | 🟡 | Bundle and job skeleton (tasks t00–t05), CI runs the tests |

## Phase gate for 5
1. Copy the new files into `C:\demand-forecasting-platform`, then run `git add -A`, `git commit`, `git push`.
2. In Databricks, discard any run-only changes, then click **Pull**.
3. Run `src/06_outliers`. It should end with `PHASE 5 OUTLIERS: PASSED`.
4. Run `src/07_features`. It should end with `PHASE 5 FEATURES: PASSED`.

## Style from Phase 4 on
Notebooks are written in plain PySpark: each step sits in its own cell with a short explanation and simple `assert` checks. There are no new helper libraries. Phases 0–3 are kept as they are, because they have already passed.

## Decisions
- **Demand target:** `orders.ordered_quantity` (unconstrained). Shipments are capped by stock.
- **Silver is a curated contract:** Bronze evolves automatically, Silver only through a change to `dfp/silver.py: SPECS`.
- **Incremental Silver:** a high-watermark on Bronze `_ingested_at` plus idempotent MERGE (no streaming foreachBatch, so it runs reliably on serverless).
- **Keying spikes (×50 / ×10)** stay in Silver; they are separated from genuine spikes in the outlier phase.
- **Late-arriving dimension:** facts with an unknown SKU are quarantined. Re-processing them after the master arrives is a planned improvement.

## Open items
- Nixtla TimeGPT: a paid API, and Free Edition has limited outbound access. Check before the model phase.
- RBAC groups: Free Edition has no account console. Check workspace-local groups in phase 9.
