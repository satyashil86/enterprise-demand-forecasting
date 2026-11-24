# Project progress tracker

Status key: ✅ done & tested locally · 🟡 code complete, awaiting Databricks run · ⬜ not started

| Phase | Notebook / module | Status | Acceptance evidence |
|---|---|---|---|
| 0 Setup & Unity Catalog | `src/00_setup.py`, `dfp/config.py`, `dfp/uc.py`, `dfp/audit.py` | ✅ | Run on Databricks Free Edition (24-Nov-2026): no missing schemas, volumes or tables; volume write test passed. |
| 1 Synthetic SAP-style data | `src/01_synthetic_data.py`, `dfp/synthetic.py`, `dfp/synthetic_validation.py` | ✅ | Run on Databricks with no errors reported (24-Nov-2026). Locally: 64/64 acceptance checks, 12 unit tests. |
| 2 Raw ingestion → Bronze (Auto Loader, COPY INTO, replay) | `src/02_raw_ingestion.py`, `dfp/ingestion.py` | 🟡 | 16 unit tests pass locally (release, options, COPY INTO SQL, drift, catalog resolution). Needs a Databricks run of `demo` mode, which should end with `PHASE 2 ACCEPTANCE: PASSED`. |
| 3 Bronze processing (replay, audit views, OPTIMIZE) | `03_bronze_processing.py` | ⬜ | Bronze tables themselves are created in Phase 2. |
| 4 Silver (cleansing, SCD1/2, quarantine) | `04_silver_cleansing.py` | ⬜ | |
| 5 EDA | `05_eda_analysis.py` | ⬜ | |
| 6 Outliers | `06_outlier_detection.py` | ⬜ | |
| 7 Features (leakage-safe) | `07_feature_engineering.py` | ⬜ | |
| 8 TimeGPT | `08_timegpt_model.py` | ⬜ | |
| 9–12 RF / XGBoost / LightGBM / SARIMA / Seasonal Naive | `09`–`12` | ⬜ | |
| 13–14 Backtesting & WAPE selection | `13`, `14` | ⬜ | |
| 15–16 104-week forecast & evaluation | `15`, `16` | ⬜ | |
| 17 Inventory analytics | `17_inventory_analytics.py` | ⬜ | |
| 18 Monitoring | `18_monitoring.py` | ⬜ | |
| Dashboard (5 pages) | `dashboard/frontend` | ⬜ | Needs the reference `demand_planning_dashboard.html` (not uploaded yet). |
| Governance / RBAC | | ⬜ | |
| Lakeflow Jobs & CI/CD | `databricks.yml`, `resources/`, `.github/workflows` | 🟡 | Bundle skeleton + 2-task foundation job; CI runs pytest. |
| Azure integration | | ⬜ | Only after Databricks validation. |

## Phase gate for 2 (needs a Databricks run)

1. In the Databricks Git folder, click **Pull** to get the new code.
2. Run `src/02_raw_ingestion.py` with env=dev, mode=demo. It should end with `PHASE 2 ACCEPTANCE: PASSED`.

## Phase gate for 0 + 1 (passed)

1. Run `src/00_setup.py` (env=dev). It should end with `PHASE 0 ACCEPTANCE: PASSED`.
2. Run `src/01_synthetic_data.py` (env=dev, overwrite=false). It should end with `PHASE 1 ACCEPTANCE: PASSED (64 checks)`.
3. Check: `SELECT * FROM demand_planning.audit.pipeline_run_log` shows two `SUCCEEDED` rows.

## Open items and risks

- **Catalog creation on Free Edition:** the setup tries `CREATE CATALOG demand_planning`. If that's not allowed, it falls back to the `workspace` catalog (dev/test only) and says so.
- **Nixtla TimeGPT:** it's a paid external API, and Free Edition limits outbound internet to trusted domains. Reaching `api.nixtla.io` from serverless has to be checked at the start of Phase 8. If it can't be reached, TimeGPT runs from CI or a local runner, and the results are written back to Delta.
- **Groups / RBAC:** Free Edition has no account console. Phase 17 will check whether workspace-local groups can be created. If not, the grant scripts are delivered and tested on Azure Databricks.
- **Demand target:** unconstrained demand = `orders.ordered_quantity` (sell-in orders). Shipments are capped by stock (synthetic fill rate ≈ 97%), so they aren't used.
