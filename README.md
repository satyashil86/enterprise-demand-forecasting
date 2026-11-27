# Enterprise AI Demand Forecasting Platform

Weekly, 104-week demand forecasting for five CPG categories (Food, Personal Care, Beauty & Wellbeing,
Ice Cream, Home Care). It runs on Databricks with PySpark, a Delta medallion architecture and Unity Catalog,
and evaluates several models (TimeGPT, Random Forest, XGBoost, LightGBM, SARIMA, Seasonal Naive).
The winning model is selected by WAPE in rolling-origin backtests.

> ⚠️ **All data in this repository is SYNTHETIC.** Brands, products, stores, customers and suppliers are invented.

The project is built phase by phase. See [PROGRESS.md](PROGRESS.md) for status and phase gates.

## Repository layout

```
databricks.yml              Asset Bundle (dev/prod targets)
resources/                  Job definitions (serverless; Free Edition compatible)
config/                     environment, source, schema, model and forecast configuration
src/
  00_setup.py               Unity Catalog: catalog, 9 schemas, 4 volumes, audit tables
  01_synthetic_data.py      Synthetic SAP-style data: 12 sources, 2 delivery batches
  02_raw_ingestion.py       Landing → Bronze: Auto Loader + COPY INTO, idempotency demo
  03_silver_cleansing.py    Bronze → Silver: validation, quarantine, MERGE, SCD1/SCD2
  04_gold.py                Silver → Gold: weekly star schema + weekly_demand_features (plain PySpark)
  05_eda.py                 EDA: demand classes, seasonality, promo/price/weather effects, lifecycle, charts
  dfp/                      Shared library (pure Python where possible, unit-testable)
    config.py  uc.py  audit.py  synthetic.py  synthetic_validation.py  ingestion.py  silver.py
tests/                      pytest suite (runs locally and in CI)
docs/                       Phase design notes
.github/workflows/ci.yml    Unit tests on every push / PR
```

## Quick start (Databricks Free Edition)

1. Push this folder to a GitHub repo. In Databricks: **Workspace → Create → Git folder** and paste the repo URL.
   (Or use the CLI: `databricks bundle deploy -t dev`, then `databricks bundle run -t dev dfp_foundation`.)
2. Open `src/00_setup.py`, attach **Serverless** compute and run all cells (env = `dev`).
3. Open `src/01_synthetic_data.py` and run all cells. The data lands in
   `/Volumes/demand_planning/raw/source_files/staging/batch_00{1,2}/`.

## Local development

```bash
pip install -r requirements-dev.txt
pytest            # ~4 min; generates the dataset and runs Silver logic on local Spark (needs Java)
```

## Unity Catalog layout

| Schema | Purpose |
|---|---|
| raw | Landing files (volume `source_files`), checkpoints (volume `checkpoints`) |
| bronze | Raw Delta tables, append-only, with source metadata |
| silver | Cleansed, deduplicated, SCD-managed data; quarantine tables |
| gold | Weekly facts and dimensions |
| features | Leakage-safe model features |
| ml | Backtests, comparison, selection, registry metadata (volume `model_artifacts`) |
| forecast | Published 104-week forecasts (P10/P50/P90) |
| monitoring | Data quality, profiling, model and pipeline monitoring |
| audit | Run and ingestion logs, outlier and selection audit (volume `config`) |

## Synthetic dataset (Phase 1): what's in it

* **Hierarchy:** 5 categories → 10 subcategories → 15 brands → 20 products → 40 SKUs (`small`). `medium` doubles the SKUs.
* **Geography / channels:** India, United Kingdom, Brazil (6 regions, including the southern hemisphere) × Modern Trade, General Trade, E-Commerce; 24 stores.
* **History:** 156 weeks (2023-11-27 → 2026-11-16). The calendar, approved promotions and planned prices extend 104 weeks ahead.
* **Demand patterns:** smooth, seasonal, intermittent, erratic, promotion-driven, price-sensitive, weather-sensitive, new product (including a cold-start SKU), end-of-life.
* **Causal drivers:** temperature (heatwaves), country festivals (Diwali, Holi, Christmas, Easter, Carnival, Black Friday), trade promotions with post-promo dips, list-price inflation, competitor prices, e-commerce growth, fortnightly General Trade ordering in India.
* **Flow:** POS sell-out → retailer orders (unconstrained demand) → DC inventory simulation → confirmed orders → billing.
* **Injected issues** (all logged in `_synthetic_ground_truth/injected_issues.csv`): exact and cross-batch duplicates, restatements, late arrivals, missing store-days, invalid SKUs, nulls, negative quantities, ×50/×10 keying errors, mixed date formats, weather sensor errors, price typos, SCD1/SCD2 master changes, a new SKU, new columns in batch 2.

## Cost notes

Phases 0–1 use only serverless compute and Unity Catalog volumes; there are no paid APIs. Nixtla TimeGPT
(Phase 8) is a paid external API. It stays disabled (`nixtla_enabled: false`) in dev until it is configured,
and its API usage will be capped and logged.
