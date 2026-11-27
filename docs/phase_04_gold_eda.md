# Phase 4: Gold + EDA (simple version)

**04_gold.py** builds weekly tables with plain PySpark:
- dims: `dim_product` (current SCD2 rows), `dim_region`, `dim_calendar` (country × week)
- facts: `fact_weekly_demand` (zero-filled), `fact_weekly_pos`, `fact_weekly_promotions` (exploded to weeks),
  `fact_weekly_price` (as-of price, forward-filled), `fact_weekly_weather`, `fact_weekly_inventory`
- `weekly_demand_features`: everything joined at SKU × region × channel × week (history only; future weeks are
  added in the forecasting phase)
- 5 `assert` checks: unique grain, totals equal Silver, no missing weeks, Monday weeks, full price/weather/calendar coverage

**05_eda.py** answers: trend, seasonality, demand pattern (ADI / CV²), promo uplift, relative-price effect,
temperature effect, POS vs orders. It saves `monitoring.eda_series_profile`.

PySpark ideas to remember: `date_trunc('week')`, `explode(sequence(...))`, `last(..., ignorenulls=True)` over a
window (forward fill), `max_by`, `try_divide` (ANSI-safe), left joins to keep every week.
