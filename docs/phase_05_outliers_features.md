# Phase 5: Outliers + features

**06_outliers.py**
- Detect: a spike = robust z (MAD) > 4 AND above the IQR fence AND > 3 × rolling median of order weeks (±4).
- Explain: if the order is > 2.5 × POS of the 3 surrounding weeks, it's a suspected data error. Otherwise the
  reason is recorded as promotion, festival/holiday, extreme weather, or confirmed by POS.
- Treat: only data errors are replaced with the rolling median; raw and clean values are kept side by side.
- Price rule: a shelf price above 2 × the list price is a typo, so the list price is used instead.
- Outputs: `gold.weekly_demand_clean`, `monitoring.outlier_audit`.
- Synthetic validation: 14/16 injected ×10 typos caught, 15 weeks treated, 0 genuine spikes changed.

**07_features.py**
- 45 features: lags (1–104), rolling mean/std (past only), seasonality, promotion, price, POS lags, weather.
- Leakage rules: only `F.lag` and `rowsBetween(-n, -1)`; this week's POS, stock and demand are never features.
- `features.feature_catalog` records for each feature whether it's known for future weeks (calendar, planned
  promo and price) or must come from the past (lags, rolling, POS).
- Checks: no forbidden columns, lag_1 = last week, roll_mean_4 excludes this week, no feature with |corr| > 0.99.

Interview points: robust z vs z-score (outliers inflate the std), why use POS to validate orders, why rolling
windows must end at t-1, and why "known in future" matters for 104-week forecasts.
