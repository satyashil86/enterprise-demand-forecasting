# Phase 3: Silver (data quality layer)

## Flow per source (`dfp/silver.py`)
```
bronze.<src>_raw ─(rows with _ingested_at > watermark)─► standardise ─► validate ─┬─ invalid ─► silver.quarantine_records
                                                                                  └─ valid ─► dedupe (latest wins) ─► MERGE ─► silver.<table>
                                                                                                            watermark ─► audit.silver_watermark
```
Order: regions → suppliers → calendar → stores → products (SCD2) → facts (validated against the masters).

## Silver tables
| Table | Type | Key |
|---|---|---|
| silver.pos, orders, sales, promotions, pricing, weather, inventory | fact, MERGE upsert, liquid clustered on (date, sku) | business key |
| silver.products_scd2 | SCD Type 2 (`product_sk`, `effective_from/to`, `is_current`) | sku_id + effective_from |
| silver.stores, regions, suppliers, calendar | SCD Type 1 | business key |
| silver.quarantine_records | rejected rows: rules, reason, raw JSON, lineage | source + record_id |
| audit.silver_watermark | last processed Bronze `_ingested_at` per source | source |

## Interview notes
- **Why quarantine instead of filtering?** Rejected rows stay auditable, can be fixed and replayed, and the
  reconciliation `bronze = valid + quarantined` proves nothing was lost.
- **Why "latest ingested wins"?** Sources re-send corrections with the same key. Ordering by ingestion time
  (not event time) makes the newest correction win and turns exact re-sends into no-ops.
- **Idempotent MERGE:** `WHEN MATCHED AND s._ingested_at >= t._ingested_at AND s._record_hash <> t._record_hash`.
  Re-running the same input changes nothing.
- **High-watermark vs streaming:** both are incremental. A watermark on `_ingested_at` is simple, restartable and
  runs on serverless. A crash between MERGE and the watermark update only causes a safe re-merge.
- **SCD2 in one MERGE (the "union trick"):** each changed key is staged twice. The row with
  `merge_key = key` expires the current version; the row with `merge_key = NULL` never matches, so it is
  inserted as the new version. Changes dated before the current version are ignored, so a replay can't roll
  history back. Type-1 attributes (sustainability_tag) are updated in place without creating a version.
- **ANSI-safe casting:** serverless runs with ANSI SQL on, so `CAST('abc' AS DOUBLE)` would fail the job.
  `try_cast` / `try_to_timestamp` return NULL instead, and a rule then quarantines the row.
- **Bronze append-only:** `delta.appendOnly = true` blocks UPDATE/DELETE, so Bronze stays a replayable audit trail.
- **Liquid clustering** (`CLUSTER BY (date, sku_id)`) replaces partitioning and Z-ORDER for these tables. `OPTIMIZE` applies it.
