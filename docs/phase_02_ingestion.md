# Phase 2: raw ingestion into Bronze

## Flow
```
staging/batch_00N/<source>/*      (made by 01_synthetic_data; stands in for source systems)
        │  release_batch()   (one delivery arrives)
        ▼
landing/<source>/batch_00N/*      (Auto Loader / COPY INTO input)
        │
        ├─ Auto Loader (cloudFiles, availableNow) ─► bronze.{sales,pos,orders,promotions,pricing,weather,inventory}_raw
        └─ COPY INTO                              ─► bronze.{products,stores,regions,suppliers,calendar}_raw
```
Checkpoints and schema locations: `/Volumes/<catalog>/raw/checkpoints/bronze/<source>/{_checkpoint,_schema}`.

## Requirement coverage
| Requirement | How it is met |
|---|---|
| Full batch | COPY INTO for master data; the first Auto Loader run with `includeExistingFiles=true` |
| Incremental | Auto Loader's checkpoint tracks processed files; COPY INTO load history |
| Streaming / micro-batch | Structured Streaming with `trigger(availableNow=True)`, `maxFilesPerTrigger=50` (several micro-batches per run). Serverless only allows availableNow, so scheduled micro-batches come from the job schedule (`resources/pipeline_job.yml`, paused by default) |
| Schema inference + explicit management | Auto Loader infers into the schema location. `config/schema_config.yml` is the contract. Drift (columns outside the contract, missing columns) goes to `monitoring.schema_change_log` |
| Schema evolution | `cloudFiles.schemaEvolutionMode=addNewColumns` plus Delta `mergeSchema`. The stream stops once on new columns and is restarted automatically (`schema_restarts`). COPY INTO uses `mergeSchema` |
| Rescued data | `_rescued_data` column (Auto Loader default when it infers the schema) |
| Source filename / ingestion timestamp | `_source_file`, `_source_file_modified_at`, `_ingested_at`, `_ingestion_run_id`, `_batch_id`, `_source_system` |
| Duplicate prevention / idempotency | Each file is processed once (checkpoint and COPY INTO history). The demo proves 0 rows on a re-run and on re-delivered files |
| Late-arriving data | Late records are accepted whenever their file arrives and keep their original event date; Bronze never filters by event date |
| Watermarking | Applied in Silver (Phase 4), where record-level dedup and merge of late or restated records happen. Bronze must keep every delivered record for replay and audit |

## Why Bronze keeps duplicates
Bronze is the replayable record of what each source sent. Removing record-level duplicates here would make
reconciliation with the source impossible. Silver dedupes on business keys, and restatements (same key,
later file) win there.

## Logs written
* `audit.ingestion_log`: one row per source per run (files, rows, batches, method, new columns)
* `monitoring.schema_change_log`: columns added, columns outside the contract, contract columns missing
* `monitoring.data_quality_results`: Phase 2 acceptance checks
* `audit.pipeline_run_log`: run status and duration
