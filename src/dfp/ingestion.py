"""Metadata-driven raw ingestion: landing files -> Bronze Delta tables (Phase 2).

Two ingestion methods, chosen per source in config/source_config.yml:

* Auto Loader (cloudFiles) for transactional / incremental sources. It tracks processed files in
  its checkpoint, so re-running never re-ingests a file (exactly-once at file level). Schema is
  inferred into a schema location, new columns are added (schemaEvolutionMode=addNewColumns), and
  values that don't fit go to `_rescued_data`. The trigger is `availableNow`: process everything
  new, then stop. That is also the only trigger serverless compute supports, so "scheduled
  micro-batches" are a Lakeflow Job running this on a schedule.
* COPY INTO for small master-data files. COPY INTO records which files it has loaded and skips them
  on re-runs. It is the simplest idempotent batch pattern.

Bronze keeps source records exactly as delivered (all columns as strings for CSV/JSON), including
duplicates, bad values and late records. Record-level dedup, typing and validation happen in Silver.
Every row gets lineage columns: source file, file modification time, ingestion timestamp, run id,
delivery batch id and source system.

Pure-Python helpers (file release, options, SQL generation, drift) are unit-tested locally; the
Spark functions need Databricks (cloudFiles and COPY INTO don't exist in open-source Spark).
"""
from __future__ import annotations

import os
import re
import shutil
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .config import ProjectConfig

SOURCE_SYSTEM = "SYNTHETIC_SAP"
METADATA_COLUMNS = ["_source_file", "_source_file_modified_at", "_ingested_at",
                    "_ingestion_run_id", "_batch_id", "_source_system"]
BATCH_RE = re.compile(r"^batch_\d{3}$")
BATCH_IN_PATH_REGEX = r"/(batch_[0-9]+)/"

SCHEMA_CHANGE_LOG_DDL = """
    run_id STRING NOT NULL,
    table_name STRING,
    column_name STRING,
    change_type STRING,
    detected_at TIMESTAMP,
    details STRING
"""


# --------------------------------------------------------------------------------------
# File release (simulates files arriving from source systems)
# --------------------------------------------------------------------------------------

def list_batches(staging_root: str) -> List[str]:
    if not os.path.isdir(staging_root):
        return []
    return sorted(d for d in os.listdir(staging_root) if BATCH_RE.match(d))


def released_batches(landing_root: str) -> List[str]:
    """Batch ids that already exist in the landing zone for at least one source."""
    found = set()
    if os.path.isdir(landing_root):
        for source in os.listdir(landing_root):
            sdir = os.path.join(landing_root, source)
            if os.path.isdir(sdir):
                found.update(d for d in os.listdir(sdir) if BATCH_RE.match(d))
    return sorted(found)


def next_unreleased_batch(staging_root: str, landing_root: str) -> Optional[str]:
    done = set(released_batches(landing_root))
    for b in list_batches(staging_root):
        if b not in done:
            return b
    return None


def plan_release(staging_root: str, landing_root: str, batch_id: str) -> List[Tuple[str, str]]:
    """staging/<batch>/<source>/<file>  ->  landing/<source>/<batch>/<file>"""
    bdir = os.path.join(staging_root, batch_id)
    if not os.path.isdir(bdir):
        raise FileNotFoundError(f"Staged batch not found: {bdir}")
    plan = []
    for source in sorted(os.listdir(bdir)):
        sdir = os.path.join(bdir, source)
        if not os.path.isdir(sdir):
            continue
        for f in sorted(os.listdir(sdir)):
            plan.append((os.path.join(sdir, f), os.path.join(landing_root, source, batch_id, f)))
    return plan


def release_batch(staging_root: str, landing_root: str, batch_id: str) -> int:
    """Copy one staged batch into landing. Re-releasing rewrites the same paths (a re-sent delivery)."""
    plan = plan_release(staging_root, landing_root, batch_id)
    for src, dst in plan:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copyfile(src, dst)
    return len(plan)


# --------------------------------------------------------------------------------------
# Option / SQL builders (pure)
# --------------------------------------------------------------------------------------

def autoloader_options(fmt: str, schema_location: str, max_files_per_trigger: int = 50) -> Dict[str, str]:
    opts = {
        "cloudFiles.format": fmt,
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "cloudFiles.inferColumnTypes": "false",      # Bronze keeps CSV/JSON values as delivered strings
        "cloudFiles.includeExistingFiles": "true",
        "cloudFiles.maxFilesPerTrigger": str(max_files_per_trigger),
        "pathGlobFilter": f"*.{fmt}",
    }
    if fmt == "csv":
        opts["header"] = "true"
    return opts


def copy_into_sql(table: str, path: str, fmt: str, run_id: str) -> str:
    fmt_opts = "'header' = 'true', 'inferSchema' = 'false', 'mergeSchema' = 'true'" if fmt == "csv" \
        else "'inferSchema' = 'false', 'mergeSchema' = 'true'"
    return f"""
COPY INTO {table}
FROM (
  SELECT
    *,
    _metadata.file_path                                        AS _source_file,
    _metadata.file_modification_time                           AS _source_file_modified_at,
    current_timestamp()                                        AS _ingested_at,
    '{run_id}'                                                 AS _ingestion_run_id,
    regexp_extract(_metadata.file_path, '{BATCH_IN_PATH_REGEX}', 1) AS _batch_id,
    '{SOURCE_SYSTEM}'                                          AS _source_system
  FROM '{path}'
)
FILEFORMAT = {fmt.upper()}
PATTERN = 'batch_*/*.{fmt}'
FORMAT_OPTIONS ({fmt_opts})
COPY_OPTIONS ('mergeSchema' = 'true')
""".strip()


def schema_drift(contract_columns: Iterable[str], actual_columns: Iterable[str]) -> Dict[str, List[str]]:
    """Compare delivered columns with the documented contract (config/schema_config.yml)."""
    contract = list(contract_columns)
    actual = [c for c in actual_columns if c not in METADATA_COLUMNS and c != "_rescued_data"]
    return {
        "not_in_contract": [c for c in actual if c not in contract],
        "missing_from_delivery": [c for c in contract if c not in actual],
    }


def is_schema_evolution_error(exc: BaseException) -> bool:
    """Auto Loader stops the stream once when it sees new columns (after recording them in the
    schema location); restarting continues with the evolved schema."""
    text = f"{type(exc).__name__} {exc}"
    return any(k in text for k in ("UnknownFieldException", "UNKNOWN_FIELD_EXCEPTION", "NEW_FIELDS_IN_RECORD",
                                   "NEW_FIELDS_IN_FILE"))


# --------------------------------------------------------------------------------------
# Spark side (Databricks only)
# --------------------------------------------------------------------------------------

def table_columns(spark, table: str) -> Optional[List[str]]:
    if not spark.catalog.tableExists(table.replace("`", "")):
        return None
    return spark.table(table).columns


def _run_stats(spark, table: str, run_id: str) -> Dict[str, object]:
    from pyspark.sql import functions as F
    r = (spark.table(table).where(F.col("_ingestion_run_id") == run_id)
         .agg(F.count(F.lit(1)).alias("rows"), F.countDistinct("_source_file").alias("files"),
              F.array_sort(F.collect_set("_batch_id")).alias("batches"))
         .first())
    return {"rows_ingested": int(r["rows"]), "files_processed": int(r["files"]), "batches": list(r["batches"] or [])}


def ingest_autoloader(spark, cfg: ProjectConfig, source: str, scfg: dict, run_id: str,
                      max_schema_retries: int = 3) -> dict:
    from pyspark.sql import functions as F

    fmt = scfg["format"]
    path = f"{cfg.landing_root}/{source}"
    ckpt = f"{cfg.checkpoint_root}/bronze/{source}"
    table = cfg.table(scfg["bronze_table"])
    before = table_columns(spark, table)
    attempts, schema_restarts = 0, 0
    while True:
        attempts += 1
        df = (spark.readStream.format("cloudFiles")
              .options(**autoloader_options(fmt, f"{ckpt}/_schema"))
              .load(path)
              .withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_source_file_modified_at", F.col("_metadata.file_modification_time"))
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_ingestion_run_id", F.lit(run_id))
              .withColumn("_batch_id", F.regexp_extract(F.col("_metadata.file_path"), BATCH_IN_PATH_REGEX, 1))
              .withColumn("_source_system", F.lit(SOURCE_SYSTEM)))
        query = (df.writeStream
                 .format("delta")
                 .option("checkpointLocation", f"{ckpt}/_checkpoint")
                 .option("mergeSchema", "true")
                 .queryName(f"bronze_{source}")
                 .trigger(availableNow=True)
                 .toTable(table))
        try:
            query.awaitTermination()
            break
        except Exception as exc:  # noqa: BLE001
            if is_schema_evolution_error(exc) and schema_restarts < max_schema_retries:
                schema_restarts += 1
                continue
            raise
    after = table_columns(spark, table) or []
    return {"source": source, "method": "autoloader", "table": table, "table_created": before is None,
            "new_columns": [c for c in after if before is not None and c not in before],
            "schema_restarts": schema_restarts, "columns": after, **_run_stats(spark, table, run_id)}


def ingest_copy_into(spark, cfg: ProjectConfig, source: str, scfg: dict, run_id: str) -> dict:
    fmt = scfg["format"]
    path = f"{cfg.landing_root}/{source}"
    table = cfg.table(scfg["bronze_table"])
    before = table_columns(spark, table)
    # schemaless placeholder; COPY INTO + mergeSchema creates the columns on first load
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} COMMENT 'Bronze (raw, append-only) - {source} - SYNTHETIC'")
    spark.sql(copy_into_sql(table, path, fmt, run_id)).collect()
    after = table_columns(spark, table) or []
    return {"source": source, "method": "copy_into", "table": table, "table_created": before is None,
            "new_columns": [c for c in after if before and c not in before],
            "schema_restarts": 0, "columns": after, **_run_stats(spark, table, run_id)}


def ingest_source(spark, cfg: ProjectConfig, source: str, run_id: str) -> dict:
    scfg = cfg.sources[source]
    path = f"{cfg.landing_root}/{source}"
    if not os.path.isdir(path):
        return {"source": source, "method": scfg["ingestion_method"], "table": cfg.table(scfg["bronze_table"]),
                "rows_ingested": 0, "files_processed": 0, "batches": [], "new_columns": [], "schema_restarts": 0,
                "columns": table_columns(spark, cfg.table(scfg["bronze_table"])) or [], "skipped": "no landing folder"}
    fn = ingest_copy_into if scfg["ingestion_method"] == "copy_into" else ingest_autoloader
    return fn(spark, cfg, source, scfg, run_id)


def ingest_all(spark, cfg: ProjectConfig, run_id: str, sources: Optional[Sequence[str]] = None) -> List[dict]:
    return [ingest_source(spark, cfg, s, run_id) for s in (sources or list(cfg.sources))]


def landing_counts(spark, cfg: ProjectConfig, source: str) -> Tuple[int, int]:
    """(rows, files) currently in the landing folder of a source, read as a plain batch (for reconciliation)."""
    fmt = cfg.sources[source]["format"]
    reader = spark.read.option("recursiveFileLookup", "true").option("pathGlobFilter", f"*.{fmt}")
    if fmt == "csv":
        reader = reader.option("header", "true")
    df = getattr(reader, fmt)(f"{cfg.landing_root}/{source}")
    return df.count(), df.select("_metadata.file_path").distinct().count()


def write_ingestion_log(spark, cfg: ProjectConfig, run_id: str, results: List[dict], step: str) -> None:
    import json
    from pyspark.sql import types as T

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [(run_id, r["source"], ",".join(r.get("batches", [])), r["method"], int(r["files_processed"]),
             int(r["rows_ingested"]), r["table"].replace("`", ""), now,
             json.dumps({"step": step, "new_columns": r["new_columns"], "schema_restarts": r["schema_restarts"],
                         "skipped": r.get("skipped")}))
            for r in results]
    schema = T.StructType([T.StructField("run_id", T.StringType(), False), T.StructField("source_name", T.StringType()),
                           T.StructField("batch_id", T.StringType()), T.StructField("ingestion_method", T.StringType()),
                           T.StructField("files_processed", T.LongType()), T.StructField("rows_ingested", T.LongType()),
                           T.StructField("target_table", T.StringType()), T.StructField("ingested_at", T.TimestampType()),
                           T.StructField("details", T.StringType())])
    spark.createDataFrame(rows, schema).write.mode("append").saveAsTable(cfg.table("audit.ingestion_log"))


def write_schema_changes(spark, cfg: ProjectConfig, run_id: str, results: List[dict]) -> int:
    """Log columns added by schema evolution and columns outside the documented contract."""
    from pyspark.sql import types as T

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for r in results:
        if r.get("skipped") or not r["columns"]:
            continue
        contract = list(cfg.expected_schemas.get(r["source"], {}) or {})
        drift = schema_drift(contract, r["columns"])
        for c in r["new_columns"]:
            rows.append((run_id, r["table"].replace("`", ""), c, "column_added", now,
                         "in contract" if c in contract else "not in contract"))
        if r.get("table_created"):
            for c in drift["not_in_contract"]:
                rows.append((run_id, r["table"].replace("`", ""), c, "column_not_in_contract", now,
                             "present when table was created"))
        for c in drift["missing_from_delivery"]:
            rows.append((run_id, r["table"].replace("`", ""), c, "contract_column_missing", now, ""))
    if rows:
        schema = T.StructType([T.StructField("run_id", T.StringType(), False), T.StructField("table_name", T.StringType()),
                               T.StructField("column_name", T.StringType()), T.StructField("change_type", T.StringType()),
                               T.StructField("detected_at", T.TimestampType()), T.StructField("details", T.StringType())])
        spark.createDataFrame(rows, schema).write.mode("append").saveAsTable(cfg.table("monitoring.schema_change_log"))
    return len(rows)


def reset_bronze(spark, cfg: ProjectConfig) -> None:
    """Drop Bronze tables, landing files and Bronze checkpoints (dev/test demo only)."""
    if cfg.env == "prod":
        raise PermissionError("reset_bronze is not allowed in prod")
    for scfg in cfg.sources.values():
        spark.sql(f"DROP TABLE IF EXISTS {cfg.table(scfg['bronze_table'])}")
    for p in (cfg.landing_root, f"{cfg.checkpoint_root}/bronze"):
        shutil.rmtree(p, ignore_errors=True)
