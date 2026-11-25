"""Bronze -> Silver: typing, validation, quarantine, dedup, MERGE, SCD1/SCD2 (Phase 3).

Principles
----------
* **Silver is a curated contract.** Every Silver table has an explicit column list and types
  (SPECS below). Bronze evolves automatically; Silver changes only when the contract is updated
  on purpose. Contract columns missing from older Bronze rows arrive as typed NULLs.
* **Never silently drop.** A record that breaks a rule goes to `silver.quarantine_records` with
  the rule(s), a reason, the original record as JSON and its lineage. Exact duplicates and
  superseded versions are collapsed, and counted in the reconciliation.
* **Incremental and idempotent.** Each run reads only Bronze rows newer than the source's
  high-watermark (`audit.silver_watermark`, on `_ingested_at`). Results are MERGEd on the business
  key, so re-processing the same rows changes nothing.
* **Latest version wins.** Within a key, the row ingested last wins (tie-break: batch, file).
  So restated records replace earlier values, and re-sent copies are no-ops. Late-arriving
  records (old event dates in a new file) are simply MERGEd in; nothing filters by event date.
* **Masters:** products are SCD Type 2 (history of tracked attributes, with type-1 attributes
  updated in place). Stores, regions, suppliers and calendar are SCD Type 1 (overwrite).

All transformation logic is plain DataFrame code (unit-tested with local PySpark under ANSI
mode, matching serverless). Only the MERGE / CREATE statements need Databricks Delta.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from .config import ProjectConfig

LINEAGE = ["_source_file", "_ingested_at", "_ingestion_run_id", "_batch_id"]
DATE_FORMATS = ("yyyy-MM-dd", "dd.MM.yyyy", "dd/MM/yyyy")
OPEN_END_DATE = "9999-12-31"
CATEGORIES = ["Food", "Personal Care", "Beauty & Wellbeing", "Ice Cream", "Home Care"]
CHANNELS = ["Modern Trade", "General Trade", "E-Commerce"]


@dataclass
class Rule:
    name: str
    fail_sql: str          # SQL boolean expression that is TRUE when the record FAILS
    reason: str


@dataclass
class Spec:
    source: str
    target: str                         # e.g. "silver.pos"
    pk: List[str]
    columns: Dict[str, str]             # contract: column -> Spark SQL type (date handled specially)
    rules: List[Rule] = field(default_factory=list)
    ri: Dict[str, str] = field(default_factory=dict)   # column -> reference name ("sku"|"store"|"region")
    cluster_by: List[str] = field(default_factory=list)
    kind: str = "fact"                  # fact | scd1 | scd2
    scd2_tracked: List[str] = field(default_factory=list)
    scd2_type1: List[str] = field(default_factory=list)
    scd2_effective_col: str = ""

    @property
    def bronze(self) -> str:
        return f"bronze.{self.source}_raw"


def _in(col: str, values: Sequence[str]) -> str:
    return f"{col} NOT IN ({', '.join(repr(v) for v in values)})"


SPECS: Dict[str, Spec] = {
    # ------------------------------------------------------------------ masters
    "regions": Spec("regions", "silver.regions", ["region_id"], kind="scd1",
                    columns={"region_id": "string", "region_name": "string", "country": "string",
                             "country_code": "string", "currency": "string", "hemisphere": "string"},
                    rules=[Rule("country_code_invalid", "length(country_code) <> 2", "country code must be ISO-2")]),
    "suppliers": Spec("suppliers", "silver.suppliers", ["supplier_id"], kind="scd1",
                      columns={"supplier_id": "string", "supplier_name": "string", "country": "string",
                               "category": "string", "lead_time_days": "int", "lead_time_std_days": "double",
                               "moq": "double", "replenishment_frequency_days": "int", "reliability_score": "double"},
                      rules=[Rule("lead_time_invalid", "lead_time_days IS NULL OR lead_time_days <= 0", "lead time must be > 0"),
                             Rule("category_invalid", _in("category", CATEGORIES), "unknown category")]),
    "calendar": Spec("calendar", "silver.calendar", ["date", "country_code"], kind="scd1",
                     columns={"date": "date", "country_code": "string", "week_start": "date", "iso_year": "int",
                              "iso_week": "int", "month": "int", "quarter": "int", "year": "int", "day_of_week": "int",
                              "is_weekend": "boolean", "holiday_flag": "boolean", "holiday_name": "string",
                              "festival_flag": "boolean", "festival_name": "string", "is_future": "boolean"},
                     rules=[Rule("week_start_not_monday", "dayofweek(week_start) <> 2", "week_start must be a Monday")]),
    "stores": Spec("stores", "silver.stores", ["store_id"], kind="scd1", ri={"region_id": "region"},
                   columns={"store_id": "string", "store_name": "string", "region_id": "string", "channel": "string",
                            "store_format": "string", "size_index": "double", "open_date": "date", "status": "string",
                            "last_modified": "date"},
                   rules=[Rule("channel_invalid", _in("channel", CHANNELS), "unknown channel")]),
    "products": Spec("products", "silver.products_scd2", ["sku_id"], kind="scd2",
                     columns={"sku_id": "string", "product_id": "string", "product_name": "string", "brand": "string",
                              "category": "string", "subcategory": "string", "pack_size": "string", "uom": "string",
                              "launch_date": "date", "discontinue_date": "date", "lifecycle_status": "string",
                              "shelf_life": "int", "supplier_id": "string", "unit_cost_usd": "double",
                              "last_modified": "date", "sustainability_tag": "string"},
                     rules=[Rule("sku_format_invalid", "NOT sku_id RLIKE '^SKU[0-9]{5}$'", "sku_id must look like SKU00000"),
                            Rule("category_invalid", _in("category", CATEGORIES), "unknown category"),
                            Rule("lifecycle_invalid", _in("lifecycle_status", ["New", "Active", "Phase-Out", "Discontinued"]),
                                 "unknown lifecycle status")],
                     scd2_tracked=["product_id", "product_name", "brand", "category", "subcategory", "pack_size", "uom",
                                   "launch_date", "discontinue_date", "lifecycle_status", "shelf_life", "supplier_id",
                                   "unit_cost_usd"],
                     scd2_type1=["sustainability_tag"], scd2_effective_col="last_modified"),
    # ------------------------------------------------------------------ facts
    "pos": Spec("pos", "silver.pos", ["transaction_id"], cluster_by=["transaction_date", "sku_id"],
                ri={"sku_id": "sku", "store_id": "store", "region_id": "region"},
                columns={"transaction_id": "string", "transaction_date": "date", "sku_id": "string", "store_id": "string",
                         "region_id": "string", "channel": "string", "quantity": "double", "revenue": "double",
                         "selling_price": "double", "promotion_id": "string", "returned_quantity": "double",
                         "loyalty_member_flag": "boolean", "basket_count": "int"},
                rules=[Rule("quantity_null", "quantity IS NULL", "quantity missing or not numeric"),
                       Rule("quantity_negative", "quantity < 0", "negative sales quantity (returns are separate)"),
                       Rule("selling_price_null", "selling_price IS NULL", "selling price missing"),
                       Rule("selling_price_non_positive", "selling_price <= 0", "selling price must be > 0"),
                       Rule("returns_exceed_quantity", "returned_quantity > quantity", "returned more than sold"),
                       Rule("channel_invalid", _in("channel", CHANNELS), "unknown channel")]),
    "orders": Spec("orders", "silver.orders", ["order_id", "order_item"], cluster_by=["order_date", "sku_id"],
                   ri={"sku_id": "sku", "region_id": "region"},
                   columns={"order_id": "string", "order_item": "int", "order_date": "date",
                            "requested_delivery_date": "date", "sku_id": "string", "region_id": "string",
                            "channel": "string", "customer_id": "string", "ordered_quantity": "double",
                            "confirmed_quantity": "double", "order_status": "string", "order_source": "string"},
                   rules=[Rule("ordered_quantity_null", "ordered_quantity IS NULL", "ordered quantity missing"),
                          Rule("ordered_quantity_negative", "ordered_quantity < 0", "negative ordered quantity"),
                          Rule("confirmed_exceeds_ordered", "confirmed_quantity > ordered_quantity",
                               "confirmed more than ordered"),
                          Rule("channel_invalid", _in("channel", CHANNELS), "unknown channel")]),
    "sales": Spec("sales", "silver.sales", ["sales_doc_id", "sales_doc_item"], cluster_by=["posting_date", "sku_id"],
                  ri={"sku_id": "sku", "region_id": "region"},
                  columns={"sales_doc_id": "string", "sales_doc_item": "int", "doc_type": "string", "order_id": "string",
                           "posting_date": "date", "sku_id": "string", "region_id": "string", "channel": "string",
                           "plant_id": "string", "delivered_quantity": "double", "uom": "string", "net_value": "double",
                           "currency": "string"},
                  rules=[Rule("delivered_quantity_invalid", "delivered_quantity IS NULL OR delivered_quantity < 0",
                              "delivered quantity missing or negative")]),
    "promotions": Spec("promotions", "silver.promotions", ["promotion_id"], cluster_by=["start_date", "sku_id"],
                       ri={"sku_id": "sku", "region_id": "region"},
                       columns={"promotion_id": "string", "sku_id": "string", "region_id": "string", "channel": "string",
                                "start_date": "date", "end_date": "date", "promotion_type": "string",
                                "discount_percentage": "double", "display_flag": "boolean", "marketing_campaign": "string",
                                "planned_promotion_flag": "boolean", "promotion_status": "string", "approved_date": "date"},
                       rules=[Rule("end_before_start", "end_date < start_date", "promotion ends before it starts"),
                              Rule("discount_out_of_range", "discount_percentage < 0 OR discount_percentage > 100",
                                   "discount must be 0-100%"),
                              Rule("status_invalid", _in("promotion_status", ["Executed", "Cancelled", "Approved", "Draft"]),
                                   "unknown promotion status")]),
    "pricing": Spec("pricing", "silver.pricing", ["price_record_id"], cluster_by=["effective_date", "sku_id"],
                    ri={"sku_id": "sku", "region_id": "region"},
                    columns={"price_record_id": "string", "sku_id": "string", "region_id": "string",
                             "effective_date": "date", "regular_price": "double", "promotional_price": "double",
                             "previous_price": "double", "current_price": "double", "competitor_price": "double",
                             "currency": "string", "price_status": "string", "change_reason": "string"},
                    rules=[Rule("regular_price_invalid", "regular_price IS NULL OR regular_price <= 0", "regular price must be > 0"),
                           Rule("current_price_invalid", "current_price IS NULL OR current_price <= 0", "current price must be > 0")]),
    "weather": Spec("weather", "silver.weather", ["region_id", "date", "weather_forecast_type"], cluster_by=["date", "region_id"],
                    ri={"region_id": "region"},
                    columns={"region_id": "string", "date": "date", "temperature": "double", "rainfall": "double",
                             "humidity": "double", "extreme_weather_flag": "boolean", "weather_forecast_type": "string",
                             "forecast_issued_date": "date"},
                    rules=[Rule("temperature_out_of_range", "temperature IS NULL OR temperature < -40 OR temperature > 60",
                                "temperature missing or physically implausible (sensor error)"),
                           Rule("rainfall_negative", "rainfall < 0", "negative rainfall"),
                           Rule("humidity_out_of_range", "humidity < 0 OR humidity > 100", "humidity outside 0-100%"),
                           Rule("forecast_type_invalid", _in("weather_forecast_type", ["actual", "forecast"]),
                                "unknown weather record type")]),
    "inventory": Spec("inventory", "silver.inventory", ["snapshot_date", "sku_id", "region_id"],
                      cluster_by=["snapshot_date", "sku_id"], ri={"sku_id": "sku", "region_id": "region"},
                      columns={"snapshot_date": "date", "sku_id": "string", "region_id": "string", "plant_id": "string",
                               "on_hand_qty": "double", "in_transit_qty": "double", "open_po_qty": "double",
                               "safety_stock_qty": "double", "reorder_point_qty": "double", "lead_time_days": "int",
                               "moq": "double", "replenishment_frequency_days": "int", "unit_cost": "double",
                               "currency": "string"},
                      rules=[Rule("on_hand_negative", "on_hand_qty < 0", "negative stock on hand"),
                             Rule("lead_time_invalid", "lead_time_days IS NULL OR lead_time_days <= 0", "lead time must be > 0")]),
}

# masters first (facts are validated against them)
PROCESS_ORDER = ["regions", "suppliers", "calendar", "stores", "products",
                 "pos", "orders", "sales", "promotions", "pricing", "weather", "inventory"]


def business_columns(spec: Spec) -> List[str]:
    return list(spec.columns)


# ======================================================================================
# Pure DataFrame transformations (unit-tested locally)
# ======================================================================================

def parse_date_expr(col: str) -> str:
    """Multi-format, ANSI-safe date parsing: first format that parses wins, else NULL."""
    parts = ", ".join(f"to_date(try_to_timestamp(`{col}`, '{f}'))" for f in DATE_FORMATS)
    return f"coalesce({parts})"


def standardise(df, spec: Spec):
    """Cast to the Silver contract. Unparseable values become NULL (and then fail a rule)."""
    from pyspark.sql import functions as F

    exprs = []
    for c, t in spec.columns.items():
        if c not in df.columns:
            exprs.append(F.lit(None).cast("date" if t == "date" else t).alias(c))
        elif t == "date":
            exprs.append(F.expr(parse_date_expr(c)).alias(c))
        elif t == "string":
            exprs.append(F.expr(f"nullif(trim(cast(`{c}` as string)), '')").alias(c))
        else:
            exprs.append(F.expr(f"try_cast(`{c}` as {t})").alias(c))
    rescued = F.col("_rescued_data") if "_rescued_data" in df.columns else F.lit(None).cast("string")
    lineage = [F.col(c) if c in df.columns else F.lit(None).cast("timestamp" if c == "_ingested_at" else "string").alias(c)
               for c in LINEAGE]
    # keep the raw value of every typed contract column so a failed cast can be explained
    raw = F.to_json(F.struct(*[F.col(c) for c in spec.columns if c in df.columns]))
    return df.select(*exprs, rescued.alias("_rescued_data"), raw.alias("_raw_json"), *lineage)


def all_rules(spec: Spec, df_columns: Sequence[str]) -> List[Rule]:
    """Contract rules + automatic rules (PK not null, typed values parse, RI, rescued data)."""
    rules = [Rule(f"{k}_null", f"{k} IS NULL", f"primary-key column {k} missing or unparseable") for k in spec.pk]
    for c, t in spec.columns.items():
        if t == "date" and c not in spec.pk and c in ("transaction_date", "order_date", "posting_date", "start_date",
                                                      "effective_date", "snapshot_date", "week_start"):
            rules.append(Rule(f"{c}_unparseable", f"{c} IS NULL", f"{c} missing or not a recognised date format"))
    for col, ref in spec.ri.items():
        rules.append(Rule(f"{col}_null", f"{col} IS NULL", f"{col} missing"))
        rules.append(Rule(f"{ref}_unknown", f"{col} IS NOT NULL AND _ri_{ref} IS NULL",
                          f"{col} not found in {ref} master (referential integrity)"))
    rules += spec.rules
    rules.append(Rule("rescued_data_present", "_rescued_data IS NOT NULL", "values did not match the schema (_rescued_data)"))
    # de-duplicate by name (a spec rule may repeat an automatic one)
    seen, out = set(), []
    for r in rules:
        if r.name not in seen:
            seen.add(r.name)
            out.append(r)
    return out


def validate(df, spec: Spec, refs: Optional[Dict[str, object]] = None):
    """Add `_failed_rules` (array of rule names) and `_rejection_reason`.

    refs: {"sku": DataFrame[sku_id], "store": DataFrame[store_id], "region": DataFrame[region_id]}
    """
    from pyspark.sql import functions as F

    refs = refs or {}
    key_col = {"sku": "sku_id", "store": "store_id", "region": "region_id"}
    for col, ref in spec.ri.items():
        keys = refs[ref].select(F.col(key_col[ref]).alias(col)).distinct().withColumn(f"_ri_{ref}", F.lit(True))
        df = df.join(F.broadcast(keys), on=col, how="left")
    rules = all_rules(spec, df.columns)
    names = F.array_compact(F.array(*[F.when(F.expr(f"coalesce({r.fail_sql}, false)"), F.lit(r.name)) for r in rules]))
    reasons = F.array_compact(F.array(*[F.when(F.expr(f"coalesce({r.fail_sql}, false)"), F.lit(r.reason)) for r in rules]))
    df = df.withColumn("_failed_rules", names).withColumn("_rejection_reason", F.concat_ws("; ", reasons))
    return df.drop(*[f"_ri_{ref}" for ref in set(spec.ri.values())])


def split_valid_invalid(df):
    from pyspark.sql import functions as F
    return df.where(F.size("_failed_rules") == 0), df.where(F.size("_failed_rules") > 0)


def record_hash(spec: Spec, cols: Optional[Sequence[str]] = None):
    from pyspark.sql import functions as F
    cols = list(cols or spec.columns)
    return F.sha2(F.to_json(F.struct(*[F.col(c) for c in cols])), 256)


def dedupe_latest(df, keys: Sequence[str]):
    """Keep one row per key: last ingested wins; tie-break on batch then file (deterministic)."""
    from pyspark.sql import Window, functions as F
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc_nulls_last(), F.col("_batch_id").desc_nulls_last(),
                                          F.col("_source_file").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def to_silver_rows(valid_df, spec: Spec, run_id: str):
    """Final Silver shape: contract columns + hash + lineage + audit columns."""
    from pyspark.sql import functions as F
    out = dedupe_latest(valid_df, spec.pk).withColumn("_record_hash", record_hash(spec))
    return out.select(*spec.columns, "_record_hash", *LINEAGE,
                      F.current_timestamp().alias("_silver_updated_at"), F.lit(run_id).alias("_silver_run_id"))


def to_quarantine_rows(invalid_df, spec: Spec, run_id: str):
    from pyspark.sql import functions as F
    rid = F.concat_ws("|", *[F.coalesce(F.col(k).cast("string"), F.lit("<null>")) for k in spec.pk])
    return invalid_df.select(
        F.lit(spec.source).alias("source"),
        rid.alias("record_id"),
        F.array_join("_failed_rules", ",").alias("rule_name"),
        F.col("_rejection_reason").alias("rejection_reason"),
        F.col("_failed_rules").alias("failed_rules"),
        F.col("_raw_json").alias("record_json"),
        *LINEAGE,
        F.current_timestamp().alias("quarantined_at"),
        F.lit(run_id).alias("run_id"))


def scd2_stage(snapshot_df, current_df, spec: Spec):
    """Stage rows for a single-MERGE SCD2 (the 'union trick').

    snapshot_df: validated, typed rows of ONE delivery (one row per key after dedupe)
    current_df : target rows with is_current = true (may be empty)

    Output = every snapshot row with merge_key = key (matches the current version: expire it if
    tracked attributes changed, or update type-1 columns in place), PLUS, for keys whose tracked
    attributes changed, a copy with merge_key = NULL (never matches -> inserted as the new version).
    Changes dated before the current version are ignored, so replaying an old delivery can't
    roll history back.
    """
    from pyspark.sql import functions as F

    key = spec.pk[0]
    eff = F.coalesce(F.col(spec.scd2_effective_col), F.to_date("_ingested_at"))
    snap = (snapshot_df
            .withColumn("effective_from", eff)
            .withColumn("_tracked_hash", record_hash(spec, spec.scd2_tracked))
            .withColumn("_type1_hash", record_hash(spec, spec.scd2_type1)))
    cur = current_df.select(F.col(key).alias("_c_key"), F.col("_tracked_hash").alias("_c_hash"),
                            F.col("effective_from").alias("_c_eff"))
    j = snap.join(cur, snap[key] == cur["_c_key"], "left")
    changed = j.where(F.col("_c_key").isNotNull() & (F.col("_tracked_hash") != F.col("_c_hash"))
                      & (F.col("effective_from") >= F.col("_c_eff")))
    cols = [c for c in snap.columns]
    matched = j.select(*cols).withColumn("merge_key", F.col(key))
    inserts = changed.select(*cols).withColumn("merge_key", F.lit(None).cast("string"))
    return matched.unionByName(inserts)


# ======================================================================================
# SQL builders (pure strings, unit-tested)
# ======================================================================================

def merge_fact_sql(target: str, view: str, pk: Sequence[str]) -> str:
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in pk)
    return (f"MERGE INTO {target} t USING {view} s ON {on}\n"
            f"WHEN MATCHED AND s._ingested_at >= t._ingested_at AND s._record_hash <> t._record_hash THEN UPDATE SET *\n"
            f"WHEN NOT MATCHED THEN INSERT *")


def merge_quarantine_sql(target: str, view: str) -> str:
    return (f"MERGE INTO {target} t USING {view} s\n"
            f"ON t.source = s.source AND t.record_id = s.record_id AND t._source_file <=> s._source_file "
            f"AND t._ingested_at <=> s._ingested_at AND t.rule_name = s.rule_name\n"
            f"WHEN NOT MATCHED THEN INSERT *")


def merge_scd2_sql(target: str, view: str, spec: Spec) -> str:
    key = spec.pk[0]
    cols = list(spec.columns)
    type1_set = ", ".join(f"t.`{c}` = s.`{c}`" for c in spec.scd2_type1)
    ins_cols = ", ".join(f"`{c}`" for c in cols)
    ins_vals = ", ".join(f"s.`{c}`" for c in cols)
    return f"""MERGE INTO {target} t
USING {view} s
ON t.`{key}` = s.merge_key AND t.is_current = true
WHEN MATCHED AND t._tracked_hash <> s._tracked_hash AND s.effective_from >= t.effective_from THEN UPDATE SET
  t.is_current = false, t.effective_to = date_sub(s.effective_from, 1), t._silver_updated_at = current_timestamp()
WHEN MATCHED AND t._tracked_hash = s._tracked_hash AND t._type1_hash <> s._type1_hash THEN UPDATE SET
  {type1_set}, t._type1_hash = s._type1_hash, t._silver_updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT
  ({ins_cols}, product_sk, effective_from, effective_to, is_current, _tracked_hash, _type1_hash,
   {", ".join(LINEAGE)}, _silver_updated_at, _silver_run_id)
VALUES
  ({ins_vals}, xxhash64(s.`{key}`, s.effective_from), s.effective_from, DATE'{OPEN_END_DATE}', true,
   s._tracked_hash, s._type1_hash, {", ".join("s." + c for c in LINEAGE)}, current_timestamp(), s._silver_run_id)"""


def merge_scd1_sql(target: str, view: str, pk: Sequence[str]) -> str:
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in pk)
    return (f"MERGE INTO {target} t USING {view} s ON {on}\n"
            f"WHEN MATCHED AND t._record_hash <> s._record_hash THEN UPDATE SET *\n"
            f"WHEN NOT MATCHED THEN INSERT *")


# ======================================================================================
# Spark / Delta side (Databricks)
# ======================================================================================

QUARANTINE_TABLE = "silver.quarantine_records"
WATERMARK_TABLE = "audit.silver_watermark"

SUPPORT_DDL = {
    QUARANTINE_TABLE: """source STRING, record_id STRING, rule_name STRING, rejection_reason STRING,
        failed_rules ARRAY<STRING>, record_json STRING, _source_file STRING, _ingested_at TIMESTAMP,
        _ingestion_run_id STRING, _batch_id STRING, quarantined_at TIMESTAMP, run_id STRING""",
    WATERMARK_TABLE: """source STRING, bronze_table STRING, last_ingested_at TIMESTAMP, rows_read BIGINT,
        last_run_id STRING, updated_at TIMESTAMP""",
}


def ensure_support_tables(spark, cfg: ProjectConfig) -> None:
    for t, cols in SUPPORT_DDL.items():
        cluster = " CLUSTER BY (source)" if t == QUARANTINE_TABLE else ""
        spark.sql(f"CREATE TABLE IF NOT EXISTS {cfg.table(t)} ({cols}){cluster}")


def _exists(spark, table: str) -> bool:
    return spark.catalog.tableExists(table.replace("`", ""))


TS_FMT = "yyyy-MM-dd HH:mm:ss.SSSSSS"


def read_watermark(spark, cfg: ProjectConfig, source: str) -> Optional[str]:
    """High-watermark as a string in the session time zone (no Python datetime round-trip)."""
    r = spark.sql(f"SELECT date_format(max(last_ingested_at), '{TS_FMT}') AS w "
                  f"FROM {cfg.table(WATERMARK_TABLE)} WHERE source = '{source}'").first()
    return r["w"] if r else None


def write_watermark(spark, cfg: ProjectConfig, source: str, spec: Spec, wm: str, rows_read: int, run_id: str) -> None:
    spark.sql(f"""MERGE INTO {cfg.table(WATERMARK_TABLE)} t
        USING (SELECT '{source}' AS source, '{spec.bronze}' AS bronze_table,
                      to_timestamp('{wm}', '{TS_FMT}') AS last_ingested_at, CAST({rows_read} AS BIGINT) AS rows_read,
                      '{run_id}' AS last_run_id, current_timestamp() AS updated_at) s
        ON t.source = s.source
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


def reference_frames(spark, cfg: ProjectConfig) -> Dict[str, object]:
    refs = {}
    for name, table, col in (("sku", "silver.products_scd2", "sku_id"), ("store", "silver.stores", "store_id"),
                             ("region", "silver.regions", "region_id")):
        t = cfg.table(table)
        refs[name] = spark.table(t).select(col) if _exists(spark, t) else spark.createDataFrame([], f"{col} string")
    return refs


def _create_like(spark, table: str, view: str, cluster_by: Sequence[str]) -> None:
    cluster = f" CLUSTER BY ({', '.join(cluster_by)})" if cluster_by else ""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table}{cluster} AS SELECT * FROM {view} WHERE 1 = 0")


def _quarantine(spark, cfg, invalid, spec, run_id) -> None:
    to_quarantine_rows(invalid, spec, run_id).createOrReplaceTempView("_q_new")
    spark.sql(merge_quarantine_sql(cfg.table(QUARANTINE_TABLE), "_q_new"))


def process_source(spark, cfg: ProjectConfig, source: str, run_id: str, refs: Optional[Dict] = None) -> dict:
    """Incrementally move one source from Bronze to Silver. Returns run statistics."""
    from pyspark.sql import functions as F

    spec = SPECS[source]
    bronze, target = cfg.table(spec.bronze), cfg.table(spec.target)
    stats = {"source": source, "target": target, "bronze_rows_read": 0, "valid_rows": 0, "quarantined_rows": 0,
             "silver_rows_after": None, "skipped": None}
    if not _exists(spark, bronze):
        stats["skipped"] = "bronze table missing"
        return stats
    wm = read_watermark(spark, cfg, source)
    new = spark.table(bronze)
    if wm is not None:
        new = new.where(F.col("_ingested_at") > F.to_timestamp(F.lit(wm), TS_FMT))
    n_new = new.count()
    stats["bronze_rows_read"] = n_new
    if n_new == 0:
        stats["skipped"] = "no new bronze rows"
        stats["silver_rows_after"] = spark.table(target).count() if _exists(spark, target) else 0
        return stats
    new_wm = new.agg(F.date_format(F.max("_ingested_at"), TS_FMT)).first()[0]

    checked = validate(standardise(new, spec), spec, refs or reference_frames(spark, cfg))
    valid, invalid = split_valid_invalid(checked)
    stats["quarantined_rows"] = invalid.count()
    stats["valid_rows"] = n_new - stats["quarantined_rows"]
    if stats["quarantined_rows"]:
        _quarantine(spark, cfg, invalid, spec, run_id)

    if spec.kind == "scd2":
        _apply_scd2(spark, cfg, spec, valid, run_id)
    else:
        rows = to_silver_rows(valid, spec, run_id)
        rows.createOrReplaceTempView("_silver_new")
        _create_like(spark, target, "_silver_new", spec.cluster_by)
        sql = merge_scd1_sql(target, "_silver_new", spec.pk) if spec.kind == "scd1" \
            else merge_fact_sql(target, "_silver_new", spec.pk)
        spark.sql(sql)

    write_watermark(spark, cfg, source, spec, new_wm, n_new, run_id)
    stats["silver_rows_after"] = spark.table(target).count()
    return stats


def _apply_scd2(spark, cfg, spec: Spec, valid, run_id: str) -> None:
    """Apply each delivery (batch) in order, one MERGE per batch."""
    from pyspark.sql import functions as F

    target = cfg.table(spec.target)
    batches = [r[0] for r in valid.select("_batch_id").distinct().orderBy("_batch_id").collect()]
    for b in batches:
        snap = dedupe_latest(valid.where(F.col("_batch_id") == b), spec.pk).withColumn("_silver_run_id", F.lit(run_id))
        if not _exists(spark, target):
            empty = scd2_stage(snap.limit(0), _empty_current(spark, spec), spec)
            empty.createOrReplaceTempView("_scd2_shape")
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {target} CLUSTER BY ({spec.pk[0]}) AS
                SELECT {", ".join(spec.columns)}, CAST(NULL AS BIGINT) AS product_sk, effective_from,
                       CAST(NULL AS DATE) AS effective_to, CAST(NULL AS BOOLEAN) AS is_current,
                       _tracked_hash, _type1_hash, {", ".join(LINEAGE)},
                       CAST(NULL AS TIMESTAMP) AS _silver_updated_at, _silver_run_id
                FROM _scd2_shape WHERE 1 = 0""")
        current = spark.table(target).where("is_current = true")
        scd2_stage(snap, current, spec).createOrReplaceTempView("_scd2_stage")
        spark.sql(merge_scd2_sql(target, "_scd2_stage", spec))


def _empty_current(spark, spec: Spec):
    return spark.createDataFrame([], f"{spec.pk[0]} string, _tracked_hash string, effective_from date")


def process_all(spark, cfg: ProjectConfig, run_id: str, sources: Optional[Sequence[str]] = None) -> List[dict]:
    """Masters first; reference frames are refreshed after the masters so facts see the new keys."""
    ensure_support_tables(spark, cfg)
    out = []
    for s in (sources or PROCESS_ORDER):
        refs = reference_frames(spark, cfg) if SPECS[s].ri else None
        out.append(process_source(spark, cfg, s, run_id, refs))
    return out


def reset_silver(spark, cfg: ProjectConfig) -> None:
    if cfg.env == "prod":
        raise PermissionError("reset_silver is not allowed in prod")
    for spec in SPECS.values():
        spark.sql(f"DROP TABLE IF EXISTS {cfg.table(spec.target)}")
    for t in SUPPORT_DDL:
        spark.sql(f"DROP TABLE IF EXISTS {cfg.table(t)}")


def harden_bronze(spark, cfg: ProjectConfig) -> List[str]:
    """Bronze is the immutable record of deliveries: make every table append-only and documented."""
    done = []
    for source, scfg in cfg.sources.items():
        t = cfg.table(scfg["bronze_table"])
        if _exists(spark, t):
            comment = scfg.get("description", "").replace("'", "")
            spark.sql(f"ALTER TABLE {t} SET TBLPROPERTIES ('delta.appendOnly' = 'true', "
                      f"'quality' = 'bronze', 'synthetic' = 'true', 'source_name' = '{source}')")
            spark.sql(f"COMMENT ON TABLE {t} IS 'BRONZE (raw, append-only, SYNTHETIC): {comment}'")
            done.append(t)
    return done
