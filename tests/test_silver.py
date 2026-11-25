"""Phase 3: Silver transformations, run on local PySpark (ANSI on, like serverless) against
Bronze-shaped copies of the real synthetic batches. Delta MERGE itself runs on Databricks."""
import pandas as pd
import pytest

pyspark = pytest.importorskip("pyspark")

from dfp import silver as sv  # noqa: E402
from dfp.config import load_config  # noqa: E402

TS = {"batch_001": "2026-11-24 10:00:00", "batch_002": "2026-11-24 11:00:00"}


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("dfp-tests")
         .config("spark.sql.ansi.enabled", "true").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timeZone", "UTC").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _to_bronze_pdf(df: pd.DataFrame, source: str, batch: str) -> pd.DataFrame:
    """Mimic Bronze: every delivered value as a string (CSV/JSON without type inference) + lineage."""
    out = pd.DataFrame({c: [None if pd.isna(v) else str(v) for v in df[c]] for c in df.columns})
    out["_source_file"] = f"/Volumes/x/raw/source_files/landing/{source}/{batch}/{source}.csv"
    out["_ingested_at"] = pd.Timestamp(TS[batch]).to_pydatetime()
    out["_ingestion_run_id"] = f"run_{batch}"
    out["_batch_id"] = batch
    return out


@pytest.fixture(scope="session")
def bronze(spark, synthetic_small):
    """{source: Spark DataFrame of batch_001 ∪ batch_002 in Bronze shape}"""
    out = {}
    for source in sv.SPECS:
        parts = [_to_bronze_pdf(b[source], source, name) for name, b in synthetic_small["bb"].batches.items() if source in b]
        pdf = pd.concat(parts, ignore_index=True)
        string_cols = [c for c in pdf.columns if c != "_ingested_at"]
        schema = ", ".join(f"`{c}` string" for c in string_cols) + ", `_ingested_at` timestamp"
        rows = [tuple(None if (v is None or (isinstance(v, float) and v != v)) else
                      (v.to_pydatetime() if isinstance(v, pd.Timestamp) else v) for v in r)
                for r in pdf[string_cols + ["_ingested_at"]].itertuples(index=False, name=None)]
        out[source] = spark.createDataFrame(rows, schema)
    return out


@pytest.fixture(scope="session")
def refs(spark, synthetic_small):
    g = synthetic_small["gen"]
    return {"sku": spark.createDataFrame(g.products[["sku_id"]]),
            "store": spark.createDataFrame(g.tables["stores"][["store_id"]]),
            "region": spark.createDataFrame(g.tables["regions"][["region_id"]])}


@pytest.fixture(scope="session")
def issues(synthetic_small):
    return pd.DataFrame(synthetic_small["bb"].issues)


def _checked(bronze, refs, source):
    spec = sv.SPECS[source]
    return sv.validate(sv.standardise(bronze[source], spec), spec, refs)


def _ids(df, spec):
    from pyspark.sql import functions as F
    rid = F.concat_ws("|", *[F.col(k).cast("string") for k in spec.pk])
    return {r[0] for r in df.select(rid).collect()}


# ------------------------------------------------------------------ contract
def test_silver_contract_covers_source_contract(config_dir):
    cfg = load_config("dev", config_dir)
    for source, spec in sv.SPECS.items():
        missing = set(cfg.expected_schemas[source]) - set(spec.columns)
        assert not missing, (source, missing)
    assert set(sv.PROCESS_ORDER) == set(sv.SPECS)
    assert sv.PROCESS_ORDER.index("products") < sv.PROCESS_ORDER.index("pos")


def test_standardise_types_and_dates(spark):
    spec = sv.SPECS["sales"]
    df = spark.createDataFrame([("S1", "10", "2026-09-24", "5.0", None), ("S2", "10", "24.09.2026", "abc", None),
                                ("S3", "10", "not a date", "1", None)],
                               "sales_doc_id string, sales_doc_item string, posting_date string, delivered_quantity string,"
                               " _ingested_at timestamp")
    rows = {r["sales_doc_id"]: r for r in sv.standardise(df, spec).collect()}
    assert str(rows["S1"]["posting_date"]) == "2026-09-24" and rows["S1"]["delivered_quantity"] == 5.0
    assert str(rows["S2"]["posting_date"]) == "2026-09-24"          # dd.MM.yyyy standardised
    assert rows["S2"]["delivered_quantity"] is None                  # non-numeric -> NULL (ANSI-safe)
    assert rows["S3"]["posting_date"] is None
    assert rows["S1"]["sales_doc_item"] == 10 and rows["S1"]["currency"] is None   # missing contract col -> typed NULL


# ------------------------------------------------------------------ validation vs injected defects
@pytest.mark.parametrize("source,issue_types", [
    ("pos", ["invalid_sku", "null_store_id", "negative_quantity", "null_selling_price"]),
    ("orders", ["null_ordered_quantity"]),
    ("weather", ["sensor_error_99.9"]),
    ("inventory", ["negative_on_hand"]),
])
def test_injected_defects_are_quarantined(bronze, refs, issues, source, issue_types):
    spec = sv.SPECS[source]
    _, invalid = sv.split_valid_invalid(_checked(bronze, refs, source))
    bad = _ids(invalid, spec)
    if source == "weather":
        bad = {"|".join(i.split("|")[:2]) for i in bad}
    for t in issue_types:
        expected = set(issues.loc[(issues.source == source) & (issues.issue_type == t), "record_key"])
        assert expected and expected <= bad, (t, len(expected - bad))


def test_quarantine_rows_have_rules_reason_and_raw_record(bronze, refs):
    from pyspark.sql import functions as F
    spec = sv.SPECS["pos"]
    _, invalid = sv.split_valid_invalid(_checked(bronze, refs, "pos"))
    q = sv.to_quarantine_rows(invalid, spec, "run_t")
    assert q.where(F.col("rule_name").isNull() | (F.col("rule_name") == "") | F.col("rejection_reason").isNull()
                   | F.col("record_json").isNull()).count() == 0
    rules = {r[0] for r in q.select(F.explode("failed_rules")).distinct().collect()}
    assert {"sku_unknown", "store_id_null", "quantity_negative", "selling_price_null"} <= rules


def test_keying_spikes_are_not_quarantined(bronze, refs, issues):
    """x50 keying errors are plausible values; they're handled by outlier detection, not rejected here."""
    spec = sv.SPECS["pos"]
    valid, _ = sv.split_valid_invalid(_checked(bronze, refs, "pos"))
    spikes = set(issues.loc[(issues.source == "pos") & (issues.issue_type == "data_entry_spike_x50"), "record_key"])
    assert spikes <= _ids(valid, spec)


def test_alt_date_formats_parsed_not_rejected(bronze, refs, issues):
    from pyspark.sql import functions as F
    spec = sv.SPECS["sales"]
    checked = _checked(bronze, refs, "sales")
    valid, invalid = sv.split_valid_invalid(checked)
    alt = set(issues.loc[issues.issue_type == "alt_date_format_dd.MM.yyyy", "record_key"])
    assert not (alt & {i.split("|")[0] for i in _ids(invalid, spec)})
    assert valid.where(F.col("posting_date").isNull()).count() == 0


def test_valid_rows_have_no_null_keys_or_dates(bronze, refs):
    from pyspark.sql import functions as F
    for source in ("pos", "orders", "sales", "pricing", "inventory"):
        spec = sv.SPECS[source]
        valid, _ = sv.split_valid_invalid(_checked(bronze, refs, source))
        cond = " OR ".join(f"{k} IS NULL" for k in spec.pk + list(spec.ri))
        assert valid.where(cond).count() == 0, source


def test_reconciliation_identity(bronze, refs):
    """bronze rows = valid rows + quarantined rows; silver rows = distinct valid keys."""
    for source in ("pos", "orders", "sales", "weather", "inventory"):
        spec = sv.SPECS[source]
        checked = _checked(bronze, refs, source)
        valid, invalid = sv.split_valid_invalid(checked)
        assert bronze[source].count() == valid.count() + invalid.count()
        silver_rows = sv.to_silver_rows(valid, spec, "r").count()
        assert silver_rows == valid.select(*spec.pk).distinct().count()
        assert silver_rows <= valid.count()


# ------------------------------------------------------------------ dedup / restatements / late
def test_restatements_win_and_resends_collapse(bronze, refs, issues):
    from pyspark.sql import functions as F
    spec = sv.SPECS["pos"]
    valid, _ = sv.split_valid_invalid(_checked(bronze, refs, "pos"))
    silver = sv.to_silver_rows(valid, spec, "r")
    assert silver.groupBy("transaction_id").count().where("count > 1").count() == 0
    restated = list(issues.loc[issues.issue_type == "restatement_latest_wins", "record_key"])
    b2 = (valid.where((F.col("_batch_id") == "batch_002") & F.col("transaction_id").isin(restated))
          .select("transaction_id", F.col("quantity").alias("q2")))
    joined = silver.join(b2, "transaction_id").where("quantity <> q2")
    assert b2.count() > 0 and joined.count() == 0      # Silver holds the corrected (batch_002) quantity


def test_late_arriving_rows_kept(bronze, refs, issues):
    spec = sv.SPECS["pos"]
    valid, _ = sv.split_valid_invalid(_checked(bronze, refs, "pos"))
    silver_ids = _ids(sv.to_silver_rows(valid, spec, "r"), spec)
    late = set(issues.loc[(issues.source == "pos") & (issues.issue_type == "late_arriving"), "record_key"])
    invalid_any = set(issues.loc[issues.issue_type.isin(["invalid_sku", "null_store_id", "negative_quantity",
                                                          "null_selling_price"]), "record_key"])
    assert (late - invalid_any) <= silver_ids


# ------------------------------------------------------------------ SCD2
def _snapshot(bronze, refs, batch):
    from pyspark.sql import functions as F
    spec = sv.SPECS["products"]
    valid, invalid = sv.split_valid_invalid(_checked(bronze, refs, "products"))
    assert invalid.count() == 0
    return sv.dedupe_latest(valid.where(F.col("_batch_id") == batch), spec.pk)


def test_scd2_stage_first_load_then_changes(spark, bronze, refs, issues):
    spec = sv.SPECS["products"]
    s1 = sv.scd2_stage(_snapshot(bronze, refs, "batch_001"), sv._empty_current(spark, spec), spec)
    assert s1.where("merge_key IS NULL").count() == 0           # first load: inserts via NOT MATCHED only
    current = s1.select("sku_id", "_tracked_hash", "effective_from")

    s2 = sv.scd2_stage(_snapshot(bronze, refs, "batch_002"), current, spec)
    new_versions = {r[0] for r in s2.where("merge_key IS NULL").select("sku_id").collect()}
    expected = set(issues.loc[issues.issue_type.isin(["scd2_lifecycle_change", "scd2_attribute_change_product_name"]),
                              "record_key"])
    assert new_versions == expected
    new_sku = set(issues.loc[issues.issue_type == "new_sku_in_batch_002", "record_key"])
    assert new_sku <= {r[0] for r in s2.select("sku_id").collect()} - {r[0] for r in current.select("sku_id").collect()}


def test_scd2_replay_of_older_delivery_does_not_roll_back(spark, bronze, refs):
    spec = sv.SPECS["products"]
    s1 = sv.scd2_stage(_snapshot(bronze, refs, "batch_001"), sv._empty_current(spark, spec), spec)
    cur1 = s1.select("sku_id", "_tracked_hash", "effective_from")
    s2 = sv.scd2_stage(_snapshot(bronze, refs, "batch_002"), cur1, spec)
    # current after batch_002 = new versions where they exist, else batch_001 version
    newv = s2.where("merge_key IS NULL").select("sku_id", "_tracked_hash", "effective_from")
    keep = cur1.join(newv.select("sku_id"), "sku_id", "left_anti")
    added = s2.where("merge_key IS NOT NULL").join(cur1.select("sku_id"), "sku_id", "left_anti") \
        .select("sku_id", "_tracked_hash", "effective_from")
    cur2 = keep.unionByName(newv).unionByName(added)
    replay = sv.scd2_stage(_snapshot(bronze, refs, "batch_001"), cur2, spec)
    assert replay.where("merge_key IS NULL").count() == 0


def test_scd2_type1_change_does_not_create_version(spark, bronze, refs):
    """sustainability_tag is new in batch_002 (type-1): it must not by itself create versions."""
    spec = sv.SPECS["products"]
    assert "sustainability_tag" in spec.scd2_type1 and "sustainability_tag" not in spec.scd2_tracked
    assert "last_modified" not in spec.scd2_tracked


# ------------------------------------------------------------------ SQL builders
def test_merge_sql_builders():
    f = sv.merge_fact_sql("`c`.`silver`.`orders`", "_v", ["order_id", "order_item"])
    assert "t.`order_id` = s.`order_id` AND t.`order_item` = s.`order_item`" in f
    assert "s._ingested_at >= t._ingested_at AND s._record_hash <> t._record_hash THEN UPDATE SET *" in f
    q = sv.merge_quarantine_sql("`c`.`silver`.`quarantine_records`", "_q")
    assert "WHEN NOT MATCHED THEN INSERT *" in q and "WHEN MATCHED" not in q
    s2 = sv.merge_scd2_sql("`c`.`silver`.`products_scd2`", "_s", sv.SPECS["products"])
    assert "t.is_current = false, t.effective_to = date_sub(s.effective_from, 1)" in s2
    assert "ON t.`sku_id` = s.merge_key AND t.is_current = true" in s2
    assert "DATE'9999-12-31'" in s2 and "xxhash64" in s2
    s1 = sv.merge_scd1_sql("`c`.`silver`.`stores`", "_v", ["store_id"])
    assert "t._record_hash <> s._record_hash THEN UPDATE SET *" in s1
