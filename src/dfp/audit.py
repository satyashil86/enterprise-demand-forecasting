"""Run logging into audit.pipeline_run_log (Spark required at call time only)."""
from __future__ import annotations

import json
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional

from .config import ProjectConfig


def new_run_id(prefix: str = "run") -> str:
    return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"


def _current_user(spark) -> Optional[str]:
    try:
        return spark.sql("SELECT current_user() AS u").first()["u"]
    except Exception:  # noqa: BLE001
        return None


def write_run_log(spark, cfg: ProjectConfig, row: Dict[str, Any]) -> None:
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("run_id", T.StringType(), False),
        T.StructField("env", T.StringType()),
        T.StructField("pipeline_step", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("started_at", T.TimestampType()),
        T.StructField("finished_at", T.TimestampType()),
        T.StructField("duration_seconds", T.DoubleType()),
        T.StructField("rows_processed", T.LongType()),
        T.StructField("details", T.StringType()),
        T.StructField("error_message", T.StringType()),
        T.StructField("executed_by", T.StringType()),
    ])
    spark.createDataFrame([row], schema).write.mode("append").saveAsTable(
        cfg.table("audit.pipeline_run_log"))


@contextmanager
def run_logged(spark, cfg: ProjectConfig, step: str, run_id: Optional[str] = None) -> Iterator[Dict[str, Any]]:
    """Context manager: yields a mutable dict for rows_processed/details; logs success or failure."""
    ctx: Dict[str, Any] = {"run_id": run_id or new_run_id(step), "rows_processed": None, "details": {}}
    started = datetime.now(timezone.utc)
    t0 = time.time()
    status, err = "SUCCEEDED", None
    try:
        yield ctx
    except Exception as exc:  # noqa: BLE001
        status, err = "FAILED", f"{type(exc).__name__}: {exc}"[:4000]
        raise
    finally:
        write_run_log(spark, cfg, {
            "run_id": ctx["run_id"], "env": cfg.env, "pipeline_step": step, "status": status,
            "started_at": started.replace(tzinfo=None),
            "finished_at": datetime.now(timezone.utc).replace(tzinfo=None),
            "duration_seconds": round(time.time() - t0, 3),
            "rows_processed": ctx["rows_processed"],
            "details": json.dumps(ctx["details"], default=str)[:8000],
            "error_message": err, "executed_by": _current_user(spark),
        })
