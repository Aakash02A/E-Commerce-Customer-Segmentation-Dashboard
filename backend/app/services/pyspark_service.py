"""PySpark processing layer (Phase 6).

Loads a *validated, eligible* transaction dataset (the CSV produced by the
data-quality stage) into Spark and computes:

  * transaction-level line totals (quantity x unit_price), and
  * customer-level aggregates (distinct order count, total quantity, total
    spend, first/most-recent purchase date).

This module is intentionally free of Flask and SQLite: it only turns an
eligible CSV into aggregated results. RFM scoring, segmentation, clustering and
recommendations are later phases and are NOT implemented here.

Design notes
------------
* Spark is slow to start, so a single session is lazily created and reused.
* The eligible CSV carries extra internal helper columns, so we read it by
  header name and cast each field explicitly rather than applying a positional
  schema (which would misalign columns).
* Only aggregated results are collected to the driver -- never the whole
  transaction set -- and dates are parsed to Spark DATE (no timezone) so
  first/last purchase dates are deterministic.
"""
from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

log = logging.getLogger("segmentation.spark")

# Fields the analysis stage must have to produce customer aggregates.
REQUIRED_COLUMNS = ("customer_id", "order_date", "quantity", "unit_price")

_SPARK_CONFIG = {
    "spark.sql.session.timeZone": "UTC",   # dates parsed to DATE -> tz-independent
    "spark.ui.enabled": "false",           # avoid port conflicts / overhead
    "spark.sql.shuffle.partitions": "8",
    "spark.driver.host": "127.0.0.1",      # avoid Windows hostname-binding issues
    # Legacy (non-ANSI) semantics: to_date/cast return NULL on unparseable input
    # instead of throwing, so defensive row filtering (not crashes) handles them.
    "spark.sql.ansi.enabled": "false",
}

_session: SparkSession | None = None


class SparkProcessingError(Exception):
    """Raised for recoverable processing problems with a user-safe message.

    The underlying cause (which may contain filesystem paths or stack details)
    is logged server-side, never surfaced to API clients.
    """


# --- Session lifecycle ---------------------------------------------------
def build_session(master: str = "local[*]", app_name: str = "CustomerSegmentationAnalytics") -> SparkSession:
    """Create (or return the active) Spark session with a hardened config.

    Uses ``getOrCreate`` so a live session is reused; call :func:`stop_session`
    to tear the shared session down. Raises :class:`SparkProcessingError` if the
    runtime cannot start (e.g. Java missing) with a clean message.
    """
    try:
        builder = SparkSession.builder.master(master).appName(app_name)
        for key, value in _SPARK_CONFIG.items():
            builder = builder.config(key, value)
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as exc:  # pragma: no cover - depends on runtime
        log.exception("Spark session failed to start")
        raise SparkProcessingError(
            "The analytics engine could not be started on this server."
        ) from exc


def get_session(master: str = "local[*]", app_name: str = "CustomerSegmentationAnalytics") -> SparkSession:
    """Return the shared lazily-created Spark session."""
    global _session
    if _session is None:
        _session = build_session(master, app_name)
    return _session


def stop_session() -> None:
    """Stop the shared session (if any) and reset the cache."""
    global _session
    if _session is not None:
        try:
            _session.stop()
        except Exception:  # pragma: no cover
            log.exception("Error while stopping Spark session")
        _session = None


def is_session_active() -> bool:
    return _session is not None


# --- Loading -------------------------------------------------------------
def _as_uri(path: str | Path) -> str:
    p = Path(path).resolve()
    return p.as_uri()


def load_eligible(spark: SparkSession, eligible_path: str | Path) -> tuple[DataFrame, bool]:
    """Load the eligible CSV into a typed DataFrame with a line-total column.

    Returns ``(dataframe, has_order_id)``. Raises SparkProcessingError when the
    file is missing or lacks required fields.
    """
    p = Path(eligible_path)
    if not p.exists():
        raise SparkProcessingError("The eligible dataset file is missing.")

    try:
        raw = (spark.read
               .option("header", "true")
               .option("inferSchema", "false")
               .option("mode", "PERMISSIVE")
               .csv(_as_uri(p)))
    except Exception as exc:
        log.exception("Failed to read eligible dataset")
        raise SparkProcessingError("The eligible dataset could not be read.") from exc

    missing = [c for c in REQUIRED_COLUMNS if c not in raw.columns]
    if missing:
        raise SparkProcessingError(
            f"The eligible dataset is missing required field(s): {', '.join(missing)}."
        )

    has_order_id = "order_id" in raw.columns
    order_id_col = F.trim(F.col("order_id")) if has_order_id else F.lit(None).cast(StringType())
    country_col = F.trim(F.col("country")) if "country" in raw.columns else F.lit(None).cast(StringType())

    df = (
        raw
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("order_id", order_id_col)
        .withColumn("country", country_col)
        # Parse the date robustly (timestamp form first, then bare date). DATE
        # has no timezone, so first/last purchase dates never shift.
        .withColumn(
            "order_date",
            F.coalesce(
                F.to_date("order_date", "yyyy-MM-dd HH:mm:ss"),
                F.to_date("order_date", "yyyy-MM-dd"),
            ),
        )
        .withColumn("quantity", F.col("quantity").cast(DoubleType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("line_total", F.col("quantity") * F.col("unit_price"))
    )

    # Defensive: the eligible set is pre-validated, but drop any row whose
    # required values did not survive parsing so aggregates stay reliable.
    df = df.filter(
        F.col("customer_id").isNotNull()
        & (F.col("customer_id") != "")
        & F.col("order_date").isNotNull()
        & F.col("quantity").isNotNull()
        & F.col("unit_price").isNotNull()
    )
    return df, has_order_id


# --- Transaction-level summary -------------------------------------------
def summarize_transactions(df: DataFrame, has_order_id: bool) -> dict:
    """Dataset rollup computed entirely in Spark (returns a single row)."""
    order_metric = F.countDistinct("order_id") if has_order_id else F.count(F.lit(1))
    row = df.agg(
        F.count(F.lit(1)).alias("transaction_count"),
        F.countDistinct("customer_id").alias("distinct_customers"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("line_total").alias("total_spend"),
        F.min("order_date").alias("first_date"),
        F.max("order_date").alias("last_date"),
        order_metric.alias("distinct_orders"),
    ).collect()[0]

    total_qty = _num(row["total_quantity"])
    total_spend = _num(row["total_spend"])
    tx = int(row["transaction_count"] or 0)
    orders = int(row["distinct_orders"] or 0) if has_order_id else tx
    return {
        "transaction_count": tx,
        "distinct_customers": int(row["distinct_customers"] or 0),
        "distinct_orders": orders,
        "total_quantity": total_qty,
        "total_spend": total_spend,
        "first_date": _iso(row["first_date"]),
        "last_date": _iso(row["last_date"]),
    }


# --- Customer-level aggregates -------------------------------------------
def aggregate_customers(df: DataFrame, has_order_id: bool) -> DataFrame:
    """Group transactions into per-customer aggregates (Spark-side)."""
    order_metric = (F.countDistinct("order_id") if has_order_id
                    else F.count(F.lit(1))).alias("order_count")
    return (
        df.groupBy("customer_id")
        .agg(
            order_metric,
            F.sum("quantity").alias("total_quantity"),
            F.round(F.sum("line_total"), 2).alias("total_spend"),
            F.min("order_date").alias("first_purchase_date"),
            F.max("order_date").alias("last_purchase_date"),
            F.max("country").alias("country"),
        )
    )


def collect_customer_metrics(agg_df: DataFrame) -> list[dict]:
    """Materialize the (bounded) per-customer aggregate rows for persistence.

    Only one row per customer is collected -- never the raw transactions.
    """
    rows = agg_df.orderBy("customer_id").collect()
    metrics = []
    for r in rows:
        metrics.append({
            "customer_id": r["customer_id"],
            "order_count": int(r["order_count"] or 0),
            "total_quantity": _num(r["total_quantity"]),
            "total_spend": _num(r["total_spend"]),
            "first_purchase_date": _iso(r["first_purchase_date"]),
            "last_purchase_date": _iso(r["last_purchase_date"]),
            "country": r["country"],
        })
    return metrics


# --- Orchestration -------------------------------------------------------
def run_aggregation(eligible_path: str | Path, master: str = "local[*]",
                    app_name: str = "CustomerSegmentationAnalytics") -> dict:
    """Compute transaction rollup + customer aggregates from an eligible CSV.

    Returns ``{"transactions": {...}, "customers": [ {...}, ... ], "settings": {...}}``.
    Raises :class:`SparkProcessingError` for missing files, absent required
    fields, or Spark failures.
    """
    spark = get_session(master, app_name)
    try:
        df, has_order_id = load_eligible(spark, eligible_path)
        transactions = summarize_transactions(df, has_order_id)
        customers = collect_customer_metrics(aggregate_customers(df, has_order_id))
    except SparkProcessingError:
        raise
    except Exception as exc:  # unexpected Spark/runtime failure
        log.exception("Spark aggregation failed")
        raise SparkProcessingError("The analytics processing failed.") from exc

    return {
        "transactions": transactions,
        "customers": customers,
        "settings": {"master": master, "has_order_id": has_order_id,
                     "spark_version": spark.version},
    }


# --- small helpers -------------------------------------------------------
def _num(value):
    if value is None:
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _iso(value):
    return value.isoformat() if value is not None else None
