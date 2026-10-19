"""Persistence for analysis runs and their customer-level aggregates (Phase 6).

Wraps the `analyses` and `customer_metrics` tables. Kept free of Spark and
Flask concerns: it only reads/writes rows. Every metric row is tied to a
specific analysis_id, which is in turn tied to one upload_id, so results are
always isolated to the dataset that produced them.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone

from app.database.connection import get_db, transaction

VALID_STATUSES = {"pending", "running", "completed", "failed"}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def create_analysis(upload_id: str, reference_date: str | None = None,
                    settings: dict | None = None) -> str:
    """Create a pending analysis record for an upload. Returns the analysis id."""
    analysis_id = uuid.uuid4().hex
    db = get_db()
    db.execute(
        """INSERT INTO analyses (id, upload_id, status, reference_date, settings_json, created_at)
           VALUES (?, ?, 'pending', ?, ?, ?)""",
        (analysis_id, upload_id, reference_date,
         json.dumps(settings, default=str) if settings else None, _now()),
    )
    db.commit()
    return analysis_id


def get(analysis_id: str) -> sqlite3.Row | None:
    return get_db().execute(
        "SELECT * FROM analyses WHERE id = ?", (analysis_id,)).fetchone()


def list_for_upload(upload_id: str):
    return get_db().execute(
        "SELECT * FROM analyses WHERE upload_id = ? ORDER BY created_at DESC, rowid DESC",
        (upload_id,)).fetchall()


def set_status(analysis_id: str, status: str, error_message: str | None = None):
    if status not in VALID_STATUSES:
        raise ValueError(f"Invalid analysis status: {status}")
    fields = {"status": status}
    if status == "running":
        fields["started_at"] = _now()
    if status in ("completed", "failed"):
        fields["completed_at"] = _now()
    if error_message is not None:
        fields["error_message"] = error_message

    cols = ", ".join(f"{k} = ?" for k in fields)
    vals = list(fields.values()) + [analysis_id]
    db = get_db()
    db.execute(f"UPDATE analyses SET {cols} WHERE id = ?", vals)
    db.commit()


def record_completed(analysis_id: str, summary: dict, reference_date: str | None = None):
    """Store the compact summary (and optional anchor date) + mark completed."""
    now = _now()
    with transaction() as db:
        db.execute(
            "UPDATE analyses SET status='completed', completed_at=?, summary_json=?, "
            "reference_date=COALESCE(?, reference_date), error_message=NULL WHERE id=?",
            (now, json.dumps(summary, default=str), reference_date, analysis_id),
        )


def store_customer_metrics(analysis_id: str, metrics: list[dict]):
    """Replace the base customer aggregates for an analysis (idempotent re-run).

    Persists only Phase 6 aggregates (frequency, monetary, total_quantity,
    first/last order date, country). RFM scores, segments and cluster labels
    are intentionally left NULL for later phases. Runs in one transaction.
    """
    with transaction() as db:
        db.execute("DELETE FROM customer_metrics WHERE analysis_id = ?", (analysis_id,))
        rows = [
            (
                analysis_id, m["customer_id"], m.get("order_count"), m.get("total_spend"),
                m.get("total_quantity"), m.get("first_purchase_date"), m.get("last_purchase_date"),
                m.get("country"),
            )
            for m in metrics
        ]
        db.executemany(
            """INSERT OR REPLACE INTO customer_metrics
                   (analysis_id, customer_id, frequency, monetary, total_quantity,
                    first_order_date, last_order_date, country)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )


def get_metrics(analysis_id: str) -> list[sqlite3.Row]:
    return get_db().execute(
        "SELECT * FROM customer_metrics WHERE analysis_id = ? ORDER BY customer_id",
        (analysis_id,)).fetchall()


def metrics_count(analysis_id: str) -> int:
    return get_db().execute(
        "SELECT COUNT(*) c FROM customer_metrics WHERE analysis_id = ?", (analysis_id,)
    ).fetchone()["c"]


# --- RFM (Phase 7) -------------------------------------------------------
def load_rfm_inputs(analysis_id: str) -> tuple[str | None, list[dict]]:
    """Return (reference_date, [customer aggregates]) for RFM scoring.

    Aggregates expose exactly what the RFM service needs: frequency (distinct
    valid orders), monetary (total spend) and last_order_date.
    """
    row = get(analysis_id)
    if row is None:
        return None, []
    aggregates = [
        {
            "customer_id": m["customer_id"],
            "frequency": m["frequency"],
            "monetary": m["monetary"],
            "last_order_date": m["last_order_date"],
        }
        for m in get_metrics(analysis_id)
    ]
    return row["reference_date"], aggregates


def update_rfm_scores(analysis_id: str, updates: list[dict]):
    """Persist RFM values/scores for ONE analysis (never touches others).

    Scopes every UPDATE by analysis_id, so results cannot leak across analyses.
    Segment/cluster columns are intentionally not written here (Phase 8).
    """
    with transaction() as db:
        for u in updates:
            db.execute(
                "UPDATE customer_metrics SET recency_days=?, r_score=?, f_score=?, "
                "m_score=? WHERE analysis_id=? AND customer_id=?",
                (u["recency_days"], u["r_score"], u["f_score"], u["m_score"],
                 analysis_id, u["customer_id"]),
            )


# --- Segmentation (Phase 8) ---------------------------------------------
def load_segmentation_inputs(analysis_id: str) -> list[dict]:
    """Return the persisted RFM rows (scores + aggregates) for one analysis."""
    return [
        {
            "customer_id": m["customer_id"],
            "r_score": m["r_score"],
            "f_score": m["f_score"],
            "m_score": m["m_score"],
            "frequency": m["frequency"],
            "monetary": m["monetary"],
            "recency_days": m["recency_days"],
        }
        for m in get_metrics(analysis_id)
    ]


def update_segments(analysis_id: str, assignments: list[dict]):
    """Persist segment (and optional cluster) labels for ONE analysis.

    Every UPDATE is scoped by analysis_id and keyed by the
    UNIQUE(analysis_id, customer_id) constraint, so re-runs overwrite in place
    (no duplicates, no stale rows) and never touch another analysis. Passing an
    assignment without a ``cluster_label`` clears it, so disabling K-Means after
    a prior run cannot leave a stale cluster behind.
    """
    with transaction() as db:
        for a in assignments:
            db.execute(
                "UPDATE customer_metrics SET segment=?, cluster_label=? "
                "WHERE analysis_id=? AND customer_id=?",
                (a["segment"], a.get("cluster_label"), analysis_id, a["customer_id"]),
            )
