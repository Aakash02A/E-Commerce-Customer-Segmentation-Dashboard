"""Dashboard analytics assembly (Phase 9).

Turns *persisted* analysis results into the values a dashboard renders:
overview KPIs, segment breakdowns, RFM distributions and a paginated customer
table. This module is read-only and derives everything from ``analyses``
(``summary_json`` produced by the PySpark stage) and ``customer_metrics`` rows
scoped to a single ``analysis_id``. It never touches raw/unvalidated uploads,
never reprocesses data, and never changes segmentation rules.

Design notes for correctness
----------------------------
* Overview totals come from the Spark-wide transaction rollup (``distinct_orders``
  counts a shared invoice once across customers, so average order value is defined
  precisely as ``total_revenue / distinct_valid_orders``).
* "Unavailable" is represented as ``None`` / ``available: false`` and kept distinct
  from a real ``0``. A completed-but-empty cohort legitimately has 0 customers; an
  analysis that has not run RFM/segmentation reports those stages as not available.
* Percentages are relative to the *segmented* population, not all customers.
* Distributions/paging are computed in SQL (see ``analysis_repository``), so the
  whole customer set is never loaded into Python or the browser at once.
"""
from __future__ import annotations

import json

from app.repositories import analysis_repository
from app.services import segmentation_service

# Canonical display order + documented business meanings (mirrors the rule set in
# ``segmentation_service``). These are *proposed* marketing categories.
SEGMENT_DESCRIPTIONS = {
    segmentation_service.CHAMPIONS:
        "Bought very recently, buy often and spend the most. Reward loyalty.",
    segmentation_service.LOYAL:
        "Buy regularly and spend well. Good candidates for upselling.",
    segmentation_service.POTENTIAL_LOYALISTS:
        "Recent buyers with promising frequency/value. Nurture toward loyalty.",
    segmentation_service.RECENT:
        "Newest / most recent purchasers with limited history so far. Onboard them.",
    segmentation_service.AT_RISK:
        "Above-average frequency/value but not buying recently. Re-engage them.",
    segmentation_service.CANNOT_LOSE_THEM:
        "Formerly top customers, now inactive. High-value wins to recover.",
    segmentation_service.HIBERNATING:
        "Below-average recency, frequency and value. Low engagement.",
    segmentation_service.OTHERS:
        "Unclassified or missing RFM scores (fallback category).",
}
# Ordered by marketing value, so charts and legends read consistently.
SEGMENT_ORDER = [
    segmentation_service.CHAMPIONS, segmentation_service.LOYAL,
    segmentation_service.POTENTIAL_LOYALISTS, segmentation_service.RECENT,
    segmentation_service.AT_RISK, segmentation_service.CANNOT_LOSE_THEM,
    segmentation_service.HIBERNATING, segmentation_service.OTHERS,
]


def list_analyses() -> list[dict]:
    """Available analyses for the switcher (newest first)."""
    return [
        {
            "analysis_id": r["id"],
            "upload_id": r["upload_id"],
            "status": r["status"],
            "reference_date": r["reference_date"],
            "source_name": r["source_name"],
            "customers_analyzed": r["customers_analyzed"],
            "created_at": r["created_at"],
        }
        for r in analysis_repository.list_all_with_sources()
    ]


def _overview(analysis_row) -> dict:
    """KPIs from the Spark transaction rollup, with None = not-yet-available."""
    summary = json.loads(analysis_row["summary_json"]) if analysis_row["summary_json"] else {}
    tx = summary.get("transactions") or {}

    total_revenue = tx.get("total_spend")
    total_orders = tx.get("distinct_orders")
    if total_revenue is not None and total_orders:
        aov = round(total_revenue / total_orders, 2)
    else:
        aov = None

    total_customers = tx.get("distinct_customers")
    if total_customers is None:
        total_customers = None  # aggregation not run yet (distinct from 0)

    return {
        "available": analysis_row["status"] == "completed",
        "total_customers": total_customers,
        "total_orders": total_orders,
        "total_revenue": total_revenue,
        "total_quantity": tx.get("total_quantity"),
        "average_order_value": aov,
        "aov_definition": "Total revenue divided by distinct valid orders.",
        "date_range": {"first": tx.get("first_date"), "last": tx.get("last_date")},
    }


def segment_breakdown(analysis_id: str) -> dict:
    dist = analysis_repository.segment_distribution(analysis_id)
    total = sum(d["count"] for d in dist)
    by_name = {d["segment"]: d["count"] for d in dist}
    segments = []
    for seg in SEGMENT_ORDER:
        count = by_name.get(seg, 0)
        if count == 0:
            continue
        segments.append({
            "segment": seg,
            "count": count,
            "percentage": round(count * 100 / total, 1) if total else 0.0,
            "description": SEGMENT_DESCRIPTIONS.get(seg, ""),
        })
    segments.sort(key=lambda s: s["count"], reverse=True)
    return {"available": total > 0, "segmented_customers": total, "segments": segments}


def rfm_analytics(analysis_id: str, flags: dict) -> dict:
    available = flags["rfm_scored"] > 0
    distributions = {}
    if available:
        distributions = {
            metric: analysis_repository.score_distribution(analysis_id, metric)
            for metric in ("recency", "frequency", "monetary")
        }
    return {
        "scores_available": available,
        "distributions": distributions,
        "summaries": analysis_repository.metric_summaries(analysis_id),
    }


def build_dashboard(analysis_row) -> dict:
    """Assemble the complete dashboard payload for one analysis."""
    analysis_id = analysis_row["id"]
    flags = analysis_repository.stage_flags(analysis_id)
    return {
        "analysis": {
            "analysis_id": analysis_id,
            "upload_id": analysis_row["upload_id"],
            "status": analysis_row["status"],
            "reference_date": analysis_row["reference_date"],
            "source_name": analysis_repository.get_source_name(analysis_id),
            "error_message": analysis_row["error_message"],
        },
        "stages": {
            "aggregated": analysis_row["status"] == "completed",
            "rfm_scored": flags["rfm_scored"] > 0,
            "segmented": flags["segmented"] > 0,
            "rfm_scored_count": flags["rfm_scored"],
            "segmented_count": flags["segmented"],
            "total_customers": flags["total_customers"],
        },
        "overview": _overview(analysis_row),
        "segment_breakdown": segment_breakdown(analysis_id),
        "rfm": rfm_analytics(analysis_id, flags),
    }


def customers(analysis_id: str, *, search=None, segment=None, sort="monetary",
               direction="desc", page=1, page_size=20) -> dict:
    """A single page of the customer table (server-side filter/sort/paginate)."""
    page = max(1, int(page))
    page_size = min(100, max(1, int(page_size)))
    offset = (page - 1) * page_size
    items, total = analysis_repository.list_customers(
        analysis_id, search=(search or None), segment=(segment or None),
        sort=sort, direction=direction, limit=page_size, offset=offset)
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": (total + page_size - 1) // page_size if total else 0,
    }
