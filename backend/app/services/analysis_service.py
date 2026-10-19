"""Analysis orchestration (Phase 6).

Coordinates the PySpark processing layer with SQLite persistence and enforces
the data-integrity rules: only a dataset that has passed mapping + validation
(status ``ready_for_analysis`` with a persisted eligible file) may be processed,
and each analysis is bound to exactly one upload. Flask routes stay thin; Spark
logic lives in ``pyspark_service``; row access lives in the repositories.
"""
from __future__ import annotations

import logging
from pathlib import Path

from flask import current_app

from app.repositories import analysis_repository, upload_repository
from app.services import pyspark_service, rfm_service, segmentation_service

log = logging.getLogger("segmentation.analysis")


class AnalysisError(Exception):
    """Base class for analysis failures with a client-safe message."""
    http_status = 500


class UploadNotFound(AnalysisError):
    http_status = 404

    def __init__(self, message="Upload not found."):
        super().__init__(message)


class NotReadyForAnalysis(AnalysisError):
    http_status = 409


class SparkAnalysisFailed(AnalysisError):
    http_status = 500


class AnalysisNotFound(AnalysisError):
    http_status = 404

    def __init__(self, message="Analysis not found."):
        super().__init__(message)


def _eligible_path_or_raise(row) -> Path:
    report = upload_repository.latest_report(row["id"]) or {}
    eligible = report.get("eligible_path")
    if not eligible or not Path(eligible).exists():
        raise NotReadyForAnalysis(
            "No eligible dataset is available. Confirm mappings and run "
            "data-quality validation before analyzing."
        )
    return Path(eligible)


def run_upload_aggregation(upload_id: str) -> dict:
    """Run the Spark aggregation for a validated upload and persist base metrics.

    Returns a compact result dict. Raises an :class:`AnalysisError` subclass on
    any integrity or processing problem (never exposing paths or stack traces).
    """
    row = upload_repository.get(upload_id)
    if row is None:
        raise UploadNotFound()
    if row["status"] != "ready_for_analysis":
        raise NotReadyForAnalysis(
            "Only datasets that have passed data-quality validation can be analyzed."
        )

    eligible_path = _eligible_path_or_raise(row)

    settings = {"source": "eligible_csv", "engine": "pyspark"}
    analysis_id = analysis_repository.create_analysis(
        upload_id, settings=settings)

    analysis_repository.set_status(analysis_id, "running")
    try:
        result = pyspark_service.run_aggregation(
            eligible_path,
            master=current_app.config["SPARK_MASTER"],
            app_name=current_app.config["SPARK_APP_NAME"],
        )
    except pyspark_service.SparkProcessingError as exc:
        # exc message is already user-safe; the cause was logged by the service.
        analysis_repository.set_status(analysis_id, "failed", error_message=str(exc))
        raise SparkAnalysisFailed(str(exc))
    except Exception:
        log.exception("Unexpected failure running aggregation for upload %s", upload_id)
        analysis_repository.set_status(
            analysis_id, "failed", error_message="Analysis failed unexpectedly.")
        raise SparkAnalysisFailed("The analysis could not be completed.")

    transactions = result["transactions"]
    customers = result["customers"]
    reference_date = transactions.get("last_date")

    analysis_repository.store_customer_metrics(analysis_id, customers)
    summary = {
        "transactions": transactions,
        "customers_analyzed": len(customers),
        "settings": result["settings"],
    }
    analysis_repository.record_completed(analysis_id, summary, reference_date=reference_date)

    return {"analysis_id": analysis_id, "upload_id": upload_id,
            "status": "completed", **summary}


def get_analysis(analysis_id: str):
    return analysis_repository.get(analysis_id)


def list_for_upload(upload_id: str):
    return analysis_repository.list_for_upload(upload_id)


def run_rfm_for_analysis(analysis_id: str) -> dict:
    """Compute and persist deterministic RFM scores for a completed analysis.

    RFM is derived only from the validated Phase 6 aggregates tied to this
    analysis_id (never from raw uploads), so results cannot cross-contaminate
    other analyses. Recency anchors on ``analyses.reference_date`` (falling back
    to the most recent purchase date). Segment/cluster labels are Phase 8 and are
    intentionally left untouched here.
    """
    row = analysis_repository.get(analysis_id)
    if row is None:
        raise AnalysisNotFound()
    if row["status"] != "completed":
        raise NotReadyForAnalysis(
            "RFM analysis requires a completed aggregation. Run aggregation first."
        )

    reference_date, aggregates = analysis_repository.load_rfm_inputs(analysis_id)
    if not aggregates:
        # Valid but empty cohort: nothing to score (not an error).
        return {"analysis_id": analysis_id, "status": "completed",
                "reference_date": reference_date, "customers_scored": 0, "sample": []}

    updates = rfm_service.calculate_rfm(reference_date, aggregates)
    analysis_repository.update_rfm_scores(analysis_id, updates)

    return {
        "analysis_id": analysis_id,
        "upload_id": row["upload_id"],
        "status": "completed",
        "reference_date": reference_date,
        "customers_scored": len(updates),
        "sample": updates[:5],
    }


def run_segmentation_for_analysis(analysis_id: str) -> dict:
    """Assign interpretable RFM segments (and optional K-Means clusters).

    Requires a *completed* analysis whose RFM scores are persisted (run the RFM
    step first). Segments are rule-based and authoritative; K-Means, when enabled
    and evaluable, only adds an auxiliary cluster id and never overrides a
    segment. All writes are scoped to this analysis_id.
    """
    row = analysis_repository.get(analysis_id)
    if row is None:
        raise AnalysisNotFound()
    if row["status"] != "completed":
        raise NotReadyForAnalysis(
            "Segmentation requires a completed analysis. Run aggregation first."
        )

    inputs = analysis_repository.load_segmentation_inputs(analysis_id)
    if not inputs:
        return {"analysis_id": analysis_id, "status": "completed",
                "customers_segmented": 0, "segment_counts": {}, "clustering": None}

    if all(i["r_score"] is None for i in inputs):
        raise NotReadyForAnalysis(
            "RFM scores are required before segmentation. Run the RFM step first."
        )

    # Rule-based segments (always computed).
    rule_assignments = segmentation_service.assign_segments(inputs)
    by_customer = {a["customer_id"]: dict(a, cluster_label=None) for a in rule_assignments}

    # Optional K-Means (kept separate; never overrides a segment).
    clustering = None
    if current_app.config.get("SEGMENTATION_ENABLE_KMEANS"):
        clustering = segmentation_service.run_kmeans(
            inputs,
            k_max=current_app.config.get("KMEANS_MAX_K", 5),
            min_customers=current_app.config.get("KMEANS_MIN_CUSTOMERS", 10),
            seed=current_app.config.get("KMEANS_RANDOM_SEED", 42),
        )
        if clustering.get("ran"):
            for cid, label in clustering["cluster_by_customer"].items():
                if cid in by_customer:
                    by_customer[cid]["cluster_label"] = label

    assignments = list(by_customer.values())
    analysis_repository.update_segments(analysis_id, assignments)

    counts = segmentation_service.segment_counts(assignments)
    return {
        "analysis_id": analysis_id,
        "upload_id": row["upload_id"],
        "status": "completed",
        "customers_segmented": len(assignments),
        "segment_counts": counts,
        "clustering": (
            {"ran": clustering["ran"], "reason": clustering.get("reason", ""),
             "k": clustering.get("k"), "inertia_curve": clustering.get("inertia_curve", {})}
            if clustering is not None
            else {"ran": False, "reason": "disabled", "k": None, "inertia_curve": {}}
        ),
    }
