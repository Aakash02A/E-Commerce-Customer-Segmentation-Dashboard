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
from app.services import pyspark_service

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
