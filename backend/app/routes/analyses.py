"""Analysis endpoints (Phase 6): trigger Spark aggregation for a validated upload.

Routes stay thin; all orchestration lives in ``analysis_service``, all Spark
work in ``pyspark_service``, all persistence in the repositories. Errors are
translated to safe HTTP responses (never stack traces or filesystem paths).
"""
from __future__ import annotations

import json

from flask import Blueprint

from app.repositories import analysis_repository
from app.services import analysis_service
from app.services.analysis_service import AnalysisError
from app.utils.response_utils import error_response, success_response

analyses_bp = Blueprint("analyses", __name__, url_prefix="/api")


@analyses_bp.post("/uploads/<upload_id>/aggregate")
def aggregate(upload_id):
    try:
        result = analysis_service.run_upload_aggregation(upload_id)
    except AnalysisError as exc:
        return error_response(str(exc), status=exc.http_status)
    return success_response(
        data=result,
        message="Aggregation complete. Customer-level metrics are ready.",
        status=201,
    )


@analyses_bp.get("/uploads/<upload_id>/analyses")
def list_analyses(upload_id):
    rows = analysis_service.list_for_upload(upload_id)
    return success_response(data={"analyses": [_row_to_dict(r) for r in rows]})


@analyses_bp.get("/analyses/<analysis_id>")
def get_analysis(analysis_id):
    row = analysis_service.get_analysis(analysis_id)
    if row is None:
        return error_response("Analysis not found.", status=404)
    return success_response(data=_row_to_dict(row))


@analyses_bp.post("/analyses/<analysis_id>/rfm")
def run_rfm(analysis_id):
    """Score the persisted customer aggregates for one completed analysis."""
    try:
        result = analysis_service.run_rfm_for_analysis(analysis_id)
    except AnalysisError as exc:
        return error_response(str(exc), status=exc.http_status)
    return success_response(
        data=result, message="RFM scoring complete. Customer scores are ready."
    )


@analyses_bp.get("/analyses/<analysis_id>/metrics")
def analysis_metrics(analysis_id):
    row = analysis_service.get_analysis(analysis_id)
    if row is None:
        return error_response("Analysis not found.", status=404)
    metrics = [dict(m) for m in analysis_repository.get_metrics(analysis_id)]
    return success_response(data={"analysis_id": analysis_id,
                                   "reference_date": row["reference_date"],
                                   "metrics": metrics})


def _row_to_dict(row):
    return {
        "id": row["id"],
        "upload_id": row["upload_id"],
        "status": row["status"],
        "reference_date": row["reference_date"],
        "summary": json.loads(row["summary_json"]) if row["summary_json"] else None,
        "error_message": row["error_message"],
        "customers_analyzed": analysis_repository.metrics_count(row["id"]),
        "created_at": row["created_at"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
    }
