"""Data validation and quality-report endpoints (Phase 4)."""
from __future__ import annotations

import json

from flask import Blueprint, current_app, request

from app.repositories import upload_repository
from app.services import data_quality_service, dataset_service
from app.utils.response_utils import error_response, success_response

validation_bp = Blueprint("validation", __name__, url_prefix="/api/uploads")


@validation_bp.get("/<upload_id>/validate")
def get_report(upload_id):
    row = upload_repository.get(upload_id)
    if row is None:
        return error_response("Upload not found.", status=404)
    return success_response(data={
        "upload_id": upload_id,
        "status": row["status"],
        "report": upload_repository.latest_report(upload_id),
    })


@validation_bp.post("/<upload_id>/validate")
def run_validation(upload_id):
    """Run structural + value validation and the quality report.

    Optional payload: { "rules": { ... } } to override business-rule handling.
    Blocks analysis when required fields are missing or data is unusable.
    """
    row = upload_repository.get(upload_id)
    if row is None:
        return error_response("Upload not found.", status=404)
    if not row["normalized_path"]:
        return error_response("Confirm column mappings before running validation.", status=409)

    payload = request.get_json(silent=True) or {}
    rules = payload.get("rules")
    mapping = json.loads(row["mapping_json"]) if row["mapping_json"] else {}

    try:
        df_typed = dataset_service.load_normalized_typed(row["normalized_path"])
    except Exception as exc:
        return error_response(f"Could not load the normalized dataset: {exc}", status=500)

    result = data_quality_service.validate(
        df_typed,
        mapping=mapping,
        rules=rules,
        price_outlier_std=current_app.config["PRICE_OUTLIER_STD_MULTIPLIER"],
    )

    # Write eligible rows to disk before recording the report. If this fails we
    # must NOT persist a "ready_for_analysis" status pointing at a missing file.
    eligible = result.pop("eligible_dataframe")
    new_status = "ready_for_analysis" if result["passed"] else "validation_failed"
    eligible_path = None
    if result["passed"]:
        eligible_path = current_app.config["PROCESSED_DIR"] / f"{upload_id}_eligible.csv"
        try:
            eligible.to_csv(eligible_path, index=False)
        except OSError as exc:
            return error_response(f"Could not persist eligible rows: {exc}", status=500)

    # Persist the report + status atomically (single transaction).
    report = {**result, "eligible_path": str(eligible_path) if eligible_path else None}
    try:
        upload_repository.record_validation_result(upload_id, report, new_status)
    except Exception as exc:  # DB error -> nothing was committed; report failure
        current_app.logger.exception("Failed to persist validation result")
        return error_response(f"Failed to persist validation result: {exc}", status=500)

    return success_response(
        data={"upload_id": upload_id, "status": new_status, "report": report},
        message=("Data passed validation and is ready for analysis."
                 if result["passed"] else
                 "Validation failed. Resolve the reported issues before analysis."),
        status=200 if result["passed"] else 422,
    )
