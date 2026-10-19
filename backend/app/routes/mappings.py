"""Column mapping endpoints (Phase 3)."""
from __future__ import annotations

import json

from flask import Blueprint, current_app, request

from app.repositories import upload_repository
from app.services import (
    column_mapping_service,
    dataset_service,
    file_reader_service,
    schema_normalization_service,
)
from app.utils.response_utils import error_response, success_response

mappings_bp = Blueprint("mappings", __name__, url_prefix="/api/uploads")


@mappings_bp.get("/<upload_id>/mapping")
def get_mapping(upload_id):
    row = upload_repository.get(upload_id)
    if row is None:
        return error_response("Upload not found.", status=404)
    detected = json.loads(row["columns_json"]) if row["columns_json"] else []
    confirmed = json.loads(row["mapping_json"]) if row["mapping_json"] else None
    return success_response(data={
        "upload_id": upload_id,
        "status": row["status"],
        "standard_fields": column_mapping_service.STANDARD_FIELDS,
        "detected_mapping": detected,
        "confirmed_mapping": confirmed,
    })


@mappings_bp.post("/<upload_id>/mapping")
def confirm_mapping(upload_id):
    """Validate + persist a user-confirmed mapping and write the normalized dataset.

    Payload: { "mapping": { "<source_column>": "<standard_field>" | "", ... } }
    """
    row = upload_repository.get(upload_id)
    if row is None:
        return error_response("Upload not found.", status=404)

    payload = request.get_json(silent=True) or {}
    mapping = payload.get("mapping")
    if not isinstance(mapping, dict) or not mapping:
        return error_response("A 'mapping' object is required.", status=400)

    try:
        df = dataset_service.load_raw_dataframe(row)
    except file_reader_service.FileReaderError as exc:
        return error_response(str(exc), status=400)

    result = column_mapping_service.validate_mapping(df, mapping)
    if not result["ok"]:
        return error_response(
            "The mapping is incomplete or conflicting. Please correct it and continue.",
            status=400, details=result,
        )

    normalized = column_mapping_service.normalize_dataframe(df, mapping)

    # Persist the normalized (standard-schema, still string-typed) dataset as CSV.
    # Plain CSV keeps the artifact dependency-free and directly readable by the
    # later PySpark stage; type coercion is re-applied at validation time.
    processed_dir = current_app.config["PROCESSED_DIR"]
    norm_path = processed_dir / f"{upload_id}_normalized.csv"
    normalized.to_csv(norm_path, index=False)

    upload_repository.update(
        upload_id,
        mapping_json=mapping,
        normalized_path=str(norm_path),
        status="mapped",
        error_message=None,
    )

    typed_preview = schema_normalization_service.normalize_types(normalized)
    return success_response(
        data={
            "upload_id": upload_id,
            "status": "mapped",
            "mapped_fields": result["mapped_fields"],
            "normalized_columns": list(normalized.columns),
            "row_count": int(len(normalized)),
            "dtypes": schema_normalization_service.coerce_summary(typed_preview),
        },
        message="Mapping confirmed and data normalized. You can now run validation.",
    )
