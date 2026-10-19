"""Upload and preview endpoints (Phase 2)."""
from __future__ import annotations

import json

import pandas as pd
from flask import Blueprint, current_app, request

from app.repositories import upload_repository
from app.services import (
    column_mapping_service,
    dataset_service,
    file_reader_service,
    file_validation_service,
)
from app.utils.file_utils import safe_stored_filename, sanitize_display_name
from app.utils.response_utils import error_response, success_response

uploads_bp = Blueprint("uploads", __name__, url_prefix="/api/uploads")


@uploads_bp.post("")
def create_upload():
    """Accept a CSV/XLSX upload, validate it, and return preview + detected mapping."""
    if "file" not in request.files:
        return error_response("No file part named 'file' was included in the request.")

    file_storage = request.files["file"]
    cfg = current_app.config
    try:
        info = file_validation_service.validate_file(
            file_storage, cfg["ALLOWED_EXTENSIONS"], cfg["MAX_CONTENT_LENGTH"]
        )
    except file_validation_service.FileValidationError as exc:
        return error_response(str(exc), status=400)

    ext = info["extension"]
    stored_filename = None
    try:
        _ext, stored_filename = safe_stored_filename(file_storage.filename, cfg["ALLOWED_EXTENSIONS"])
        dest = cfg["UPLOAD_DIR"] / stored_filename
        file_storage.save(dest)
    except ValueError as exc:
        return error_response(str(exc), status=400)
    except Exception:
        if stored_filename:
            (cfg["UPLOAD_DIR"] / stored_filename).unlink(missing_ok=True)
        return error_response("Failed to store the uploaded file.", status=500)

    # Excel: enumerate sheets for the sheet selector.
    sheet_names = None
    if ext == "xlsx":
        try:
            sheet_names = file_reader_service.list_sheets(str(dest))
        except file_validation_service.FileValidationError as exc:
            dest.unlink(missing_ok=True)
            return error_response(str(exc), status=400)

    # Default to the first sheet so a preview/mapping can be produced immediately.
    selected_sheet = (sheet_names[0] if sheet_names else None)
    upload_id = upload_repository.create(
        original_filename=sanitize_display_name(info["original_filename"]),
        stored_filename=stored_filename,
        file_path=str(dest),
        extension=ext,
        size=info["size"],
        sheet_names=sheet_names,
        status="uploaded",
    )
    upload_repository.update(upload_id, selected_sheet=selected_sheet)

    row = upload_repository.get(upload_id)
    try:
        df = dataset_service.load_raw_dataframe(row)
    except file_reader_service.FileReaderError as exc:
        upload_repository.update(upload_id, status="failed", error_message=str(exc))
        dest.unlink(missing_ok=True)
        return error_response(str(exc), status=400)

    if df.empty:
        upload_repository.update(upload_id, status="failed", error_message="Dataset has no rows.")
        return error_response("The dataset contains no readable rows.", status=400)

    detected = column_mapping_service.detect_mapping(df)
    upload_repository.set_json(upload_id, "columns_json", detected)
    upload_repository.update(upload_id, status="mapping_required")

    return success_response(
        data={
            "upload_id": upload_id,
            "status": "mapping_required",
            "filename": row["original_filename"],
            "extension": ext,
            "sheet_names": sheet_names,
            "selected_sheet": selected_sheet,
            "row_count": int(len(df)),
            "column_count": int(df.shape[1]),
            "preview": _preview(df),
            "detected_mapping": detected,
        },
        message="File uploaded and validated. Please confirm column mappings.",
        status=201,
    )


@uploads_bp.get("/<upload_id>/preview")
def preview(upload_id):
    row = upload_repository.get(upload_id)
    if row is None:
        return error_response("Upload not found.", status=404)
    sheet = request.args.get("sheet")
    if sheet and row["extension"] == "xlsx":
        upload_repository.update(upload_id, selected_sheet=sheet)
        row = upload_repository.get(upload_id)
    try:
        df = dataset_service.load_raw_dataframe(row)
    except file_reader_service.FileReaderError as exc:
        return error_response(str(exc), status=400)
    return success_response(data={
        "upload_id": upload_id,
        "selected_sheet": row["selected_sheet"],
        "sheet_names": json.loads(row["sheet_names"]) if row["sheet_names"] else None,
        "row_count": int(len(df)),
        "preview": _preview(df),
    })


@uploads_bp.get("/<upload_id>/sheets")
def sheets(upload_id):
    row = upload_repository.get(upload_id)
    if row is None:
        return error_response("Upload not found.", status=404)
    names = json.loads(row["sheet_names"]) if row["sheet_names"] else []
    return success_response(data={"sheet_names": names, "selected_sheet": row["selected_sheet"]})


@uploads_bp.get("/summary")
def summary():
    return success_response(data=upload_repository.summary())


@uploads_bp.get("")
def list_uploads():
    """List recent datasets and their workflow status."""
    rows = upload_repository.list_recent(limit=50)
    return success_response(data={"uploads": [_row_to_dict(r) for r in rows]})


@uploads_bp.get("/<upload_id>")
def get_upload(upload_id):
    row = upload_repository.get(upload_id)
    if row is None:
        return error_response("Upload not found.", status=404)
    return success_response(data=_row_to_dict(row))


def _preview(df, n=10) -> dict:
    head = df.head(n)
    return {
        "columns": [str(c) for c in df.columns],
        "rows": [[None if pd.isna(v) else str(v) for v in rec] for rec in head.to_numpy()],
    }


def _row_to_dict(row) -> dict:
    return {
        "id": row["id"],
        "original_filename": row["original_filename"],
        "extension": row["extension"],
        "status": row["status"],
        "row_count": None,
        "sheet_names": json.loads(row["sheet_names"]) if row["sheet_names"] else None,
        "selected_sheet": row["selected_sheet"],
        "mapping": json.loads(row["mapping_json"]) if row["mapping_json"] else None,
        "error_message": row["error_message"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
