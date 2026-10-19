"""CRUD for the `uploads` table (SQLite via stdlib sqlite3)."""
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone

from app.database.connection import get_db, transaction


# Columns that may be written through update(); guards against typos/injection.
_UPDATABLE_COLUMNS = {
    "original_filename", "stored_filename", "file_path", "mime_type", "file_size",
    "extension", "sheet_names", "selected_sheet", "columns_json", "mapping_json",
    "normalized_path", "status", "error_message",
}

# Valid workflow states for the status column.
VALID_STATUSES = {
    "uploaded", "mapping_required", "mapped", "validation_failed",
    "ready_for_analysis", "failed", "deleted",
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def create(original_filename, stored_filename, file_path, extension, size,
           sheet_names=None, status="uploaded") -> str:
    upload_id = uuid.uuid4().hex
    db = get_db()
    db.execute(
        """INSERT INTO uploads (id, original_filename, stored_filename, file_path,
               mime_type, file_size, extension, sheet_names, status, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (upload_id, original_filename, stored_filename, str(file_path), None,
         size, extension, json.dumps(sheet_names) if sheet_names else None,
         status, _now(), _now()),
    )
    db.commit()
    return upload_id


def get(upload_id: str) -> sqlite3.Row | None:
    return get_db().execute("SELECT * FROM uploads WHERE id = ?", (upload_id,)).fetchone()


def update(upload_id: str, **fields):
    """Update whitelisted columns for an upload.

    Raises ValueError on an unknown column or status so bad writes fail loudly
    instead of corrupting persistence.
    """
    if not fields:
        return
    unknown = set(fields) - _UPDATABLE_COLUMNS
    if unknown:
        raise ValueError(f"Attempt to update non-existent upload column(s): {sorted(unknown)}")
    if "status" in fields and fields["status"] is not None and fields["status"] not in VALID_STATUSES:
        raise ValueError(f"Invalid upload status: {fields['status']}")

    fields["updated_at"] = _now()
    cols = ", ".join(f"{k} = ?" for k in fields)
    vals = [_jsonize(v) for v in fields.values()] + [upload_id]
    db = get_db()
    db.execute(f"UPDATE uploads SET {cols} WHERE id = ?", vals)
    db.commit()


def set_json(upload_id: str, column: str, value):
    update(upload_id, **{column: json.dumps(value, default=str)})


def record_validation_result(upload_id: str, report: dict, status: str) -> int:
    """Atomically store a quality report and move the upload to `status`.

    The report INSERT and the upload status UPDATE commit together, so a failure
    partway through leaves neither persisted (no orphan report, no status that
    contradicts the stored report). Returns the new report id.
    """
    if status not in VALID_STATUSES:
        raise ValueError(f"Invalid upload status: {status}")
    summary = report["summary"]
    now = _now()
    with transaction() as db:
        cur = db.execute(
            """INSERT INTO data_quality_reports
                   (upload_id, report_json, total_rows, valid_rows, invalid_rows, passed, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (upload_id, json.dumps(report, default=str),
             summary["total_rows"], summary["valid_rows"], summary["invalid_rows"],
             1 if report["passed"] else 0, now),
        )
        db.execute(
            "UPDATE uploads SET status = ?, error_message = NULL, updated_at = ? WHERE id = ?",
            (status, now, upload_id),
        )
    return cur.lastrowid


def latest_report(upload_id: str) -> dict | None:
    rec = get_db().execute(
        "SELECT report_json FROM data_quality_reports WHERE upload_id = ? "
        "ORDER BY id DESC LIMIT 1", (upload_id,),
    ).fetchone()
    return json.loads(rec["report_json"]) if rec else None


def report_count(upload_id: str) -> int:
    return get_db().execute(
        "SELECT COUNT(*) c FROM data_quality_reports WHERE upload_id = ?", (upload_id,)
    ).fetchone()["c"]


def summary() -> dict:
    db = get_db()
    total = db.execute(
        "SELECT COUNT(*) c FROM uploads WHERE status != 'deleted'").fetchone()["c"]
    ready = db.execute(
        "SELECT COUNT(*) c FROM uploads WHERE status = 'ready_for_analysis'").fetchone()["c"]
    return {"total_uploads": total, "ready_for_analysis": ready}


def list_recent(limit: int = 50):
    return get_db().execute(
        "SELECT * FROM uploads WHERE status != 'deleted' "
        "ORDER BY created_at DESC, rowid DESC LIMIT ?", (limit,)
    ).fetchall()


def _jsonize(v):
    # Store dict/list as JSON text; leave scalars untouched.
    if isinstance(v, (dict, list)):
        return json.dumps(v, default=str)
    return v
