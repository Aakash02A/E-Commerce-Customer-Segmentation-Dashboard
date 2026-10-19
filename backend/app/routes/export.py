"""Export endpoints (Phase 10).

Downloadable CSV/Excel exports for a single *completed* analysis. Routes stay
thin: existence/status checks come from ``analysis_service`` and all assembly +
serialization lives in ``export_service`` (which reads only persisted results
scoped to the ``analysis_id``). Errors are clean JSON with safe HTTP codes; file
downloads use a whitelisted filename and never surface filesystem paths.
"""
from __future__ import annotations

import io

from flask import Blueprint, request, send_file

from app.services import analysis_service, export_service
from app.utils.response_utils import error_response

export_bp = Blueprint("export", __name__, url_prefix="/api")


@export_bp.get("/analyses/<analysis_id>/export")
def export_analysis(analysis_id):
    row = analysis_service.get_analysis(analysis_id)
    if row is None:
        return error_response("Analysis not found.", status=404)
    if row["status"] != "completed":
        return error_response(
            "Only a completed analysis can be exported. Run the analysis first.",
            status=409,
        )

    try:
        result = export_service.build_export(
            row,
            dataset=request.args.get("dataset", "full"),
            fmt=request.args.get("format", "xlsx"),
        )
    except export_service.ExportError as exc:
        return error_response(str(exc), status=exc.http_status)

    return send_file(
        io.BytesIO(result["bytes"]),
        mimetype=result["mimetype"],
        as_attachment=True,
        download_name=result["filename"],
    )
