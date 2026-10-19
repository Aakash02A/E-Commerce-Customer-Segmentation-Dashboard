"""Dashboard endpoints (Phase 9).

Read-only analytics views for a selected analysis. Routes stay thin: existence
and status checks come from ``analysis_service``, all aggregation lives in
``dashboard_service`` (which reads only persisted results scoped to the
``analysis_id``). Invalid/missing analysis ids return clean 404s; no stack traces
or filesystem paths are ever surfaced.
"""
from __future__ import annotations

from flask import Blueprint, request

from app.services import analysis_service, dashboard_service
from app.utils.response_utils import error_response, success_response

dashboard_bp = Blueprint("dashboard", __name__, url_prefix="/api")


@dashboard_bp.get("/analyses")
def list_all_analyses():
    """Analyses available for the dashboard switcher (newest first)."""
    return success_response(data={"analyses": dashboard_service.list_analyses()})


@dashboard_bp.get("/analyses/<analysis_id>/dashboard")
def dashboard_payload(analysis_id):
    row = analysis_service.get_analysis(analysis_id)
    if row is None:
        return error_response("Analysis not found.", status=404)
    return success_response(data=dashboard_service.build_dashboard(row))


@dashboard_bp.get("/analyses/<analysis_id>/customers")
def dashboard_customers(analysis_id):
    row = analysis_service.get_analysis(analysis_id)
    if row is None:
        return error_response("Analysis not found.", status=404)
    args = request.args
    try:
        data = dashboard_service.customers(
            analysis_id,
            search=args.get("search"),
            segment=args.get("segment"),
            sort=args.get("sort", "monetary"),
            direction=args.get("direction", "desc"),
            page=args.get("page", 1),
            page_size=args.get("page_size", 20),
        )
    except (TypeError, ValueError):
        return error_response("Invalid pagination or filter parameters.", status=400)
    return success_response(data=data)
