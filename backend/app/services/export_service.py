"""CSV / Excel export assembly (Phase 10).

Turns the *persisted* results of one completed analysis into downloadable
spreadsheet files. This module is read-only and reuses the same sources the
dashboard renders from — ``dashboard_service.build_dashboard`` for the overview
and segment breakdown, and ``analysis_repository.get_metrics`` for the customer
rows — so exported values are always identical to those displayed and never
come from a raw/unvalidated upload.

Exported datasets
-----------------
* ``overview``   KPI key/value pairs (analysis metadata, totals, AOV + its
  definition, analyzed date range).
* ``customers``  one row per customer: aggregates + RFM scores + segment +
  optional cluster label.
* ``segments``   one row per populated segment: count, share (%), description.
* ``full``       an Excel workbook combining the three sheets above. CSV has no
  notion of multiple sheets, so ``full`` is Excel-only.

Security
--------
* **Formula injection:** a spreadsheet cell whose text begins with ``=``, ``+``,
  ``-``, ``@``, TAB or CR can be executed by Excel/Sheets. Every string cell is
  neutralized by prefixing a single quote so it is treated as literal text. This
  mirrors the OWASP recommendation and is applied to both CSV and XLSX.
* **Safe filenames:** filenames are built only from a fixed whitelist plus the
  analysis id (a hex uuid), never from user text, so path traversal / control
  characters cannot reach ``Content-Disposition``.
* Numbers and booleans are exported as real numeric cells and are never touched
  by the string sanitizer (so a legitimate negative number is preserved).
"""
from __future__ import annotations

import csv
import io

from openpyxl import Workbook

from app.repositories import analysis_repository
from app.services import dashboard_service

# --- Formats / datasets -------------------------------------------------
FORMATS = ("csv", "xlsx")
DATASETS = ("customers", "segments", "overview", "full")

# Pass the bare type to send_file(mimetype=...): Flask appends charset=utf-8 for
# text/* types itself, so including it here would duplicate the charset parameter.
CSV_MIMETYPE = "text/csv"
XLSX_MIMETYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

# --- Column models (headers + the row builders keep them in sync) --------
CUSTOMER_HEADERS = [
    "Customer ID", "First Order Date", "Last Order Date", "Recency (days)",
    "Frequency (orders)", "Monetary (total spend)", "Total Quantity",
    "R Score", "F Score", "M Score", "Segment", "Cluster Label", "Country",
]
SEGMENT_HEADERS = ["Segment", "Customers", "Share %", "Description"]
OVERVIEW_HEADERS = ["Metric", "Value"]

# Leading characters that turn a cell into a formula in a spreadsheet.
_UNSAFE_PREFIX = ("=", "+", "-", "@", "\t", "\r")


class ExportError(Exception):
    """Base export failure carrying a client-safe message + HTTP status."""
    http_status = 500


class InvalidExportRequest(ExportError):
    http_status = 400


class ExportFailed(ExportError):
    http_status = 500


def _neutralize(value):
    """Prefix a single quote to text cells that could be interpreted as a
    formula. Non-string cells (numbers/booleans/None) pass through untouched."""
    if isinstance(value, str) and value and value[0] in _UNSAFE_PREFIX:
        return "'" + value
    return value


def _csv_cell(value):
    if value is None:
        return ""
    return _neutralize(value)


def _xl_cell(value):
    # openpyxl writes None as a blank cell and numbers as numeric cells.
    return _neutralize(value)


# --- Row builders -------------------------------------------------------
def customer_rows(analysis_id: str) -> list[list]:
    """One row per customer, ordered by customer id (deterministic)."""
    rows = []
    for m in analysis_repository.get_metrics(analysis_id):
        rows.append([
            m["customer_id"], m["first_order_date"], m["last_order_date"],
            m["recency_days"], m["frequency"], m["monetary"], m["total_quantity"],
            m["r_score"], m["f_score"], m["m_score"], m["segment"],
            m["cluster_label"], m["country"],
        ])
    return rows


def segment_rows(dash: dict) -> list[list]:
    return [
        [s["segment"], s["count"], s["percentage"], s["description"]]
        for s in dash["segment_breakdown"]["segments"]
    ]


def overview_rows(analysis_row, dash: dict) -> list[list]:
    ov = dash["overview"]
    info = dash["analysis"]
    return [
        ["Analysis ID", analysis_row["id"]],
        ["Source Dataset", info["source_name"]],
        ["Status", analysis_row["status"]],
        ["Reference Date", analysis_row["reference_date"]],
        ["Total Customers", ov["total_customers"]],
        ["Total Valid Orders", ov["total_orders"]],
        ["Total Revenue", ov["total_revenue"]],
        ["Total Quantity", ov["total_quantity"]],
        ["Average Order Value", ov["average_order_value"]],
        ["AOV Definition", ov["aov_definition"]],
        ["Date Range Start", ov["date_range"]["first"]],
        ["Date Range End", ov["date_range"]["last"]],
    ]


def _sheet_for(dataset: str, analysis_row, dash: dict) -> tuple[str, list, list]:
    """Return (title, headers, rows) for a single concrete dataset."""
    if dataset == "customers":
        return ("Customers", CUSTOMER_HEADERS,
                customer_rows(analysis_row["id"]))
    if dataset == "segments":
        return ("Segments", SEGMENT_HEADERS, segment_rows(dash))
    if dataset == "overview":
        return ("Overview", OVERVIEW_HEADERS, overview_rows(analysis_row, dash))
    raise InvalidExportRequest(f"Unknown export dataset: {dataset}")


# --- Serializers --------------------------------------------------------
def _to_csv_bytes(headers: list, rows: list[list]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\r\n")
    writer.writerow(headers)
    for row in rows:
        writer.writerow([_csv_cell(v) for v in row])
    # utf-8-sig emits a BOM so Excel opens UTF-8 text correctly.
    return buf.getvalue().encode("utf-8-sig")


def _to_xlsx_bytes(sheets: list[tuple[str, list, list]]) -> bytes:
    wb = Workbook()
    for index, (title, headers, rows) in enumerate(sheets):
        ws = wb.active if index == 0 else wb.create_sheet()
        ws.title = title[:31]
        ws.append(list(headers))
        for row in rows:
            ws.append([_xl_cell(v) for v in row])
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


def _safe_filename(analysis_id: str, dataset: str, ext: str) -> str:
    """Filename from a validated dataset token + hex analysis id only."""
    hex_part = "".join(ch for ch in (analysis_id or "") if ch.isalnum())[:12]
    return f"analysis_{hex_part or 'export'}_{dataset}.{ext}"


# --- Public entrypoint --------------------------------------------------
def build_export(analysis_row, dataset: str = "full", fmt: str = "xlsx") -> dict:
    """Assemble one export file.

    Returns ``{"bytes", "filename", "mimetype"}``. Raises
    :class:`InvalidExportRequest` for an unknown format/dataset (or ``full``
    requested as CSV, which has no multi-sheet notion) and :class:`ExportFailed`
    if serialization itself fails.
    """
    fmt = (fmt or "xlsx").lower()
    dataset = (dataset or "full").lower()
    if fmt not in FORMATS:
        raise InvalidExportRequest(f"Unsupported export format: {fmt}")
    if dataset not in DATASETS:
        raise InvalidExportRequest(f"Unknown export dataset: {dataset}")

    dash = dashboard_service.build_dashboard(analysis_row)
    analysis_id = analysis_row["id"]

    try:
        if fmt == "csv":
            if dataset == "full":
                raise InvalidExportRequest(
                    "CSV export requires a specific dataset; use the Excel "
                    "format to export all sheets at once.")
            title, headers, rows = _sheet_for(dataset, analysis_row, dash)
            data = _to_csv_bytes(headers, rows)
            ext = "csv"
            mimetype = CSV_MIMETYPE
        else:
            if dataset == "full":
                sheets = [
                    _sheet_for(name, analysis_row, dash)
                    for name in ("overview", "customers", "segments")
                ]
            else:
                sheets = [_sheet_for(dataset, analysis_row, dash)]
            data = _to_xlsx_bytes(sheets)
            ext = "xlsx"
            mimetype = XLSX_MIMETYPE
    except InvalidExportRequest:
        raise
    except Exception:  # serialization failure -> clean, non-leaking 500
        raise ExportFailed("The export could not be generated.")

    return {
        "bytes": data,
        "filename": _safe_filename(analysis_id, dataset, ext),
        "mimetype": mimetype,
    }
