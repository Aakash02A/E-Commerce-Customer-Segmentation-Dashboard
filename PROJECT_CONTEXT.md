# PROJECT_CONTEXT.md
# E-Commerce Customer Segmentation & Analytics Dashboard — Complete Project Specification

**Document type:** Product requirements, technical architecture, implementation rules, and AI coding-agent instructions \
**Project name:** E-Commerce Customer Segmentation & Analytics Dashboard \
**Project type:** Full-stack data-analytics web application (server-rendered, MVP local deployment) \
**Primary domain:** Big Data Analytics / customer segmentation \
**Secondary domains:** Data engineering, data quality/validation, RFM analytics, optional clustering, data visualization \
**Development approach:** Incremental, phase-gated, test-driven, security-conscious, honesty-first 

---

## 1. Project Overview

### 1.1 What is this project?

The E-Commerce Customer Segmentation & Analytics Dashboard is a full-stack web
application that lets a business upload its own transaction dataset, validate and map its
columns, process the data through a PySpark pipeline, compute RFM metrics, assign
interpretable customer segments, and (in later phases) explore results through an
interactive dashboard and exportable reports.

The application combines:

- Flask (application factory + blueprints) for the backend and server-rendered pages.
- Jinja templates, Bootstrap 5, vanilla JavaScript, and Chart.js for the frontend.
- Pandas and NumPy for ingestion, normalization, and deterministic analytics.
- Apache PySpark for scalable, DataFrame-based transaction aggregation.
- SQLite (Python `sqlite3`, no ORM) for metadata and compact analytical results.

This must be a real application, not a static dashboard or a demonstration filled with
hardcoded numbers. All analytics originate from the uploaded, validated dataset.

### 1.2 Problem statement

Different companies export transaction data with different column names and data-quality
characteristics. Analysts need a way to upload a raw transaction file, reconcile its
columns to a standard schema, understand and control which records are valid, and then
derive trustworthy customer-behavior metrics and segments — without hand-writing
per-dataset scripts or trusting silently corrupted input.

This project addresses that by enforcing a documented pipeline: upload → validate →
map → normalize → quality-gate → Spark aggregation → RFM → segmentation → dashboard → export.

### 1.3 Project objectives

1. Accept user-uploaded transaction datasets in CSV and XLSX.
2. Detect and map arbitrary column names to a fixed standard schema, with user correction.
3. Validate data structure and values, applying documented, configurable business rules.
4. Block analysis when required fields are missing or the dataset is fundamentally unusable.
5. Process eligible transactions at scale with PySpark and compute per-customer aggregates.
6. Calculate RFM metrics and deterministic, documented, mutually exclusive segments.
7. Optionally run reproducible K-Means clustering without overriding rule-based segments.
8. Persist analysis metadata and compact customer-level results in SQLite with isolation.
9. Present results through a real multi-page interface driven by API data (Phase 9).
10. Export customer lists and segment summaries (Phase 10).
11. Include automated tests and honest, reproducible verification instructions.

### 1.4 What the project is not

This project is not intended to be:

- A React/FastAPI/MySQL single-page application. (The approved stack is Flask + Jinja +
  Bootstrap + SQLite + PySpark; see Section 4.)
- A distributed Spark cluster management platform or a big-data infrastructure demo.
- An enterprise multi-tenant SaaS with authentication (not yet; see Section 11).
- A predictive churn/AI model. Recommendations, if added, must be rule-based and clearly
  distinguished from predictions unless a trained, evaluated model genuinely exists.

Do not claim capabilities that have not been implemented and tested. In particular, do not
claim the application is distributed, cluster-based, or enterprise-scale merely because it
uses PySpark, and do not present raw cluster numbers as meaningful categories without
interpretation.

---

## 2. Target Users and Usage

The application is aimed at e-commerce analysts, small-business operators, students, and
developers who need customer-behavior insight from transaction data without building
bespoke pipelines.

Typical use cases:

- Upload a store's order history and reconcile its columns to a standard schema.
- Identify invalid, cancelled, returned, or duplicate records before analysis.
- Segment customers by RFM to understand value and attrition.
- Review which customers are at risk versus which are champions.
- Reopen previously saved analyses and export results for spreadsheets or reports.

The current MVP is a **local, single-user** application. There is no authentication, user
profiling, role management, or multi-tenancy. Do not introduce these unless explicitly
approved as a later requirement (see Section 11 for the security consequences of this
scope).

---

## 3. Functional Requirements

### FR-01: Data file upload

Users must be able to:

- Upload a CSV (`.csv`) or Excel Open XML (`.xlsx`) transaction file.
- See validation feedback for unsupported, empty, corrupted, oversized, or unreadable files.
- Select the worksheet to read from an XLSX workbook with multiple sheets.
- Preview the parsed dataset (columns, sample rows, detected dtypes).
- See clear status messages: uploaded, mapping required, validated/failed, ready, completed.

Supported formats: CSV and XLSX. `.xls` and JSON are optional future extensions. PDF is not
a primary input format because transaction tables in PDFs require a separate extraction and
verification workflow. Do not claim arbitrary formats are supported.

### FR-02: File validation and safe storage

The backend must validate file extension **and** actual content where practical, enforce a
configurable maximum upload size, and treat the uploaded filename as untrusted. Requirements:

- Generate safe internal (UUID-based) filenames and prevent path traversal.
- Keep uploads isolated under a configurable storage directory.
- Handle empty, corrupted, unreadable, and password-protected files with controlled errors.
- Handle differing CSV delimiters and encodings where practical (CSV read as strings).
- Define retention/deletion behavior (see `UPLOAD_RETENTION_DAYS`).
- Never execute uploaded files or macros.

Implemented and tested in Phases 0–2 (`file_validation_service.py`, `file_reader_service.py`,
`file_utils.py`).

### FR-03: Automatic column detection and mapping

Different companies label equivalent data differently (`CustomerID`, `Client`,
`Buyer_Code`; `InvoiceDate`, `OrderDate`, `Purchase_Date`; `Quantity`, `Qty`, `Units`;
`UnitPrice`, `Price`, `Unit_Cost`). The application must not require identical column names.

- Normalize column names (case, spaces, underscores, punctuation, abbreviations).
- Use a configurable synonym dictionary plus data-type and sample-value pattern detection.
- Assign conservative confidence scores; do not rely on names alone (`Price` could be unit
  price, order total, discount, or shipping).
- Do not auto-map ambiguous columns without confirmation.

Implemented in Phase 3 (`column_mapping_service.py`).

### FR-04: Interactive mapping confirmation

A dedicated mapping screen must show, per column: original name, suggested standard field,
confidence/match status, sample values, validation status, a dropdown to change the mapping,
and an option to mark optional fields unused. The user must be able to correct every
uncertain mapping. Prevent two incompatible standard fields from mapping to the same source
column and require user action when required data is missing or ambiguous.

Implemented in Phase 3 (`column_mapping.html` + `column_mapping.js`, GET/POST
`/api/uploads/<id>/mapping`).

### FR-05: Standard internal schema

All recognized input columns normalize to a fixed internal schema:

| Standard field | Meaning | Requirement |
|---|---|---|
| `customer_id` | Customer identifier | Required |
| `order_id` | Invoice/transaction identifier | Strongly recommended; required for order-level analytics |
| `order_date` | Purchase timestamp or date | Required |
| `quantity` | Units purchased | Required |
| `unit_price` | Price per unit | Required |
| `product_id` | Product identifier | Optional |
| `product_description` | Product name/description | Optional |
| `country` | Customer/transaction country | Optional |
| `transaction_status` | Completed/cancelled/returned | Optional |

The minimum schema for RFM is customer ID, purchase date, and a defensible measure of
purchase value and frequency. For transaction-based RFM, order ID should normally be present
so frequency counts **distinct orders**, not line items. If order ID is unavailable, do not
silently count each row as an order — explain the limitation and request suitable data or
offer a clearly labeled alternative.

Implemented in Phase 3/4 (`schema_normalization_service.py`, `dataset_service.py`).

### FR-06: Data validation and quality report (mandatory gate)

Validation must run before analysis is enabled.

- **Structural:** required fields exist; selected columns actually exist; compatible types;
  duplicate column detection; empty-dataset rejection; unreadable-structure rejection.
- **Value:** missing customer IDs; missing/invalid dates; non-numeric quantity/price; zero or
  negative quantities/prices; duplicate candidates; cancelled/returned/refunded records;
  suspicious values (prices beyond a configurable outlier threshold).
- **Business rules (documented, non-destructive, configurable):** a negative quantity may be
  a return, a negative value may be a refund, duplicate rows may be legitimate repeated lines,
  and cancelled invoices should not contribute to completed-purchase metrics.

The report must show rows inspected, valid, invalid, duplicate candidates, missing-value
counts, warnings/errors, and excluded records with reasons, and preserve an audit summary of
applied rules. **Block analysis when required fields are missing or the dataset is
fundamentally unusable.** Optional fields must not block analysis.

Implemented in Phase 4 (`data_quality_service.py`), verified end-to-end: 20 rows → 17 valid,
3 excluded, 11 distinct customers, status → `ready_for_analysis`.

### FR-07: PySpark processing pipeline

Implement only after mapping and validation are complete and tested.

1. Load only the validated, eligible dataset (never the raw upload).
2. Build a Spark DataFrame with explicit casts and timezone-independent DATE parsing.
3. Apply documented business rules for cancellations and returns.
4. Compute line totals: `line_total = quantity × unit_price`.
5. Aggregate to customer level: distinct order count (frequency), total quantity, total spend
   (monetary), and first/last purchase dates.
6. Collect only bounded aggregates (no full-dataset collect; `pyarrow` is intentionally absent,
   so `toPandas()` is avoided).
7. Persist compact results for SQLite and dashboard use.

Implemented in Phase 6 (`pyspark_service.py`, `analysis_service.py`).

### FR-08: RFM analysis

Implement RFM as the primary segmentation input.

- **Recency:** whole days between each customer's most recent purchase and an explicit,
  reproducible analysis reference date (fallback: cohort max purchase date); negatives clamped
  to 0.
- **Frequency:** count of distinct valid orders per customer.
- **Monetary:** total eligible spend per customer.
- **Scoring (deterministic, documented):** quintile bins 1..5 via the cohort's average
  percentile rank. Higher is better for Frequency and Monetary; **Recency is inverted** (fewer
  days → higher R). Identical values receive identical scores (ties), results are independent
  of input order, small/1-customer cohorts compress to the neutral middle bin (3), and
  missing/`None` metrics are excluded and scored `None` rather than crashing.

Implemented in Phase 7 (`rfm_service.py` + persistence + `POST /api/analyses/<id>/rfm`).

### FR-09: Customer segmentation

Map persisted RFM scores to interpretable segment labels. The proposed business categories
are: **Champions, Loyal Customers, Potential Loyalists, Recent Customers, At Risk, Cannot Lose
Them, Hibernating, Others.**

- Assignment is deterministic and documented: customers are partitioned by a recency tier,
  then an F/M ladder where the first matching rule wins, so every scored customer receives
  **exactly one** label with no overlaps.
- A customer missing any R/F/M score falls back to **Others** (never dropped or crashed).

Implemented in Phase 8 (`segmentation_service.py` + `POST /api/analyses/<id>/segment`), with a
full 5×5×5 grid test proving every combination yields exactly one valid label and all eight
labels are reachable.

### FR-10: Optional K-Means clustering

K-Means is optional, separate from, and must never override the rule-based segment.

- Use NumPy (no new dependency, no sklearn). Standardize features (z-score, zero-variance-safe).
- Use a reproducible seed and distinct-centroid initialization.
- Select `k` by an inertia **elbow** evaluation over `2..min(k_max, distinct)` — not arbitrary —
  and reject degenerate/collapsed runs.
- Skip with an explicit reason when customers < `KMEANS_MIN_CUSTOMERS` (10), distinct feature
  vectors < 3, all features are zero-variance, or any input has missing values.
- **Off by default** (`SEGMENTATION_ENABLE_KMEANS=0`). Do not present raw cluster numbers as
  meaningful categories without evaluation.

Implemented in Phase 8. Rule segments remain authoritative whether or not clustering runs.

### FR-11: Multi-page dashboard (implemented — Phase 9)

A complete multi-page interface, not a single landing page.

- Navigation: Home, Upload, Mapping, Data Quality, Analytics Dashboard, Customer Segments,
  Customer Details, Analysis History, Settings/About. (All page routes and templates exist;
  `dashboard.html`, `customer_segments.html`, and `customer_details.html` now render real
  results for a selected analysis via `/api/*`, with meaningful empty/unavailable states when
  no data or stage is present.)
- KPI cards: total customers analyzed, total eligible orders, total eligible revenue, average
  order value, average customer spend, repeat-customer rate when reliably computable.
- Visualizations (Chart.js, driven by real API data): segment distribution, revenue by segment,
  purchase trends, frequency vs monetary, RFM score distribution, segment performance.
- Filters (date range, segment, country) must update metrics/charts consistently; no
  non-functional filters.
- Customer details: id, last purchase, days since, distinct orders, total value, RFM scores,
  segment — with pagination/search.

Charts must be generated from API responses, never hardcoded example data.

**Implemented in Phase 9 (verified).** Overview KPIs (customers, orders, revenue, quantity,
average order value = revenue ÷ distinct valid orders, date range), segment distribution
(counts + percentages, doughnut + table), RFM analytics (score distributions and
recency/frequency/monetary summaries), and a server-side customer explorer (search, segment
filter, column sort, pagination) are all driven from **persisted** analysis results scoped by
`analysis_id` (`GET /api/analyses`, `/api/analyses/<id>/dashboard`, `/api/analyses/<id>/customers`).
Analyses can be switched without mixing results, and unavailable stages are surfaced as
"unavailable" rather than fake zeros. **Not in the Phase 9 MVP** (still required before their
phase): date-range/country filters, revenue-by-segment and purchase-trend charts, repeat-customer
rate, and rule-based recommendations (deferred; see Section 21).

### FR-12: CSV/Excel export (implemented — Phase 10)

Export a user-selected **completed** analysis, scoped to one `analysis_id` and built only from
persisted results (never a raw/unvalidated upload). `GET /api/analyses/<id>/export` accepts
`format=csv|xlsx` and `dataset=customers|segments|overview|full`.

- **Customers** — per-customer aggregates, RFM scores (R/F/M), segment assignment and optional
  cluster label; **Segments** — count, share (%) and description; **Overview** — KPI key/value
  pairs (revenue, distinct valid orders, AOV with its explicit definition, analyzed date range).
  `full` produces a multi-sheet Excel workbook (Overview + Customers + Segments); CSV requires a
  concrete dataset.
- Requirements met: meaningful headers, UTF-8 (CSV written with a BOM for Excel), RFC-4180
  escaping, **spreadsheet formula-injection mitigation** (text cells leading with `=`, `+`, `-`,
  `@`, TAB or CR are neutralized with a leading quote), values identical to those displayed (same
  `dashboard_service` / repository source of truth), and correct handling of empty results
  (a headers-only file, not an error). Download filenames are built only from a whitelisted
  dataset token plus the hex analysis id (no user text, no path separators). Errors are clean:
  `404` unknown analysis, `409` non-completed analysis, `400` invalid format/dataset.

### FR-13: Analysis history and persistence

Persist analysis metadata and compact results in SQLite (see Section 9). Requirements:
save status/timestamps, save confirmed mapping and quality summaries, save customer-level
aggregates when practical, reopen previously completed analyses, handle interrupted/failed
attempts, prevent one analysis from overwriting another, and define retention/deletion
behavior. Do not store full raw datasets or unnecessary sensitive values.

Implemented in Phase 5, with a two-process restart survival test.

### FR-14: Health and readiness

`GET /api/health` reports that the Flask process is running with a simple JSON status and
without exposing credentials, internal paths, or configuration.

---

## 4. Technology Stack

Use the following technologies unless a documented compatibility issue requires an approved
change.

### Frontend

- HTML5, CSS3
- Bootstrap 5
- Vanilla JavaScript (ES6+)
- Chart.js for visualizations
- Jinja templates rendered by Flask (server-side; no build step)

### Backend

- Python 3.13
- Flask (application factory + blueprints)
- Flask-Cors

### Analytics and big-data processing

- Pandas and NumPy (ingestion, normalization, deterministic analytics)
- Python standard-library date/time
- Apache PySpark (DataFrame aggregation)
- Optional K-Means implemented with NumPy (no scikit-learn dependency)

### Database

- SQLite via Python `sqlite3` (no ORM, no Alembic)
- Schema managed by `schema.sql` plus an idempotent additive-column migration at init

### Testing and tooling

- pytest and the Flask test client
- Git/GitHub
- Local Windows 11 compatibility

### Pinned versions (verified on this machine)

Python 3.13.14, Java 21.0.12 LTS, PySpark 4.2.0, Flask 3.0.3, pandas 2.2.3, numpy 2.5.0,
Flask-Cors 4.0.1, openpyxl, pytest 8.3.3. **`pyarrow` was intentionally removed** after a
repository-wide check confirmed no direct or indirect use (data IO is CSV + openpyxl; Spark
reads CSV with its own parser).

**Do not introduce React, Node.js backends, FastAPI, MySQL, MongoDB, scikit-learn, or
unnecessary cloud services.** Do not silently change the approved stack.

---

## 5. High-Level Architecture

The application follows a layered architecture with strict separation between HTTP,
orchestration, pure computation, and persistence.

### Request/data flow

1. A user interacts with a Flask-rendered page (Bootstrap + vanilla JS).
2. The browser calls a JSON API under `/api/...`.
3. A thin route validates the request and calls a service.
4. Ingestion services read and validate the file; mapping and quality services normalize and
   gate the dataset; state is persisted via repositories.
5. When the dataset is `ready_for_analysis`, `analysis_service` runs the PySpark aggregation,
   then `rfm_service` scores RFM, then `segmentation_service` assigns segments (and optional
   clusters).
6. Repositories persist compact results to SQLite, scoped by `analysis_id`.
7. Routes return structured JSON; the dashboard renders it with Chart.js (Phase 9).

### Layer responsibilities

**Routes (blueprints)** — Receive HTTP requests, validate/coordinate, call services, return
structured JSON or HTML, convert known failures to appropriate status codes. Routes contain
no analytical implementation.

**Services** — Application/business logic. Split into:
- Pure, framework-free computation: `rfm_service.py`, `segmentation_service.py`,
  `pyspark_service.py` (no Flask, no SQLite).
- Ingestion and gating: `file_validation_service.py`, `file_reader_service.py`,
  `column_mapping_service.py`, `schema_normalization_service.py`, `data_quality_service.py`,
  `dataset_service.py`.
- Orchestration: `analysis_service.py` enforces the staged pipeline integrity gate
  (aggregate → RFM requires a `completed` analysis → segment requires RFM scores) and typed
  error statuses (404/409).

**Repositories** — All SQLite queries and persistence. `upload_repository.py` and
`analysis_repository.py`. Every write in the analytics path is transactional and scoped by
`analysis_id` to guarantee cross-analysis isolation. No SQL lives in routes or in pure
services.

**Database layer** — `database/connection.py` (shared connection factory with
`PRAGMA foreign_keys=ON`, `busy_timeout`, a `transaction()` context manager, and additive
migration), `database/schema.sql`, `database/init_db.py`.

**Core configuration** — `config.py` loads environment-overridable settings (paths, upload
limits, quality thresholds, Spark, segmentation flags) with dev/testing/production classes.

**Utilities** — `utils/file_utils.py` (safe filenames), `utils/logging_utils.py`,
`utils/response_utils.py`.

---

## 6. Repository Structure

This reflects the **actual** repository. Files marked *(planned)* do not yet exist and must
not be documented as if they do.

```text
E-Commerce-Customer-Segmentation-Dashboard/
├── PROJECT_CONTEXT.md
├── ARCHITECTURE.md
├── TASKS.md
├── README.md
├── LICENSE
│
├── frontend/
│   ├── templates/
│   │   ├── base.html
│   │   ├── index.html
│   │   ├── upload.html
│   │   ├── column_mapping.html
│   │   ├── data_validation.html
│   │   ├── dashboard.html              # analytics dashboard (Phase 9)
│   │   ├── customer_segments.html      # segment distribution + drill-down (Phase 9)
│   │   ├── customer_details.html       # per-customer explorer (Phase 9)
│   │   ├── analysis_history.html
│   │   ├── settings.html
│   │   ├── _steps.html
│   │   └── errors/ (400.html, 404.html, 500.html)
│   └── static/
│       ├── css/style.css
│       └── js/ (main.js, upload.js, column_mapping.js, validation.js, notifications.js,
│               charts.js, dashboard.js, customer_segments.js, customer_details.js)
│
├── backend/
│   ├── run.py                          # entrypoint
│   ├── restart_check.py                # two-process persistence smoke test
│   ├── requirements.txt
│   ├── .env.example
│   └── app/
│       ├── __init__.py                 # application factory + error handlers + health
│       ├── config.py
│       ├── routes/
│       │   ├── pages.py                # HTML pages
│       │   ├── uploads.py              # upload/preview/sheets/summary
│       │   ├── mappings.py             # GET/POST mapping
│       │   ├── validation.py           # GET/POST validate
│       │   ├── analyses.py             # aggregate, rfm, segment, metrics
│       │   ├── dashboard.py            # GET /api/analyses, /<id>/dashboard, /<id>/customers
│       │   └── export.py               # GET /api/analyses/<id>/export (CSV/XLSX download)
│       ├── services/
│       │   ├── file_validation_service.py
│       │   ├── file_reader_service.py
│       │   ├── column_mapping_service.py
│       │   ├── schema_normalization_service.py
│       │   ├── data_quality_service.py
│       │   ├── dataset_service.py
│       │   ├── pyspark_service.py
│       │   ├── rfm_service.py
│       │   ├── segmentation_service.py
│       │   ├── analysis_service.py
│       │   ├── dashboard_service.py        # Phase 9 read-only dashboard assembly
│       │   ├── recommendation_service.py   # (planned Phase 11+)
│       │   └── export_service.py           # Phase 10 CSV/XLSX export (formula-injection safe)
│       ├── repositories/
│       │   ├── upload_repository.py
│       │   └── analysis_repository.py
│       ├── database/
│       │   ├── connection.py
│       │   ├── init_db.py
│       │   └── schema.sql
│       └── utils/
│           ├── file_utils.py
│           ├── logging_utils.py
│           └── response_utils.py
│
├── database/ecommerce.db               # created at runtime
├── storage/ (uploads/, processed/, exports/, temp/)
├── sample_data/ (sample_retail.csv, sample_retail.xlsx)
└── tests/ (see Section 14)
```

Do not create empty placeholder modules "just to match" this tree; create files when the
relevant phase needs them.

---

## 7. API Contract

All API responses use consistent JSON and appropriate HTTP status codes. JSON error bodies
follow `{"success": false, "message": <str>, "details": <obj|null>}`; HTML requests receive
error pages instead. Centralized handling lives in the app factory.

### Health

- `GET /api/health` — liveness; returns `{"success": true, "status": "ok", ...}`.

### Uploads (`/api/uploads`)

- `POST /api/uploads` — upload a file; creates an `uploads` record; returns upload id/status.
- `GET /api/uploads` — list recent uploads.
- `GET /api/uploads/summary` — aggregate counts by status.
- `GET /api/uploads/<upload_id>` — one upload's metadata.
- `GET /api/uploads/<upload_id>/preview` — parsed preview (columns, sample rows, dtypes).
- `GET /api/uploads/<upload_id>/sheets` — worksheet names for an XLSX.

### Mapping (`/api/uploads`)

- `GET /api/uploads/<upload_id>/mapping` — detected columns + suggested mapping + confidence.
- `POST /api/uploads/<upload_id>/mapping` — confirm/correct mapping → normalize → persist
  standard-schema CSV.

### Validation (`/api/uploads`)

- `GET /api/uploads/<upload_id>/validate` — latest quality report.
- `POST /api/uploads/<upload_id>/validate` — run validation; write report + status in one
  transaction; set `ready_for_analysis` when it passes.

### Analyses (`/api`)

- `POST /api/uploads/<upload_id>/aggregate` — run the PySpark pipeline for a validated upload;
  creates a `completed` analysis with `reference_date` and base aggregates.
- `GET /api/uploads/<upload_id>/analyses` — analyses for an upload.
- `GET /api/analyses/<analysis_id>` — one analysis.
- `POST /api/analyses/<analysis_id>/rfm` — compute and persist RFM scores (requires `completed`).
- `POST /api/analyses/<analysis_id>/segment` — assign segments + optional clusters (requires
  RFM scores).
- `GET /api/analyses/<analysis_id>/metrics` — per-customer metrics (RFM, segment, cluster).

### Dashboard (`/api`) — Phase 9, read-only, scoped by `analysis_id`

- `GET /api/analyses` — list analyses for the switcher (id, source name, status, reference date,
  customers analyzed), newest first.
- `GET /api/analyses/<analysis_id>/dashboard` — assemble the dashboard payload (overview KPIs,
  segment breakdown + percentages, RFM distributions/summaries, stage flags) from persisted
  results. `404` for an unknown analysis id.
- `GET /api/analyses/<analysis_id>/customers` — server-side paged/filtered/sorted customer table
  (`search`, `segment`, `sort`, `direction`, `page`, `page_size`). `400` for invalid params,
  `404` for an unknown analysis id. Aggregation and paging happen in SQL, so full cohorts are
  never loaded into the browser.

### Pipeline status values

Uploads: `uploaded → mapping_required → mapped → validation_failed → ready_for_analysis`
(and `failed`, `deleted`). Analyses: `pending → running → completed` (and `failed`).

### Integrity and error rules

Analytics endpoints enforce the staged gate and return clean status codes: `404` when the
analysis does not exist, `409` when prerequisites are unmet (e.g. segment before RFM), and
user-safe errors for Spark/processing failures. Never return stack traces, filesystem paths,
credentials, or internal exception details to clients. Prevent one analysis from reading or
overwriting another's data.

---

## 8. PySpark Design and Performance

PySpark is included because transaction aggregation and big-data processing are part of the
project, but it brings Java-compatibility, startup-latency, and memory concerns.

### Required principles

1. Keep all Spark logic inside `pyspark_service.py`; it stays independent of Flask and SQLite.
2. Manage the session through a shared, lazily initialized singleton — never create a new
   Spark session per request, and never launch multiple contexts accidentally.
3. Hardened session settings: `spark.sql.ansi.enabled=false`, UTC, Spark UI off,
   `driver.host=127.0.0.1`.
4. Load **only** the validated, eligible dataset (never the raw upload), read by header name
   with explicit casts, and parse dates without timezone rollover.
5. Collect only bounded aggregates. Because `pyarrow` is intentionally absent, avoid
   `toPandas()`; results are collected as small aggregate rows.
6. Surface failures as a user-safe `SparkProcessingError` with no leaked stack traces/paths.
7. Verify numerical correctness against known expected results; keep the analytics service
   independent of the transport layer.

### Runtime compatibility (verified)

Before integration, the installed runtimes were inspected and confirmed compatible: Python
3.13.14, Java 21.0.12 LTS, PySpark 4.2.0. Spark `groupBy/agg/collect` was verified to work
without `pyarrow`. Do not upgrade or downgrade runtimes blindly.

### Honesty about scale

Do not claim the application is distributed, cluster-based, or enterprise-scale. Local
execution is used. Do not claim PySpark makes small datasets faster; its value here is
scalable processing and learning distributed operations. Keep the Spark Web UI (dev-time
diagnostics at `http://localhost:4040`) separate from the business dashboard, do not assume
port 4040 is always available, and never expose it publicly.

---

## 9. SQLite and Data Persistence

SQLite (Python `sqlite3`, no ORM) stores application metadata and compact analytical results.

### Schema (implemented)

- **`uploads`** — id (TEXT PK), original/stored filename, file_path, mime_type, file_size,
  extension, sheet_names (JSON), selected_sheet, columns_json, mapping_json, normalized_path,
  status, error_message, created_at, updated_at. Indexed on status and created_at.
- **`data_quality_reports`** — autoincrement id, upload_id (FK, `ON DELETE CASCADE`),
  report_json (audit of applied rules), total/valid/invalid rows, passed flag, created_at.
- **`analyses`** — id (TEXT PK), upload_id (FK, cascade), status, `reference_date` (RFM anchor),
  settings_json, summary_json, error_message, started_at, completed_at, created_at.
- **`customer_metrics`** — autoincrement id, analysis_id (FK, cascade), customer_id,
  `recency_days`, `frequency`, `monetary`, `total_quantity`, `first_order_date`,
  `last_order_date`, `r_score`, `f_score`, `m_score`, `segment`, `cluster_label`, `country`,
  with `UNIQUE (analysis_id, customer_id)` and indexes for analysis and segment lookups.

### Persistence principles

- Connection factory enforces `PRAGMA foreign_keys=ON` and `busy_timeout`; a `transaction()`
  context manager wraps multi-write operations so they succeed or roll back together.
- Every analytics write is scoped by `analysis_id` and keyed by `UNIQUE(analysis_id,
  customer_id)`, so reruns overwrite in place (no duplicates, no stale clusters) and never
  touch another analysis.
- Schema changes are **additive and idempotent** (`ALTER TABLE ADD COLUMN` applied safely at
  init). RFM Phase 7 and segmentation Phase 8 required **no** new migration because their
  columns already existed as nullable additive columns.
- `schema.sql` uses `IF NOT EXISTS`; keep it safe to re-run.
- Do not store full raw datasets or unnecessary sensitive values.
- Never commit database credentials; there are none for SQLite (a file DB). Do not silently
  switch databases when a connection fails.

---

## 10. Frontend Design Requirements

The frontend is a real, server-rendered Flask/Jinja multi-page application using Bootstrap 5,
vanilla JavaScript, and Chart.js. It is not a React SPA and must not be converted to one.

### Structure

- `base.html` provides the layout, sidebar navigation, and shared asset blocks.
- `_steps.html` shows pipeline progress. Each page is a dedicated template with a focused
  JavaScript module (`upload.js`, `column_mapping.js`, `validation.js`, and — Phase 9 —
  `dashboard.js`, `customer_segments.js`, `customer_details.js`, `charts.js`). `main.js` handles
  shared behavior; `notifications.js` handles user feedback.
- Data flows through the `/api/...` JSON endpoints; pages call them with `fetch`/XHR.

### UX requirements

- Professional, modern analytics interface — a usable tool, not a promotional landing page.
- Responsive on desktop and mobile; accessible labels/controls; consistent metrics display.
- Visible loading, empty, success, and error states; useful validation messages.
- Upload progress and processing status; confirmation before destructive actions.
- **No fake data presented as real results; no broken/inert controls; no decorative
  non-functional buttons; no charts that do not reflect the active analysis.**

As of Phase 9, `dashboard.html`, `customer_segments.html`, and `customer_details.html` render
values derived from a selected analysis's persisted results (never placeholder numbers), and
show honest empty/unavailable states when there is no analysis, an unrun stage, or no customers
matching a filter.

---

## 11. Security Requirements

Security is part of implementation, not a final cosmetic phase.

### Upload and file security

- Enforce configurable size limits and content-based extension checks (do not trust the
  filename or client MIME alone).
- Generate UUID-based safe stored filenames; prevent path traversal; keep temporary uploads
  isolated; define retention/deletion behavior; never execute uploaded files or macros.

### Data handling

- Use parameterized SQL for every query.
- Render uploaded/derived text as text, not executable HTML; avoid unsafe injection.
- Do not expose database credentials, internal filesystem paths, full stack traces, or
  sensitive customer records through API responses.
- Keep uploaded data and analysis results accessible only through their own ids; never leak
  one analysis's data into another.

### Multi-user caution

The current MVP is a **local single-user** application with no authentication. This is
documented, not hidden. If multiple companies/users ever share a deployment, authentication
and server-side ownership checks are mandatory before exposing it — a public, unauthenticated
deployment of this MVP would leak other users' uploads, results, and history. Do not add
auth, tenants, or sessions without explicit approval; do not pretend the MVP is multi-tenant
safe.

### Spark/UI

- Do not expose the Spark Web UI publicly. Do not run the Flask development server in
  production without evaluating a production WSGI setup.

---

## 12. Configuration and Environment

Deployment-specific settings are environment-overridable and centralized in `config.py`
(dev/testing/production classes). `backend/.env.example` documents required settings with
placeholders only; the real `.env` is ignored.

Key settings and defaults:

- Paths: `DATABASE_PATH` (`database/ecommerce.db`), `UPLOAD_DIR`, `PROCESSED_DIR`,
  `EXPORTS_DIR`, `TEMP_DIR` (under `storage/`).
- Upload: `MAX_CONTENT_LENGTH` (100 MB), `ALLOWED_EXTENSIONS` = {csv, xlsx},
  `UPLOAD_RETENTION_DAYS` (30).
- Data quality: `PRICE_OUTLIER_STD_MULTIPLIER` (6.0).
- Spark: `SPARK_MASTER` (`local[*]`), `SPARK_APP_NAME` (`CustomerSegmentationAnalytics`).
- Segmentation: `SEGMENTATION_ENABLE_KMEANS` (off), `KMEANS_RANDOM_SEED` (42),
  `KMEANS_MAX_K` (5), `KMEANS_MIN_CUSTOMERS` (10).
- Misc: `SECRET_KEY` (dev-local-single-user), `FLASK_ENV` (development).
- Security/CSP (Phase 11): `SECURITY_HEADERS_ENABLED` (1), `CDN_ORIGIN`
  (`https://cdn.jsdelivr.net`), `CSP_FRAME_ANCESTORS` (`'none'`), `X_FRAME_OPTIONS` (DENY),
  `REFERRER_POLICY` (no-referrer). Invalid CSP/header values fail fast at startup.
- Rate limiting (Phase 11): `RATE_LIMIT_ENABLED` (1), `RATE_LIMIT_WINDOW_SECONDS` (60),
  `RATE_LIMIT_EXPENSIVE` (10), `RATE_LIMIT_STANDARD` (30) — in-memory, per-process, per-IP.
- Servers (Phase 11): `WAITRESS_THREADS` (4) for `serve.py`; Gunicorn is launched via
  `wsgi:app` (see §13). The rate limiter does **not** trust `X-Forwarded-For` by default.

Validate configuration at startup and provide actionable errors for missing required
settings. Never put secrets in frontend assets.

---

## 13. Deployment

The application is a **local single-user MVP** (no authentication). See
[DEPLOYMENT.md](DEPLOYMENT.md) for the full operations guide. Do not expose it publicly
without authn/authz and per-user isolation (§11 multi-user caution).

Development (Flask dev server):

```bash
pip install -r backend/requirements.txt
cd backend && python run.py            # http://127.0.0.1:5000 (dev only)
```

Production — a real WSGI server, never the dev server:

```powershell
# Windows: Waitress (ident="" suppresses the Server header)
cd backend && python serve.py
```

```bash
# Linux/Docker: Gunicorn (1 worker because of the single embedded PySpark JVM)
cd backend && gunicorn wsgi:app --bind 0.0.0.0:5000 --workers 1 --threads 4 --timeout 120
```

Containerized:

```bash
docker compose up --build -d           # web on 127.0.0.1:5000, SQLite+storage in named volumes
```

The Docker image (python:3.13-slim + OpenJDK 21 + PySpark + Gunicorn, non-root `appuser`,
tini, HEALTHCHECK) was built and run in Phase 11; the **full 233-test suite passes inside
the container** and a live HTTP pipeline (upload→…→export) runs Spark inside the served
worker, with SQLite surviving recreation. Do not assume a host supports PySpark/Java just
because it supports Python; do not raise the Gunicorn worker count without re-verifying JVM
memory/lifecycle. HTTPS/TLS and (if banner-hiding is required) `Server`-header stripping at
a trusted reverse proxy are deployment responsibilities.

---

## 14. Testing Strategy

Use pytest and the Flask test client. `tests/conftest.py` gives every test its own isolated
database and storage via a `build_app()` helper, uses a function-scoped Spark session
(`local[1]`), and provides deterministic sample CSVs and standard/alternate mappings.

### Current coverage — 127 passing, 1 pre-existing warning

- `test_uploads.py` — valid CSV/XLSX, empty, corrupt, unsupported, oversized, header-only,
  multi-sheet, path traversal.
- `test_column_mapping.py` — standard/alternate names, case, spaces/underscores, ambiguity,
  missing required fields, conflicts, manual corrections.
- `test_validation.py` — missing customer IDs, invalid dates, invalid numerics, duplicate
  candidates, cancelled/returns/negative quantities, empty valid sets, blocking.
- `test_database.py` — FK enforcement, transaction rollback, atomic writes, write guards,
  upload isolation, restart survival.
- `test_pyspark_service.py` + `test_analyses.py` — session init, line totals, aggregate
  accuracy, date/numeric parsing, empty/invalid handling, cross-analysis isolation, 404/409.
- `test_rfm_service.py` — scoring direction, ties, order independence, single/zero/None
  handling, negative clamp, empty cohort, persistence, endpoint guards, isolation, restart.
- `test_segmentation_service.py` — every label boundary, missing-value fallback, full-grid
  exactly-one + reachability, K-Means skip/reproducibility/missing/k-by-evaluation,
  persistence, rerun idempotency, isolation, restart survival.
- `test_dashboard.py` — overview metrics vs aggregate, revenue == customer-metric sum, segment
  counts/percentages, RFM distributions/summaries, aggregate-only incomplete stage handled as
  unavailable-not-zero, customer search/filter/sort/pagination, cross-analysis isolation,
  single-customer cohort, 404 unknown id, 400 bad params, no path/traceback leakage.
- `test_export.py` — customers/segments/overview CSV contents match persisted results and the
  dashboard, full XLSX workbook sheets + numeric cells, cross-analysis isolation, formula-injection
  neutralization (CSV and XLSX), headers-only empty cohort, 404/409/400 error paths, safe attachment
  filename, no path/traceback leakage.

### Test honesty (mandatory)

- Never claim a test passed unless it was actually executed. The full suite is run with
  `python -m pytest` from `backend/` before any phase is reported complete.
- Never replace a failing test with a hardcoded success, and never delete an existing test to
  make the suite green.
- Investigate and report failures. Separate passing tests from tests that could not run due
  to an unavailable dependency, and say so explicitly.
- Use small deterministic datasets with known expected results; verify invalid data does not
  silently produce a "successful" analysis.

---

## 15. Development Phases

Implemented in this order. Status reflects the verified current state.

- **Phase 0 — Repository audit & runtime verification.** *Completed.* Verified Python 3.13.14,
  Java 21.0.12, PySpark 4.2.0, Flask 3.0.3, pandas 2.2.3; chose not to add `pyarrow`; created
  `.gitignore` and `backend/.env.example`.
- **Phase 1 — Application foundation.** *Completed.* App factory, blueprints, centralized
  error handling, env config, SQLite schema, `/api/health`, multi-page UI shell; all pages 200.
- **Phase 2 — File upload & preview.** *Completed.* CSV/XLSX upload, safe storage, sheet
  selection, preview, status. (10 tests.)
- **Phase 3 — Automatic column mapping.** *Completed.* Synonym dictionary, detection,
  confidence, mapping UI, manual correction, schema generation. (17 cumulative tests.)
- **Phase 4 — Data validation & quality (mandatory gate).** *Completed.* Structural + value
  checks, configurable non-destructive rules, audit, eligible-row selection, analysis
  blocking. (31 cumulative.) Gate verified: 20 rows → 17 valid / 3 excluded / 11 customers.
- **Phase 5 — SQLite persistence polish.** *Completed.* Transaction + connection hardening,
  write guards, atomic report+status, two-process restart proof. (42 cumulative.)
- **Phase 6 — PySpark integration.** *Completed.* Hardened session, eligible-CSV-only load,
  line totals, customer aggregates, bounded collect, `reference_date` anchor. (63 cumulative.)
- **Phase 7 — RFM analysis.** *Completed.* Deterministic quintile scoring (recency inverted),
  edge-case safety, persistence scoped by `analysis_id`; `POST /rfm`, `GET /metrics`.
  (78 cumulative.)
- **Phase 8 — Customer segmentation.** *Completed.* 8 deterministic rule segments (exactly
  one each) + optional self-evaluating NumPy K-Means (off by default); `POST /segment`.
  (95 cumulative.)
- **Phase 9 — Analytics dashboard & visualization.** *Completed.* Read-only dashboard scoped by
  `analysis_id`: overview KPIs, segment distribution (counts + %), RFM distributions/summaries,
  and a server-side customer explorer (search/filter/sort/paginate) driven from persisted results
  via `GET /api/analyses[/<id>/dashboard|customers]`; pages render real data with honest
  empty/unavailable states. (111 cumulative.) Rule-based recommendations remain deferred (see §21).
- **Phase 10 — Export.** *Completed.* Read-only CSV/Excel export of a **completed** analysis scoped
  by `analysis_id`, built from the same persisted results the dashboard shows: `customers`
  (aggregates + RFM scores + segment + cluster), `segments` (count + share % + description),
  `overview` (KPI key/value pairs incl. AOV + definition) and a `full` multi-sheet XLSX workbook;
  `GET /api/analyses/<id>/export`. Spreadsheet formula injection is neutralized, filenames are
  whitelisted, empty cohorts yield headers-only files, and 404/409/400 errors are clean.
  (127 cumulative.)
- **Phase 11 — Security, hardening & deployment.** *Completed.* Strict CSP + security
  headers (`security_service.py`), inline-script externalization, per-process tiered rate
  limiting on mutating endpoints (`rate_limit_service.py`), production WSGI servers — Waitress
  (`serve.py`, `Server` header suppressed & live-verified) and Gunicorn (`wsgi.py`, 1 worker /
  4 threads / 120s for the PySpark JVM) — and a tested Docker image + Compose (built & run;
  full suite green in-container). Deployment/ops documented in `DEPLOYMENT.md`. Single-user MVP
  caveats (no authn/multi-tenancy; process-local limiter; TLS at a proxy) are documented,
  not hidden. (**233 cumulative.**)

Do not mark a phase complete merely because files exist — verify its functionality actually
runs and report real command output.

---

## 16. Coding Standards

1. Prefer clear, readable implementations over clever abstractions; descriptive names.
2. Keep functions focused on one responsibility.
3. Separate routes, services, repositories, database, and configuration.
4. Keep pure computation (RFM, segmentation, Spark) free of Flask and SQLite dependencies.
5. Avoid circular imports and unnecessary global state.
6. Manage DB sessions, transactions, and temporary files predictably.
7. Use type hints on functions and public service interfaces.
8. Keep configuration separate from business logic; no hardcoded credentials.
9. Avoid duplicated analytical algorithms and unused dependencies/placeholder modules.
10. **Never hardcode example analytical results.** All metrics, charts, and segment counts
    derive from the uploaded dataset; clearly label any sample dataset as an example.
11. Do not add authentication, caching, queues, microservices, cloud infrastructure, a second
    database, or a second charting library without an approved requirement.
12. Keep documentation aligned with the actual implementation; distinguish planned from done.
13. Prefer small, testable, reviewable changes; preserve working code unless a change is
    justified; explain important compatibility/architecture decisions.
14. Do not silently change the approved technology stack.

---

## 17. AI Coding-Agent Operating Rules

This section is mandatory for every coding agent working on the project.

### Before editing

1. Read this entire `PROJECT_CONTEXT.md`.
2. Inspect the current repository and relevant source/docs — do not trust prior reports
   blindly; re-read the actual files.
3. Identify the current phase and its completion criteria.
4. Check available runtimes and installed dependencies.
5. Prepare a short implementation plan and list the files to be created or changed.

Do not assume the repository is empty, and do not delete existing files without understanding
their purpose.

### During implementation

1. Work only on the authorized phase; implement the smallest complete solution for it.
2. Enforce the mandatory pipeline order (Section 5 / FR-07 gate). **PySpark analysis MUST NOT
   begin until required columns are mapped, confirmed, and validated.**
3. Do not skip the mapping or validation phases; do not write bespoke analytics per column
   naming convention — normalize to one canonical schema and keep analytics independent of
   original column names.
4. Add tests alongside new functionality; run the relevant tests and report actual results.
5. Investigate failures rather than hiding them; do not remove tests to go green.
6. Update `TASKS.md` after every phase and keep architecture/setup docs synchronized.
7. Never put real credentials into source control; never claim functionality exists when it
   is only planned.

### After implementation

Provide a concise report: phase completed, files created/modified, important decisions,
commands executed, actual test results, tests not run and why, known limitations, remaining
security concerns, exact next-developer commands, and recommended next step.

### Phase boundaries

Do not proceed to the next phase automatically. After completing and reporting a phase, STOP
and wait for explicit approval before making substantial changes for the following phase. If a
blocking issue is found, explain it and propose the smallest safe resolution rather than making
an unrelated architectural change.

### Handling uncertainty

Follow documented requirements where possible; choose the simplest safe implementation for
reversible details; document assumptions; and ask one focused question only when an unresolved
decision materially affects security, data loss, cost, compatibility, or architecture. Do not
ask the user to decide things resolvable by inspection or testing.

---

## 18. Definition of Done

A phase is complete only when its requirements are implemented **and verified**, not merely
because code was generated. The project satisfies this when:

- A user can upload a valid CSV/XLSX, preview it, and map differing column names to the
  standard schema, correcting ambiguous mappings manually.
- Missing required fields block analysis with understandable errors; invalid records are
  handled by documented rules.
- A validated dataset is processed with PySpark; RFM metrics and segments are computed
  correctly and deterministically, bound to the right analysis with no cross-contamination.
- Optional K-Means is reproducible, self-evaluated, and never overrides rule-based segments.
- Results and history persist in SQLite and survive restarts; reruns do not duplicate.
- (Phase 9+) The dashboard displays values derived from actual analysis; filters and charts
  work consistently; no fake/hardcoded metrics remain in the user journey.
- (Phase 10+) Customer lists and segment/overview reports export correctly to CSV/Excel, scoped to
  a completed analysis, with formula-injection-safe cells and correct empty-result handling.
- Tests cover critical mapping, validation, analytics, persistence, dashboard, export, and edge
  cases; the full suite is executed and reported honestly (currently **233 passing, 1 pre-existing warning**).
- Documentation matches the actual implementation, and known limitations are documented
  honestly.

---

## 19. Current Priorities

Phases 0–11 are complete and verified (**233 passing, 1 pre-existing warning**), including
production hardening (CSP/security headers, rate limiting, Waitress/Gunicorn WSGI, and a
tested Docker deployment documented in `DEPLOYMENT.md`). Remaining work is future feature
classified — not part of the current scope — and requires explicit approval before starting.

Deferred dashboard follow-ons (still open, not part of the Phase 9 MVP): date-range/country
filters, revenue-by-segment and purchase-trend charts, repeat-customer rate, and rule-based
recommendations (see Section 21). Any public/multi-user deployment additionally requires
authentication, per-user isolation, and shared/distributed rate limiting first.

---

## 20. Agent Resume Point

Read `PROJECT_CONTEXT.md` completely and treat it as the primary requirements document. The
repository is in the state described above: the full upload → mapping → validation → PySpark
aggregation → RFM → segmentation → dashboard → export pipeline is implemented and tested
(**127 passing, 1 pre-existing warning**); the security/hardening phase is intentionally not yet
built.

**Stop here and wait for explicit approval before beginning Phase 11 — Security, hardening &
deployment.**

---

## 21. Deferred / Optional Feature Proposals

These are **proposals, not implemented features**. Do not build them until their prerequisites
are complete and explicit approval is given.

### 21.1 Predictive churn / lifetime-value scoring

**Status:** Proposed. The MVP provides descriptive RFM segmentation and (planned) rule-based
recommendations. A trained, evaluated predictive model (e.g., churn probability or CLV) is out
of scope. Do not present rule-based recommendations as predictions, and do not claim accurate
churn prediction unless such a model is actually built, validated, and honestly described.

### 21.2 Multi-tenant authentication

**Status:** Proposed. The application is currently a documented local single-user MVP. If it
becomes multi-user, authentication plus server-side ownership checks are required before any
public deployment (see Section 11). This is an architectural decision, not a cosmetic feature.

### 21.3 Additional input formats (JSON, legacy .xls)

**Status:** Proposed. Only CSV and XLSX are supported today. Add formats only when a real
requirement emerges and can be validated and tested safely.
