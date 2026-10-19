# E-Commerce Customer Segmentation Dashboard — System Architecture

## 1. Architectural Overview

This project is a decoupled, layered full-stack application built for transaction
ingestion, configurable data-quality validation, and PySpark-based customer
analytics. The upload → mapping → validation foundation is a hard gate: the Spark
stage only ever consumes a *validated, eligible* dataset.

```
[ Browser / Client ]
        │
        ▼ (HTTP / JSON / Multipart file uploads)
[ Flask-Served Frontend: Jinja templates + Static (Bootstrap 5, Chart.js, vanilla JS) ]
        │
        ▼ (fetch /api/*)
[ Flask Backend (run.py, Port 5000) — Blueprints + centralized error handling ]
        │
  ┌─────┴──────────────────────────────┐
  │                                    │
  ▼                                    ▼
[ Analytics Services ]         [ Ingestion & Mapping ]
  • pyspark_service              • file_validation_service (ext/size/magic-number)
    - line totals (qty x price)   • file_reader_service (CSV / XLSX, sheets)
    - customer aggregates         • column_mapping_service (synonym + pattern)
  • rfm_service (Phase 7)         • schema_normalization_service (type coercion)
    - deterministic quintile      • dataset_service (raw + typed eligible loaders)
      R/F/M scoring from aggregates
  • segmentation_service (Phase 8)
    - rule-based segments + optional numpy K-Means
  • data_quality_service
    - configurable rules
    - eligible row selection
  • analysis_service (integrity -> Spark/RFM/segment -> persist)
  │                                    │
  └─────┬──────────────────────────────┘
        ▼
[ SQLite Persistence / Repositories (stdlib sqlite3, no ORM) ]
  • uploads • data_quality_reports • analyses • customer_metrics
  • transactional writes, FK enforcement, additive column migrations
```

> Note on stack: unlike a React/Vite + FastAPI + MySQL design, this MVP uses
> **server-rendered Jinja templates + static vanilla JS**, a **Flask** API, and
> **SQLite** via the standard library. There is no frontend build step.

## 2. Directory Structure

```text
E-Commerce-Customer-Segmentation-Dashboard/
├── frontend/                          # Server-rendered UI (templates + static assets)
│   ├── static/
│   │   ├── css/style.css              # Bootstrap 5 design-system overrides
│   │   └── js/                        # Vanilla JS page controllers
│   │       ├── main.js                # Shared API helper, session, formatters
│   │       ├── notifications.js       # Toast / alert helpers
│   │       ├── upload.js              # Drag-drop upload + preview
│   │       ├── column_mapping.js      # Mapping editor UI
│   │       └── validation.js          # Data-quality rule UI
│   └── templates/                     # Jinja2 templates served by Flask
│       ├── base.html                  # Layout + sidebar navigation
│       ├── _steps.html                # Workflow step indicator partial
│       ├── index.html                 # Landing / workflow overview
│       ├── upload.html                # Step 1 — Upload & preview
│       ├── column_mapping.html        # Step 2 — Automatic column mapping
│       ├── data_validation.html       # Step 3 — Validation & quality report
│       ├── analysis_history.html      # Dataset / analysis history
│       ├── dashboard.html             # Analytics dashboard (Phase 8+, empty state)
│       ├── customer_segments.html     # Segmentation view (Phase 7+, empty state)
│       ├── customer_details.html      # Per-customer detail (Phase 7+, empty state)
│       ├── settings.html              # About / pipeline / tech stack
│       └── errors/                    # 400 / 404 / 500 pages
│
├── backend/                           # Flask application (run.py, Port 5000)
│   ├── app/
│   │   ├── __init__.py                # App factory, blueprint + error registration
│   │   ├── config.py                  # Env-based config, paths, Spark settings
│   │   ├── database/
│   │   │   ├── connection.py          # sqlite3 helpers, transaction(), init + migration
│   │   │   └── schema.sql             # uploads, data_quality_reports, analyses, customer_metrics
│   │   ├── repositories/              # SQLite data-access layer (no Flask/Spark deps)
│   │   │   ├── upload_repository.py   # uploads + reports, atomic write, write guards
│   │   │   └── analysis_repository.py # analyses + customer_metrics persistence
│   │   ├── services/                  # Pure processing layers (framework-agnostic)
│   │   │   ├── file_validation_service.py
│   │   │   ├── file_reader_service.py
│   │   │   ├── dataset_service.py
│   │   │   ├── column_mapping_service.py
│   │   │   ├── schema_normalization_service.py
│   │   │   ├── data_quality_service.py
│   │   │   ├── pyspark_service.py     # Spark session, line totals, customer aggregates
│   │   │   ├── rfm_service.py         # Phase 7 deterministic quintile RFM scoring (pure)
│   │   │   ├── segmentation_service.py# Phase 8 rule-based segments + optional K-Means
│   │   │   └── analysis_service.py    # integrity -> Spark/RFM/segment -> persist
│   │   ├── routes/                    # Flask blueprints (thin HTTP layer)
│   │   │   ├── pages.py               # HTML page routes
│   │   │   ├── uploads.py             # POST /api/uploads, preview, sheets, list
│   │   │   ├── mappings.py            # GET/POST /api/uploads/<id>/mapping
│   │   │   ├── validation.py          # GET/POST /api/uploads/<id>/validate
│   │   │   └── analyses.py            # aggregate, rfm, segment, metrics endpoints
│   │   └── utils/                     # file_utils, response_utils, logging_utils
│   ├── tests/                         # pytest suite (isolated DB + storage per test)
│   │   ├── conftest.py                # fixtures, build_app(), prepare_ready_upload()
│   │   ├── test_uploads.py
│   │   ├── test_column_mapping.py
│   │   ├── test_validation.py
│   │   ├── test_database.py           # persistence, transactions, FK, restart survival
│   │   ├── test_pyspark_service.py    # Spark math, parsing, isolation, errors
│   │   ├── test_analyses.py           # integration + SQLite compatibility
│   │   ├── test_rfm_service.py        # Phase 7 RFM scoring, ties, persistence
│   │   └── test_segmentation_service.py # Phase 8 segments, K-Means, isolation
│   ├── .env.example                   # Environment configuration template
│   ├── pytest.ini                     # pythonpath=., testpaths=tests
│   ├── requirements.txt               # Flask, pandas, PySpark, openpyxl, ...
│   ├── run.py                         # Entry point (Flask dev server)
│   └── restart_check.py               # Two-process persistence smoke test
│
├── sample_data/                       # Demo datasets (incl. validation edge cases)
│   ├── sample_retail.csv
│   ├── sample_retail.xlsx
│   └── sample_multi_sheet.xlsx
│
├── database/                          # Runtime SQLite DB (gitignored)
├── storage/                           # Runtime uploads/processed/exports/temp (gitignored)
│
├── .gitignore                         # Exclusion rules (runtime dirs, caches, .env)
├── ARCHITECTURE.md                    # This architecture specification
├── LICENSE                            # Project license
├── PROJECT_CONTEXT.md                 # Primary product & technical requirements (master prompt)
├── README.md                          # Setup, verification, and run guide
└── TASKS.md                           # Phase-by-phase implementation log
```

## 3. Layer Responsibilities

- **Frontend (Jinja templates + vanilla JS):** Server-rendered multi-page UI with a
  Bootstrap 5 shell and sidebar; drives the upload → mapping → validation → analysis
  workflow via `fetch` calls to `/api/*`, and renders charts with Chart.js. Pages for
  later phases show honest empty states.
- **Backend API (`backend/app/routes/` + `__init__.py`):** Flask blueprints form a thin
  HTTP layer — request parsing, multipart upload security (UUID filenames, path-traversal
  neutralization, magic-number checks), centralized JSON/HTML error handling, and `/api/health`.
  Routes never contain business or Spark logic.
- **Core Configuration (`backend/app/config.py`):** Environment-overridable settings for
  paths, upload limits, data-quality thresholds, and Spark (`SPARK_MASTER`, app name);
  dev/testing/production config classes.
- **Ingestion & Mapping Services (`backend/app/services/`):** Read CSV/XLSX as strings,
  auto-detect columns via synonym + value-pattern scoring, resolve conflicts, and
  normalize to the canonical schema; coerce types with explicit parse-failure tracking.
- **Validation / Quality (`data_quality_service.py`):** Runs structural + value checks
  with configurable, non-destructive business rules (cancelled, returns, duplicates,
  missing required), produces an audit-backed report, selects the eligible rows, and
  blocks analysis on unusable data. This is the mandatory gate before Spark.
- **Analytics Services (`pyspark_service.py`, Phase 6):** Loads only the persisted
  eligible CSV into a hardened, lazily-shared Spark session, computes transaction-level
  line totals and customer-level aggregates (distinct orders, total quantity, total spend,
  first/last purchase date), collecting only bounded aggregates.
- **RFM Scoring (`rfm_service.py`, Phase 7):** A pure, deterministic module that scores
  each customer 1–5 on Recency/Frequency/Monetary from the persisted aggregates via the
  cohort's average-percentile rank (ties equal, order-independent, Recency inverted),
  anchored on `analyses.reference_date`.
- **Segmentation (`segmentation_service.py`, Phase 8):** A pure module mapping RFM scores
  to one interpretable, mutually-exclusive rule-based segment (Champions, Loyal, Recent,
  Potential Loyalists, At Risk, Cannot Lose Them, Hibernating, Others fallback). Optional
  numpy K-Means (no new dependency, off by default) is kept separate, self-evaluates `k`
  by an inertia elbow, skips safely on small/constant/missing cohorts, and never overrides
  the rule-based segment.
- **Orchestration (`analysis_service.py`):** Enforces the integrity rules that only a
  `ready_for_analysis` dataset with a present eligible file may be aggregated, binds each
  analysis to exactly one upload, and gates the staged pipeline (aggregate → RFM → segment),
  refusing to score or segment before its prerequisite has completed.
- **Persistence Layer (`backend/app/database/` + `repositories/`):** SQLite via the
  standard library (`PRAGMA foreign_keys=ON`), a `transaction()` helper for atomic
  writes, an idempotent additive column migration in `init_db`, and repository modules
  (`uploads`, `data_quality_reports`, `analyses`, `customer_metrics`) that stay free of
  Flask and Spark concerns.
- **Testing (`backend/tests/`):** Pytest suite with per-test isolated DB/storage,
  covering file handling, mapping, quality rules, full-pipeline E2E, persistence/
  transaction/FK/restart integrity, and PySpark math/isolation/error handling.

  ## 4. API Surface

All JSON responses share the envelope `{ "success": bool, "message": str, "data": object, "details": object|null }`.
Pages are thin shells; their content is loaded by JavaScript from `/api/*`.

### 4.1 Page Routes (server-rendered HTML)

| Method | Path | Template | Workflow Step |
|--------|------|----------|---------------|
| GET | `/` | `index.html` | Overview / landing |
| GET | `/upload` | `upload.html` | 1 — Upload & preview |
| GET | `/mapping` | `column_mapping.html` | 2 — Column mapping |
| GET | `/validation` | `data_validation.html` | 3 — Validation & quality |
| GET | `/dashboard` | `dashboard.html` | Analytics (Phase 8+, empty state) |
| GET | `/segments` | `customer_segments.html` | Segmentation (Phase 7+, empty state) |
| GET | `/customers` | `customer_details.html` | Per-customer detail (Phase 7+, empty state) |
| GET | `/history` | `analysis_history.html` | Dataset / analysis history |
| GET | `/settings` | `settings.html` | About / pipeline / stack |

### 4.2 JSON API (`/api/*`)

| Method | Path | Purpose | Notable Responses |
|--------|------|---------|-------------------|
| GET | `/api/health` | Liveness probe | 200 |
| POST | `/api/uploads` | Multipart upload (`file`); validates, stores, auto-detects mapping | 201 · 400 (invalid) · 413 (too large) · 500 |
| GET | `/api/uploads` | Recent datasets + workflow status | 200 |
| GET | `/api/uploads/summary` | Counts (total / ready) | 200 |
| GET | `/api/uploads/<id>` | Single upload detail | 200 · 404 |
| GET | `/api/uploads/<id>/preview` | Paged preview (`?sheet=` to switch sheet) | 200 · 400 · 404 |
| GET | `/api/uploads/<id>/sheets` | Excel worksheet names | 200 · 404 |
| GET | `/api/uploads/<id>/mapping` | Detected + confirmed mapping, standard fields | 200 · 404 |
| POST | `/api/uploads/<id>/mapping` | Confirm mapping → normalize → write `*_normalized.csv` | 200 · 400 (incomplete/conflict) · 404 |
| GET | `/api/uploads/<id>/validate` | Retrieve persisted quality report | 200 · 404 |
| POST | `/api/uploads/<id>/validate` | Run quality rules → write `*_eligible.csv` + report | 200 (passed) · 409 (no mapping) · 422 (failed) · 500 |
| POST | `/api/uploads/<id>/aggregate` | Run PySpark aggregation → persist metrics | 201 · 404 · 409 (not ready) · 500 (Spark) |
| GET | `/api/uploads/<id>/analyses` | Analysis runs for an upload | 200 |
| GET | `/api/analyses/<id>` | Single analysis status + summary | 200 · 404 |
| POST | `/api/analyses/<id>/rfm` | Compute + persist deterministic RFM scores (Phase 7) | 200 · 404 · 409 (not completed) |
| POST | `/api/analyses/<id>/segment` | Assign rule-based segments (+ optional K-Means) (Phase 8) | 200 · 404 · 409 (no RFM yet) |
| GET | `/api/analyses/<id>/metrics` | Per-customer aggregates + RFM scores + segments | 200 · 404 |

## 5. Data Pipeline Order

Strictly gated; each stage only consumes the artifact produced by the previous one.
PySpark **never** touches the raw upload or the normalized file — only the validated
`*_eligible.csv`.

```
[1] UPLOAD            POST /api/uploads
    file_validation_service → file_reader_service → column_mapping_service.detect
    artifact: stored file (uploads/<uuid>), columns_json
    status:   uploaded -> mapping_required
        │
        ▼
[2] MAPPING           POST /api/uploads/<id>/mapping
    validate_mapping → normalize_dataframe (rename to canonical schema)
    artifact: processed/<id>_normalized.csv
    status:   -> mapped
        │
        ▼
[3] VALIDATION        POST /api/uploads/<id>/validate
    schema_normalization_service.normalize_types → data_quality_service.validate
    (configurable rules; eligible rows + quality report)
    artifact: processed/<id>_eligible.csv  +  data_quality_reports row
    status:   -> ready_for_analysis  (or validation_failed -> blocks Spark)
        │  ============ MANDATORY GATE ============
        ▼
[4] AGGREGATION       POST /api/uploads/<id>/aggregate   (Phase 6, PySpark)
    integrity check (ready + eligible file present)
    pyspark_service: load eligible CSV -> line totals -> customer aggregates
    analysis_repository: analyses row + customer_metrics (base aggregates)
    reference_date = last purchase date (Phase 7 RFM anchor)
    status:   analysis pending -> running -> completed (or failed)
        │
        ▼
[5] RFM SCORING       POST /api/analyses/<id>/rfm   (Phase 7, pure Python)
    gate: analysis must be completed
    rfm_service: average-percentile quintile scores (R/F/M 1..5, Recency inverted)
    analysis_repository.update_rfm_scores (scoped by analysis_id)
        │
        ▼
[6] SEGMENTATION      POST /api/analyses/<id>/segment   (Phase 8)
    gate: RFM scores present
    segmentation_service: rule-based segment (exactly one per customer)
    + optional numpy K-Means (separate, never overrides segment)
    analysis_repository.update_segments
        │
        ▼
[7] DASHBOARD / EXPORT          (Phase 9+, not yet implemented)
```

**Upload/analysis status lifecycle**

`uploaded → mapping_required → mapped → ready_for_analysis → (analysis) completed`
Failure side-states: `validation_failed` (blocking quality issues), `failed`
(processing/storage error), `deleted` (reserved for retention; excluded from
history/summary).