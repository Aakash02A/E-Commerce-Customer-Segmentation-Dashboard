# E-Commerce Customer Segmentation Dashboard

A full-stack, data-driven web application for segmenting e-commerce customers from their
own transaction datasets. Users upload a CSV/Excel file, validate and map its columns to a
standard schema, process the eligible transactions with **Apache PySpark**, compute **RFM**
metrics, and assign interpretable **customer segments** — all backed by **SQLite**
persistence. The interface is a real multi-page **Flask + Bootstrap 5 + Chart.js**
application, not a static demo with hardcoded numbers.

> **Project status:** Phases 0–8 (upload → mapping → validation → PySpark aggregation → RFM
> → segmentation) are implemented and verified by an automated suite (**95/95 tests
> passing**). The analytics dashboard, exports, and production hardening (Phases 9–11) are
> not yet built — those pages currently render honest empty states rather than placeholder
> metrics. See [TASKS.md](TASKS.md) for the phase tracker and
> [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md) / [ARCHITECTURE.md](ARCHITECTURE.md) for the full
> specification and design.

---

## Features

- **User-data-driven analytics** — every metric derives from the uploaded, validated dataset.
- **File upload & preview** — CSV and XLSX, with worksheet selection, safe UUID-based storage,
  path-traversal protection, and size/type validation.
- **Automatic column mapping** — synonym + type/pattern detection with conservative confidence
  scores, an interactive correction UI, and normalization to one standard schema.
- **Data-quality gate (mandatory)** — structural and value checks with configurable,
  non-destructive business rules (cancellations, returns, duplicates); blocks analysis when
  required data is missing or unusable.
- **PySpark processing** — hardened, shared Spark session; line totals and per-customer
  aggregates (distinct orders, total quantity, total spend, first/last purchase date); bounded
  collection only.
- **Deterministic RFM** — quintile scores (Recency inverted) with tie-safe, order-independent
  scoring anchored on an explicit reference date.
- **Rule-based segmentation** — 8 documented, mutually exclusive segments (exactly one per
  customer) plus an **optional**, reproducible, self-evaluating NumPy K-Means (off by default,
  never overrides rule labels).
- **SQLite persistence** — analyses and per-customer metrics scoped by `analysis_id`, surviving
  restarts, with no cross-analysis contamination.

---

## Technology Stack

| Layer | Technology |
|---|---|
| Frontend | HTML5, CSS3, Bootstrap 5, vanilla JavaScript, Chart.js, Jinja templates |
| Backend | Python 3.13, Flask (app factory + blueprints), Flask-Cors |
| Analytics | Pandas, NumPy |
| Big-data | Apache PySpark 4.2.0 (on Java 21), optional K-Means via NumPy |
| Database | SQLite (Python `sqlite3`, no ORM) |
| Testing | pytest, Flask test client |

The application is server-rendered by a single Flask process — there is no separate frontend
server and no React/FastAPI/MySQL. `pyarrow` and `scikit-learn` are intentionally **not** used.

---

## Prerequisites

- **Python 3.13**
- **Java 21 (LTS)** — required by PySpark
- A modern browser
- OS: developed and verified locally on **Windows 11**

Verify your runtimes:

```powershell
python --version    # 3.13.x
java -version       # 21.x
```

---

## Quick Start

```powershell
# 1. Install dependencies
pip install -r backend/requirements.txt

# 2. Run the app (single Flask server serves pages + API)
cd backend
python run.py
```

Open **http://127.0.0.1:5000** in your browser. The SQLite database and `storage/` directories
are created automatically on first run.

Optional environment overrides (see `backend/.env.example`):

| Variable | Default | Purpose |
|---|---|---|
| `FLASK_ENV` | `development` | Config profile (`development`/`testing`/`production`) |
| `FLASK_RUN_HOST` / `FLASK_RUN_PORT` | `127.0.0.1` / `5000` | Bind address |
| `DATABASE_PATH` | `database/ecommerce.db` | SQLite file |
| `UPLOAD_DIR`, `PROCESSED_DIR`, `EXPORTS_DIR`, `TEMP_DIR` | under `storage/` | File storage |
| `MAX_CONTENT_LENGTH` | `104857600` (100 MB) | Upload size limit |
| `SPARK_MASTER` | `local[*]` | Spark master |
| `SEGMENTATION_ENABLE_KMEANS` | `0` | Enable optional K-Means (`1` to turn on) |
| `KMEANS_RANDOM_SEED` / `KMEANS_MAX_K` / `KMEANS_MIN_CUSTOMERS` | `42` / `5` / `10` | K-Means settings |

---

## Usage Workflow

1. **Upload Data** — upload a CSV or XLSX transaction file (or use `sample_data/sample_retail.csv`).
2. **Preview & Map** — review the detected columns and confirm/correct the mapping to the
   standard schema.
3. **Data Quality** — review the validation report; apply business rules; the dataset becomes
   `ready_for_analysis` only when it passes.
4. **Aggregate** — run the PySpark pipeline to compute per-customer aggregates.
5. **RFM** — compute deterministic Recency/Frequency/Monetary scores.
6. **Segment** — assign rule-based segments (and optional K-Means clusters).
7. **Dashboard / Segments / Customers** — *(Phase 9)* interactive KPIs and charts; currently
   empty states.

A sample run on `sample_retail.csv` yields **11 customers**, 15 distinct orders, and 6 distinct
segments, with the K-Means evaluator self-selecting `k=4` when enabled.

---

## API Reference

All API responses are JSON (`{"success": ..., "message": ..., "details": ...}` on error).

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/api/health` | Liveness check |
| POST | `/api/uploads` | Upload a CSV/XLSX file |
| GET | `/api/uploads` | List recent uploads |
| GET | `/api/uploads/summary` | Counts by status |
| GET | `/api/uploads/<id>` | One upload's metadata |
| GET | `/api/uploads/<id>/preview` | Parsed preview (columns, sample rows) |
| GET | `/api/uploads/<id>/sheets` | Worksheet names (XLSX) |
| GET | `/api/uploads/<id>/mapping` | Detected columns + suggested mapping |
| POST | `/api/uploads/<id>/mapping` | Confirm/correct mapping → normalize |
| GET | `/api/uploads/<id>/validate` | Latest data-quality report |
| POST | `/api/uploads/<id>/validate` | Run validation gate |
| POST | `/api/uploads/<id>/aggregate` | Run PySpark pipeline → analysis |
| GET | `/api/uploads/<id>/analyses` | Analyses for an upload |
| GET | `/api/analyses/<id>` | One analysis |
| POST | `/api/analyses/<id>/rfm` | Compute + persist RFM scores |
| POST | `/api/analyses/<id>/segment` | Assign segments (+ optional clusters) |
| GET | `/api/analyses/<id>/metrics` | Per-customer metrics |

Page routes: `/` (home), `/upload`, `/mapping`, `/validation`, `/dashboard`, `/segments`,
`/customers`, `/history`, `/settings`.

---

## Data Contract

Uploaded files are normalized to a standard schema:

| Field | Meaning | Requirement |
|---|---|---|
| `customer_id` | Customer identifier | Required |
| `order_id` | Invoice/transaction id | Strongly recommended (needed for distinct-order frequency) |
| `order_date` | Purchase date/timestamp | Required |
| `quantity` | Units purchased | Required |
| `unit_price` | Price per unit | Required |
| `product_id` | Product identifier | Optional |
| `product_description` | Product name/description | Optional |
| `country` | Customer/transaction country | Optional |
| `transaction_status` | Completed/cancelled/returned | Optional |

Monetary value is computed as `line_total = quantity × unit_price`. Frequency counts **distinct
orders**, not line items.

---

## Testing

From the `backend/` directory:

```powershell
python -m pytest
```

Current result: **95 passed**. Each test runs against an isolated SQLite DB and storage
directory. The suite covers file handling, column mapping, validation, database persistence and
restart survival, PySpark aggregation, RFM scoring, and segmentation (rules + K-Means).

Persistence restart check:

```powershell
python restart_check.py write
python restart_check.py read
```

---

## Security & Limitations

- **Local single-user MVP:** there is **no authentication or multi-tenancy**. Do not expose this
  MVP publicly — uploads, results, and history are not user-isolated. A multi-user deployment
  would require authentication and server-side ownership checks first.
- Upload size limit (default 100 MB), safe filenames, path-traversal protection, and
  parameterized SQL are enforced.
- Internal errors are returned as clean messages/HTML pages — no stack traces, filesystem paths,
  or credentials are exposed to clients.
- PySpark runs locally; this project does **not** claim distributed, cluster-scale performance.
- The dashboard, exports, and production hardening are **not yet implemented** (Phases 9–11).

---

## License

See [LICENSE](LICENSE).
