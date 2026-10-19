# E-Commerce Customer Segmentation Dashboard — Implementation Task Tracker

## Phase Status Summary

| Phase | Description | Status | Verification |
|---|---|---|---|
| **Phase 0** | Repository Inspection & Runtime Verification | **Completed** | Python 3.13.14, Java 21.0.12 LTS, PySpark 4.2.0, pandas 2.2.3, Flask 3.0.3 verified |
| **Phase 1** | Foundation (Flask app factory, SQLite, Multi-page UI) | **Completed** | All 9 pages 200 OK, `/api/health`, schema created |
| **Phase 2** | File Upload & Preview (CSV/XLSX) | **Completed** | Tests passing (10/10), upload/preview/sheets, path-traversal neutralized |
| **Phase 3** | Automatic Column Detection & Mapping | **Completed** | Tests passing (17/17), sample auto-mapped 8/8 columns |
| **Phase 4** | Data Validation & Quality Report (mandatory gate) | **Completed** | Tests passing (31/31), E2E 20 rows → 17 valid / 3 excluded / 11 customers |
| **Phase 5** | SQLite Persistence Polish | **Completed** | Tests passing (42/42), FK + transaction + two-process restart verified |
| **Phase 6** | PySpark Integration (line totals + customer aggregates) | **Completed** | Tests passing (63/63), Spark 4.2.0 aggregates verified, pyarrow-free |
| **Phase 7** | RFM Analysis (Recency / Frequency / Monetary scoring) | **Completed** | Tests passing (78/78), deterministic quintile scores persisted; 11 sample customers scored, recency anchored on `reference_date` |
| **Phase 8** | Customer Segmentation (segment labels / clustering) | **Completed** | Tests passing (95/95), deterministic rule-based RFM segments (8 labels) persisted per analysis; optional numpy K-Means (self-evaluating, off by default) |
| **Phase 9** | Analytics Dashboard & Visualization | **Completed** | Tests passing (111/111), read-only dashboard scoped by `analysis_id` (overview KPIs, segment distribution + %, RFM distributions/summaries, server-side customer explorer) rendered live via Chart.js; UI verified in running app |
| **Phase 10** | CSV / Excel Export | **Not Started** | — |
| **Phase 11** | Production Hardening & Deployment | **Not Started** | — |

> Status legend: **Completed** = built and verified by automated tests · **Not Started** = roadmap phase.
> Test totals are cumulative across the whole `backend/tests` suite (`python -m pytest`).

---

## Phase 0 Checklist (Completed)

- [x] Audited repository: confirmed the prior v1 demo was removed and `backend/` + `frontend/` were empty; rebuilt from scratch.
- [x] Verified runtimes and compatibility: Python 3.13.14, Java 21.0.12 LTS, PySpark 4.2.0, Flask 3.0.3, pandas 2.2.3, numpy 2.5.0, Flask-Cors 4.0.1, pytest 8.3.3.
- [x] Installed `openpyxl` for XLSX; deliberately chose **not** to add `pyarrow` and persist normalized/eligible data as plain CSV instead.
- [x] Created root `.gitignore` and `backend/.env.example`.

---

## Phase 1 Checklist (Completed)

- [x] Flask application factory (`backend/app/__init__.py`) with blueprint registration and centralized error handling (JSON for `/api/*`, HTML pages otherwise).
- [x] Environment-overridable config (`backend/app/config.py`): storage paths, upload limits, data-quality thresholds, Spark settings; dev/testing/production classes.
- [x] SQLite persistence via stdlib `sqlite3` (`database/connection.py`, `database/schema.sql`) — tables: `uploads`, `data_quality_reports`, `analyses`, `customer_metrics`.
- [x] Implemented `/api/health` liveness endpoint.
- [x] Built multi-page UI shell: `base.html` + sidebar nav (Bootstrap 5), shared `main.js`, `notifications.js`, `style.css`, `_steps.html`, error pages (400/404/500), Settings/About.
- [x] Verified all 9 pages return HTTP 200 and `/api/health` reports OK.

---

## Phase 2 Checklist (Completed)

- [x] Implemented `file_validation_service.py`: extension whitelist, size limit, magic-number/content sanity, empty/corrupt rejection, Excel worksheet listing.
- [x] Implemented `file_reader_service.py`: CSV (delimiter sniffing + encoding fallback, all values as strings) and XLSX (sheet selection).
- [x] Implemented `file_utils.py`: UUID-based safe stored filenames with path-traversal neutralization.
- [x] Exposed `POST /api/uploads`, `GET /api/uploads/<id>/preview`, `/<id>/sheets`, `/api/uploads/summary`, upload list and detail.
- [x] Built upload UI: drag-and-drop with XHR progress, preview table, and sheet selector.
- [x] Added automated file-handling suite `backend/tests/test_uploads.py` (10 tests passing) covering valid CSV/XLSX, empty, corrupt, unsupported, oversized, header-only, multi-sheet, path-traversal.

---

## Phase 3 Checklist (Completed)

- [x] Implemented `column_mapping_service.py`: standard schema, synonym dictionary, name-normalizing + value/pattern detection with conservative confidence scoring, conflict resolution, `validate_mapping`, and `normalize_dataframe`.
- [x] Built mapping UI (`column_mapping.html` + `column_mapping.js`): per-column dropdowns, sample values, confidence badges, required-field guard, duplicate-target warning.
- [x] Exposed `GET`/`POST /api/uploads/<id>/mapping` (validate mapping → normalize → persist standard-schema CSV).
- [x] Added mapping tests `backend/tests/test_column_mapping.py` (7 tests; 17/17 cumulative).
- [x] Verified on `sample_data/sample_retail.csv`: all 8 columns (Client, Order_Ref, Purchase_Date, Units, Unit_Cost, StockCode, …) auto-detected and mapped correctly.

---

## Phase 4 Checklist (Completed) — Mandatory Validation Gate

- [x] Implemented `schema_normalization_service.py`: typed coercion (dates/numerics) with parse-failure tracking; typed CSV loader `dataset_service.load_normalized_typed`.
- [x] Implemented `data_quality_service.py`: structural + value checks, configurable non-destructive business rules (cancelled / returns / duplicates / missing required), audit of applied rules, eligible-row selection, and analysis blocking.
- [x] Built validation UI (`data_validation.html` + `validation.js`): rule selectors, summary KPIs, findings table (blocking/excluded/warning), pass/fail status, SQLite persistence.
- [x] Exposed `GET`/`POST /api/uploads/<id>/validate`.
- [x] Added validation tests `backend/tests/test_validation.py` (14 tests; 31/31 cumulative).
- [x] Verified gate end-to-end (live server + tests): 20 rows → 17 valid, 3 excluded (cancelled `C489451`, missing customer, non-numeric qty), 11 distinct customers; status → `ready_for_analysis`.
- [x] Confirmed **PySpark was NOT started** until mapping + validation were tested end-to-end, as mandated.

---

## Phase 5 Checklist (Completed) — SQLite Persistence Polish

- [x] Fixed Flask `g` usage to attribute access; added a `transaction()` context manager and a shared `_configure()` connection factory (`PRAGMA foreign_keys=ON`, `busy_timeout=5000`) in `database/connection.py`.
- [x] Hardened `upload_repository.py`: whitelisted updatable columns + valid statuses (unknown writes raise `ValueError`), atomic `record_validation_result()`, `latest_report()`, `report_count()`, and delete-aware `summary()`/`list_recent()`.
- [x] Reworked `routes/validation.py`: report + status written in ONE transaction; eligible CSV persisted to disk before `ready_for_analysis`; clean 500 with no half-applied state.
- [x] Added `backend/restart_check.py` two-process persistence smoke test.
- [x] Made `tests/conftest.py` give every test its own DB + storage via `build_app()`.
- [x] Added `backend/tests/test_database.py` (11 tests: FK enforcement, transaction rollback, atomic writes, write guards, upload isolation, restart survival).
- [x] Verified full suite **42/42 passed**; proved persistence survives a **real process restart** (status `ready_for_analysis`, confirmed mapping, and `17/20 valid` report recovered).

---

## Phase 6 Checklist (Completed) — PySpark Integration

- [x] Verified Spark 4.2.0 starts on Java 21/Windows and runs `groupBy/agg/collect` **without pyarrow** (so `toPandas()` is avoided).
- [x] Implemented `services/pyspark_service.py`: lazily-shared, hardened session (`spark.sql.ansi.enabled=false`, UTC, UI off, `driver.host=127.0.0.1`); loads only the eligible CSV by header name + explicit casts; tz-independent DATE parsing (no rollover); line totals (`quantity × unit_price`); customer aggregates (distinct order count, total quantity, total spend, first/last purchase date); collects only bounded aggregates; user-safe `SparkProcessingError`.
- [x] Implemented `repositories/analysis_repository.py`: `analyses` + `customer_metrics` persistence, transactional bulk metric write, per-analysis isolation via `UNIQUE(analysis_id, customer_id)`.
- [x] Implemented `services/analysis_service.py`: integrity gate (only `ready_for_analysis` + present eligible file, never raw/normalized), binds each analysis to one upload, records `reference_date = last purchase date` (Phase 7 anchor), pending → running → completed/failed transitions.
- [x] Added thin `routes/analyses.py` blueprint: `POST /api/uploads/<id>/aggregate`, `GET /api/uploads/<id>/analyses`, `GET /api/analyses/<id>`; registered in the app factory.
- [x] Extended `schema.sql` + `connection.py`: added `total_quantity`, `first_order_date` to `customer_metrics`; added an idempotent additive column migration in `init_db`.
- [x] Added `tests/test_pyspark_service.py` (12) and `tests/test_analyses.py` (9).
- [x] Verified full suite **63/63 passed**; live example on `sample_retail.csv`: 11 customers, 15 distinct orders, total quantity 142, total spend 258.90, range 2010-12-01 → 2010-12-20.

---

## Phase 7 Checklist (Completed) — RFM Analysis

Scope: Recency / Frequency / Monetary **scoring only**. Segment labels, clustering,
recommendations, dashboards and exports are explicitly deferred to Phase 8+.

- [x] Added `services/rfm_service.py`: a pure, framework-free computation module
  (no Flask / no SQLite / no Spark) that turns persisted aggregates into scored rows.
- [x] RFM values are computed **only from the validated Phase 6 aggregates**
  (`customer_metrics.frequency` = distinct valid orders, `monetary` = total valid
  spend, `last_order_date` = most recent purchase) — never from raw uploads.
- [x] Recency = whole days between each customer's most recent purchase and the
  analysis `reference_date` (fallback: cohort max purchase date); negative values
  clamped to 0.
- [x] **Deterministic quintile scoring (documented):** score 1..5 via the cohort's
  *average percentile rank* — `pct = (count(< v) + count(<= v)) / 2 / n`,
  `score = floor(pct * n_bins) + 1` (capped at `n_bins`). Higher = better for
  Frequency and Monetary; **Recency is inverted** (fewer days → higher R score).
- [x] Edge cases handled safely: identical values always receive identical scores
  (ties), results are independent of input order, small/1-customer cohorts
  compress to the neutral middle bin (3), and `None`/missing metrics are excluded
  and scored `None` rather than crashing.
- [x] Added `analysis_repository.load_rfm_inputs()` and `update_rfm_scores()`
  (single transaction, every UPDATE scoped by `analysis_id` → no cross-analysis
  contamination). Segment/cluster columns are intentionally left untouched.
- [x] Added `analysis_service.run_rfm_for_analysis()`: integrity gate (analysis
  must exist and be `completed`, else 404/409), empty-cohort safe, typed errors.
- [x] Added thin routes `POST /api/analyses/<id>/rfm` and
  `GET /api/analyses/<id>/metrics`.
- [x] No new schema migration required — `recency_days`, `r_score`, `f_score`,
  `m_score` already exist as nullable additive columns in `customer_metrics`.
- [x] Added `tests/test_rfm_service.py` (15 tests: scoring direction, ties,
  order-independence, single/zero/None handling, negative clamp, empty cohort,
  persistence, endpoint, 404/409 guards, cross-analysis isolation, restart survival).
- [x] Verified full suite **78/78 passed** (63 prior + 15 new, zero regressions).
- [x] Live example on `sample_retail.csv` (reference_date 2010-12-20): 11 customers
  scored, e.g. `17860` (0 days ago) → R=5, `17850` (19 days, freq 1, £31.76) →
  R=2/F=2/M=4; recency ties (six customers at 19 days) all received R=2.
- [x] Removed the unused `pyarrow` dependency from `backend/requirements.txt`
  after a repository-wide check confirmed no direct or indirect usage (data IO is
  CSV + openpyxl; Spark reads CSV with its own parser).

---

## Phase 8 Checklist (Completed) — Customer Segmentation

Scope: interpretable RFM **segment labels** + **optional** K-Means. Dashboards,
recommendations and exports are Phase 9+.

- [x] Added `services/segmentation_service.py`: a pure computation module
  (no Flask / SQLite / Spark) that maps persisted RFM scores to segments.
- [x] **Rule-based segments (deterministic, documented, mutually exclusive).**
  Customers are partitioned by a recency tier, then an F/M ladder (first match
  wins), so every scored customer gets **exactly one** of 8 proposed labels:
  - Active `R >= 4`: F>=4 & M>=4 → **Champions**; elif F>=3 → **Loyal Customers**;
    elif M>=4 → **Potential Loyalists**; else → **Recent Customers**.
  - Warm `R == 3`: F>=4 & M>=4 → **Loyal Customers**; elif F>=3 or M>=3 →
    **Potential Loyalists**; else → **Others**.
  - Cold `R <= 2`: F>=4 & M>=4 → **Cannot Lose Them**; elif F>=3 or M>=3 →
    **At Risk**; else → **Hibernating**.
  - Missing any R/F/M score → **Others** fallback (never dropped/crashed).
- [x] Verified full-grid properties: all 5×5×5 score combos yield a valid label,
  all 8 labels are reachable, and no customer can receive two labels.
- [x] **Optional K-Means (numpy, no new dependency).** Separate from and never
  overrides the rule segment. Standardizes features (z-score, zero-variance-safe),
  seeded init (`KMEANS_RANDOM_SEED`, default 42) and distinct-centroid seeding for
  reproducibility; selects `k` by an **inertia elbow** over `2..min(k_max, distinct)`
  (not arbitrary) and rejects degenerate/collapsed runs. Skips — with an explicit
  reason — when customers < `KMEANS_MIN_CUSTOMERS` (10), distinct feature vectors
  < 3, features are all zero-variance, or any input has missing values (rows
  dropped). Off by default (`SEGMENTATION_ENABLE_KMEANS=0`).
- [x] `analysis_repository.load_segmentation_inputs()` + `update_segments()` —
  one transaction; every UPDATE scoped by `analysis_id` and keyed by
  `UNIQUE(analysis_id, customer_id)`, so reruns overwrite in place (no duplicates,
  no stale clusters) and never touch another analysis.
- [x] `analysis_service.run_segmentation_for_analysis()` — integrity gate (analysis
  must be `completed` and already RFM-scored, else 404/409); rule segments always
  written; cluster ids merged only when K-Means actually ran.
- [x] Added thin route `POST /api/analyses/<id>/segment` (metrics endpoint already
  returns `segment` + `cluster_label`).
- [x] No new schema migration needed — `segment` and `cluster_label` are existing
  nullable columns on `customer_metrics`.
- [x] Added `tests/test_segmentation_service.py` (17 tests: every label boundary,
  missing-value fallback, full-grid exactly-one + reachability, K-Means skip /
  reproducibility / missing handling / k-by-evaluation, persistence, rerun
  idempotency, cross-analysis isolation, 404/409 guards, restart survival).
- [x] Verified full suite **95/95 passed** (78 prior + 17 new, zero regressions).
- [x] Live example on `sample_retail.csv`: 11 customers → 6 distinct segments
  (Champions 0, Loyal 2, Recent 2, Potential Loyalists 1, At Risk 2, Cannot Lose
  Them 2, Hibernating 2, Others 0); with K-Means enabled the evaluator self-selected
  **k=4** from the inertia curve {2:21.59, 3:14.69, 4:9.10, 5:8.64}.

---

## Phase 9 Checklist (Completed) — Analytics Dashboard & Visualization

Scope: read-only visualization of **persisted** analysis results, scoped per
`analysis_id`. Recommendations, exports, auth and multi-tenancy are deferred;
K-Means stays off by default and segmentation rules were not changed.

- [x] `services/dashboard_service.py`: a pure, read-only assembly module (no
  writes) that composes overview KPIs, segment breakdown and RFM analytics from
  persisted rows + the Spark `summary_json` rollup. Documents the 8 segment labels.
- [x] **Overview metrics** from the completed analysis rollup: eligible customers,
  valid orders, total revenue, total quantity, **average order value** (defined as
  revenue ÷ distinct valid orders, returned with an explicit `aov_definition`), and
  the analyzed `date_range` (first/last order date).
- [x] **Segmentation**: per-segment count + percentage (`count/segmented`, parts sum
  to ~100%), ordered by the canonical label ladder, each with a description;
  `available:false` distinguishes an un-run stage from a real zero.
- [x] **RFM analytics**: score distributions (recency/frequency/monetary, values
  1–5) built via SQL `GROUP BY`, plus min/max/avg/count summaries for recency_days,
  frequency, monetary and total_quantity. Distributions/summaries are omitted (not
  faked as 0) when the RFM stage has not run.
- [x] **Customer explorer**: server-side search, segment filter, whitelisted-column
  sort, and `LIMIT/OFFSET` pagination — the full cohort is never loaded into the
  browser. Returns `{items,total,page,page_size,pages}` with a clean empty page.
- [x] **Analysis info + switcher**: `GET /api/analyses` lists analyses (id, source
  dataset name, status, reference date, customers analyzed) newest-first so users
  switch without mixing results; every read is scoped by `analysis_id`.
- [x] Added read methods to `analysis_repository.py` (`list_all_with_sources`,
  `stage_flags`, `segment_distribution`, `score_distribution`, `metric_summaries`,
  `list_customers`) — SQL-side aggregation, parameterized values, whitelisted sort
  columns to prevent injection.
- [x] Added thin blueprint `routes/dashboard.py` (`/api`): `GET /api/analyses`,
  `GET /api/analyses/<id>/dashboard`, `GET /api/analyses/<id>/customers`. 404 on
  unknown analysis, 400 on invalid pagination params; registered in `app/__init__.py`.
- [x] Frontend (Bootstrap 5 + Chart.js 4.4.3 + vanilla JS, no new framework):
  `static/js/charts.js` (palette + chart registry that destroys before redraw),
  rebuilt `dashboard.html`/`.js` (KPIs, segment doughnut + table, RFM summary cards
  + score bar charts, stage notes), `customer_segments.html`/`.js` (distribution +
  segment cards linking to filtered customers), and `customer_details.html`/`.js`
  (search/filter/sort/paginate table with meaningful empty states + `aria-sort`).
  `validation.js` "Proceed to Analysis" now runs aggregate → RFM → segment and
  opens the dashboard on real data.
- [x] Added `tests/test_dashboard.py` (16 tests: empty list, overview matches
  aggregate, revenue equals customer-metric sum, segment counts/percentages, RFM
  distributions cover all scored customers, aggregate-only incomplete stage handled
  as unavailable-not-zero, pagination/sort/search/segment-filter, cross-analysis
  isolation, single-customer cohort (neutral bin 3 / 100%), 404s, bad-pagination
  400, no path/traceback leakage, `/metrics` regression, analyses list).
- [x] Verified full suite **111/111 passed** (95 prior + 16 new, zero regressions).
- [x] Live UI verification in the running app over `sample_retail.csv`: dashboard,
  segments and customers pages render real data (11 customers, 15 orders, revenue
  258.90, quantity 142, AOV 17.26, range 2010-12-01→2010-12-20; 6 segments; search,
  sort, segment drill-down and empty state confirmed) with no console/API errors.

---

## Phase 10 Checklist (Not Started) — Export

- [ ] Implement CSV/Excel export of customer metrics and segment summaries.

---

## Phase 11 Checklist (Not Started) — Hardening & Deployment

- [ ] Security headers / rate limiting, containerization, and production docs.

---

## Verification Commands

- Install: `pip install -r backend/requirements.txt` (+ `pip install openpyxl`)
- Run: `cd backend && python run.py` → http://127.0.0.1:5000
- Test (full suite): `cd backend && python -m pytest` → **95 passed**
- Persistence restart check: `cd backend && python restart_check.py write` then `python restart_check.py read`
- Aggregate a validated dataset: `POST /api/uploads/<id>/aggregate`
- Score RFM for a completed analysis: `POST /api/analyses/<id>/rfm` → read via `GET /api/analyses/<id>/metrics`
- Assign segments (after RFM): `POST /api/analyses/<id>/segment` (optional K-Means via `SEGMENTATION_ENABLE_KMEANS=1`)