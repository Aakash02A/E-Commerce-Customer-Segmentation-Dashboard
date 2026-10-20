# Deployment & Operations Guide

This is the authoritative production/deployment reference for the E-Commerce Customer
Segmentation Dashboard. The [README](README.md) keeps a short quick-start and links here.

Everything below is derived from the actual code (`backend/app/config.py`,
`backend/app/services/security_service.py`, `backend/app/services/rate_limit_service.py`,
`backend/serve.py`, `backend/wsgi.py`, `Dockerfile`, `docker-compose.yml`) and from the
verification performed in Phase 11. **This is a local, single-user MVP — see
[Security limitations](#12-security-headers-csp-rate-limiting-and-known-limitations) before
any public exposure.**

---

## 1. Prerequisites and supported platforms

| Component | Version | Notes |
|---|---|---|
| Python | **3.13** | Verified 3.13.14 |
| Java (JDK/JRE) | **21 LTS** | Required by PySpark 4.2 (supports Java 17/21); verified 21.0.12 |
| PySpark | 4.2.0 | Runs `local[*]` (single-machine), not a cluster claim |
| Flask | 3.0.3 | Served by a production WSGI server, **not** `flask run` |

Supported platforms:

- **Native Windows** (development and single-host production via Waitress).
- **Linux / Docker** (production via Gunicorn, containerized).

Verify runtimes:

```powershell
python --version    # 3.13.x
java -version       # 21.x
```

---

## 2. Installing dependencies

```powershell
pip install -r backend/requirements.txt
```

`requirements.txt` pins the runtime and includes **Waitress** (cross-platform) and
**Gunicorn** (`gunicorn==23.0.0; sys_platform != "win32"` — the marker skips it on
Windows, since Gunicorn is POSIX-only). `pytest` and `python-dotenv` are also pinned;
the Docker image installs the same manifest for deterministic builds.

---

## 3. Configuration and environment variables

All settings are environment-overridable and centralized in `backend/app/config.py`
(dev/testing/production classes). `backend/.env.example` documents them with safe
defaults; a real `.env` is gitignored. **Never commit secrets or absolute host paths.**

Core settings:

| Variable | Default | Purpose |
|---|---|---|
| `FLASK_ENV` | `development` | Config profile: `development` / `testing` / `production` |
| `SECRET_KEY` | `dev-local-single-user` | Flask signing key — **set a real value in any shared deployment** |
| `FLASK_RUN_HOST` / `FLASK_RUN_PORT` | `127.0.0.1` / `5000` | Waitress bind (also used by dev `run.py`) |
| `DATABASE_PATH` | `database/ecommerce.db` | SQLite file (resolved from repo root) |
| `UPLOAD_DIR` / `PROCESSED_DIR` / `EXPORTS_DIR` / `TEMP_DIR` | under `storage/` | File storage directories |
| `MAX_CONTENT_LENGTH` | `104857600` (100 MB) | Upload size limit |
| `SPARK_MASTER` | `local[*]` | Spark master |
| `SEGMENTATION_ENABLE_KMEANS` | `0` | Optional K-Means (`1` to enable) |

Security headers / CSP:

| Variable | Default |
|---|---|
| `SECURITY_HEADERS_ENABLED` | `1` |
| `CDN_ORIGIN` | `https://cdn.jsdelivr.net` |
| `CSP_FRAME_ANCESTORS` | `'none'` |
| `X_FRAME_OPTIONS` | `DENY` |
| `REFERRER_POLICY` | `no-referrer` |

Rate limiting:

| Variable | Default |
|---|---|
| `RATE_LIMIT_ENABLED` | `1` |
| `RATE_LIMIT_WINDOW_SECONDS` | `60` |
| `RATE_LIMIT_EXPENSIVE` | `10` |
| `RATE_LIMIT_STANDARD` | `30` |

Invalid security/CSP or rate-limit values are **rejected at startup** with a fail-fast
`ValueError` (the app refuses to serve with a weakened policy or a malformed limit).

---

## 4. Running the application

`run.py` starts **Flask's development server** (debug/reloader) — use it for local
development only. For production use one of the paths below. Both load the app through
the `create_app()` factory with `FLASK_ENV=production` (DEBUG off, no reloader).

### 4.1 Native Windows — Waitress (`serve.py`)

```powershell
cd backend
$env:FLASK_ENV="production"      # optional; serve.py already defaults to production
$env:FLASK_RUN_HOST="127.0.0.1"  # default
$env:FLASK_RUN_PORT="5000"       # default
$env:WAITRESS_THREADS="4"        # default
python serve.py
```

`serve.py` calls `waitress.serve(app, ..., ident="")`. The `ident=""` argument is the
**documented, supported** way to make Waitress emit **no `Server` response header** —
verified against a live server (see [headers](#12-security-headers-csp-rate-limiting-and-known-limitations)).

### 4.2 Linux — Gunicorn (`wsgi:app`)

```bash
cd backend
FLASK_ENV=production gunicorn wsgi:app \
  --bind 0.0.0.0:5000 \
  --workers 1 --threads 4 \
  --timeout 120 --graceful-timeout 30 --keep-alive 5
```

- **One worker, deliberately:** the app embeds a single lazily-shared PySpark JVM. Each
  extra worker forks another JVM (memory + lifecycle risk), so do **not** raise the
  worker count without re-verifying the PySpark lifecycle and memory.
- **Threads (4)** provide request concurrency within that one process.
- **`--timeout 120`** tolerates Spark session startup and long analyses (a short default
  timeout would kill legitimate work).

### 4.3 Stopping / restarting

Send `SIGINT`/`SIGTERM` (Ctrl-C in a shell; `docker compose stop` for the container).
Spark tears down with the process; SQLite data persists on disk. Restarting simply
re-reads the database.

---

## 5. Data, storage, and persistence

- **SQLite** lives at `DATABASE_PATH` (default `database/ecommerce.db`, resolved from the
  repo root). It survives process restarts and container recreation **when the directory
  is persisted** (see the Docker volumes below).
- **Uploads / processed / exports / temp** live under `storage/` (sub-locations
  configurable per directory). The eligible/normalized CSVs that Spark reads are written
  under `storage/processed/`, so that path must be persisted alongside the DB to keep
  completed analyses fully reproducible.
- On first run the app creates the database and storage directories automatically.

---

## 6. Docker build and Compose startup

Files: `Dockerfile`, `docker-compose.yml`, `.dockerignore` (all at the repo root).

```powershell
docker compose up --build -d      # build + run in background
docker compose logs -f web         # follow startup logs
curl.exe -i http://127.0.0.1:5000/api/health
docker compose down                # stop; named volumes (data) are RETAINED
docker compose down -v             # stop AND DELETE the data volumes
```

The image (`~1.68 GB`) is based on `python:3.13-slim` with **OpenJDK 21**, `curl` (health
check) and `tini` (PID 1 for graceful shutdown). It runs **Gunicorn** with the same
1-worker / 4-thread / 120s-timeout profile as §4.2, as the **non-root `appuser` (uid
10001)**, with a `HEALTHCHECK` against `/api/health`.

Compose provides:

- **Named volumes** `app_database` (`/app/database`) and `app_storage` (`/app/storage`)
  so SQLite + uploaded/processed/exported files **survive container recreation**.
- **Loopback-only** port publish (`127.0.0.1:5000:5000`) — not exposed to the network.
- `mem_limit: 3g` as a documented PySpark JVM budget; adjust to the host.
- `cap_drop: ALL` and `no-new-privileges:true` for a tighter security posture.
- `SECRET_KEY` passed through **only when set on the host** (list-form env), so no empty
  value overrides the app default and no secret is committed:

  ```powershell
  $env:SECRET_KEY = "generate-a-real-value"; docker compose up -d
  ```

A fresh container starts with **no files from the developer's machine** — build-context
exclusions in `.dockerignore` keep host `database/`, `storage/`, `.git`, caches, `.env`,
and `assets/` out of the image.

---

## 7. Health check and smoke verification

```powershell
curl.exe -i http://127.0.0.1:5000/api/health          # 200 {"success":true,"status":"ok"}
curl.exe -i http://127.0.0.1:5000/                    # home page 200 + security headers
```

Full end-to-end workflow (upload → mapping → validation → **PySpark** aggregate → RFM →
segment → dashboard → export) is covered by the automated suite and was additionally run
**inside the container** and **over live HTTP** during Phase 11 (see the completion
report). The canonical example on `sample_data/sample_retail.csv` yields 11 customers,
6 segments, revenue 258.90 and AOV 17.26.

---

## 8. Running the tests

```powershell
cd backend
python -m pytest                 # full suite (currently 233 passed, 1 pre-existing warning)
```

Inside the built image (proves PySpark/Java work in the container):

```bash
docker compose exec web python -m pytest
```

---

## 9. Reverse proxy and forwarded headers

By default the rate limiter identifies clients by **`request.remote_addr`** (the direct
socket peer) and **does not trust `X-Forwarded-For`** — a spoofable header cannot be used
to evade limits. This is correct when the app is reached directly.

If you place the app behind a **trusted reverse proxy** (nginx, a load balancer) that
overwrites/validates forwarded headers, you may enable header handling (e.g. Werkzeug
`ProxyFix`) **and** restrict it to that proxy's known IPs. Configure `TRUSTED_PROXIES`
only with addresses you control; never trust arbitrary client-supplied `X-Forwarded-For`.
This was intentionally left off for the loopback MVP.

> Gunicorn's own metrics/logging bind and TLS termination are also the proxy's job in a
> public deployment. See the next section for the `Server`-header note.

---

## 10. PySpark and memory/Java considerations

- Spark runs `local[*]` inside the single Gunicorn worker / Waitress process; the JVM is
  created lazily on first analysis and reused.
- In Docker, keep the container memory budget comfortably above the JVM heap you expect
  (default `mem_limit: 3g`); Spark inherits `JAVA_HOME` from the image.
- The Spark Web UI is disabled (`spark.ui.enabled=false`) and the driver binds to
  `127.0.0.1` — do not expose it.
- Do **not** run multiple Gunicorn workers with this architecture without re-validating
  JVM memory and startup cost.

---

## 11. Before any public internet exposure

This MVP is **single-user with no authentication**. Publishing it as-is would leak every
user's uploads, results and history. Before exposing publicly you must revisit, at
minimum:

- **Authentication + per-user ownership** across uploads, analyses and exports.
- **HTTPS/TLS** termination (reverse proxy) and secure cookie settings.
- A real **`SECRET_KEY`** from a secret store, not the default.
- **Network binding / firewall** (stop binding loopback-only, but restrict ingress).
- **Rate limiting across processes** — the in-memory limiter is per-process only; a
  multi-worker or multi-instance deployment needs a shared store (out of current scope).
- **Resource limits / isolation** for uploads and long-running Spark jobs.

---

## 12. Security headers, CSP, rate limiting, and known limitations

- **CSP + hardening headers** (emitted by `security_service.py` on HTML/JSON responses):
  strict `script-src 'self' <CDN_ORIGIN>` with **no `unsafe-inline`/`unsafe-eval`**
  (inline scripts were externalized), `X-Content-Type-Options: nosniff`, `X-Frame-Options`,
  `Referrer-Policy`, `Cross-Origin-Opener-Policy`, `Permissions-Policy`. File downloads
  (CSV/XLSX exports) are intentionally exempt from CSP.
- **Rate limiting** (`rate_limit_service.py`): in-memory sliding-window on mutating
  `/api/` endpoints only, tiered (expensive: upload/aggregate/rfm/segment; standard: other
  writes). Returns a JSON `429` with `Retry-After` + `X-RateLimit-*` headers. **Process-local
  and resets on restart** — not distributed or persistent.
- **`Server` header — verified behavior:**
  - **Waitress:** suppressed cleanly via the supported `ident=""` option → **no `Server`
    header** (verified against a live Windows server).
  - **Gunicorn:** always emits `Server: gunicorn` and exposes **no supported flag** to
    remove it (confirmed in `gunicorn/http/wsgi.py`). If the banner must be hidden, strip
    it at a **trusted reverse proxy** (e.g. nginx `proxy_hide_header Server;` +
    `server_tokens off;`). This limitation is documented rather than worked around with
    brittle monkey-patching.
- **Exports** remain formula-injection-safe with whitelisted filenames.
- **Image size** (~1.68 GB) reflects the Python + JDK 21 + PySpark runtime; it was not
  reduced at the cost of a working Spark runtime.
