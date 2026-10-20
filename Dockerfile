# Production image for the E-Commerce Customer Segmentation Dashboard.
# Target runtime: Python 3.13 + JDK 21 (PySpark 4.2 supports Java 17/21) +
# Flask served by Gunicorn. SQLite persistence lives in a mounted volume.

FROM python:3.13-slim

# --- Runtime / JVM configuration -----------------------------------------
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    FLASK_ENV=production \
    PYTHONPATH=/app/backend \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# --- System dependencies --------------------------------------------------
# openjdk-21-jre-headless: Spark JVM runtime.
# curl: used only for the container health check.
# tini: proper PID 1 signal handling / zombie reaping for graceful shutdown.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        curl \
        tini \
    && rm -rf /var/lib/apt/lists/*

# --- Application code -----------------------------------------------------
# Non-root runtime user. Writable app tree so the JVM, SQLite and the upload/
# export directories can be used by the application process.
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin appuser

WORKDIR /app
COPY . /app

# The app derives its data paths from the repo root (/app): database/ and
# storage/{uploads,processed,exports,temp}. Create them and hand ownership to
# appuser before dropping privileges. Build context excludes these dirs, so
# they are created fresh here (real data arrives via mounted volumes).
RUN mkdir -p /app/database \
        /app/storage/uploads /app/storage/processed \
        /app/storage/exports /app/storage/temp \
    && pip install --no-cache-dir -r backend/requirements.txt \
    && chown -R appuser:appuser /app

USER appuser

# Gunicorn runs the app (NOT Flask's dev server). One worker because the app
# embeds a single PySpark JVM; threads give concurrency without extra JVMs.
# --timeout is high to tolerate Spark session startup + long analyses.
# NOTE: Gunicorn always emits a `Server` response header and exposes no
# supported switch to remove it (see gunicorn/http/wsgi.py); if the banner must
# be hidden, strip it at a trusted reverse proxy (e.g. `proxy_hide_header Server;`).
WORKDIR /app/backend
EXPOSE 5000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:5000/api/health || exit 1

# tini as PID 1 forwards SIGTERM/SIGINT to Gunicorn for graceful shutdown.
ENTRYPOINT ["tini", "--"]
CMD ["gunicorn", "wsgi:app", \
     "--bind", "0.0.0.0:5000", \
     "--workers", "1", \
     "--threads", "4", \
     "--timeout", "120", \
     "--graceful-timeout", "30", \
     "--keep-alive", "5"]
