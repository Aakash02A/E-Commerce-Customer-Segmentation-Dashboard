"""Application configuration for the E-Commerce Customer Segmentation Dashboard."""
from pathlib import Path

import os

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # project root
BACKEND_DIR = Path(__file__).resolve().parent.parent      # backend/
FRONTEND_DIR = BASE_DIR / "frontend"


class Config:
    """Base configuration. Values can be overridden via environment variables."""

    # --- Paths ---------------------------------------------------------
    BASE_DIR = BASE_DIR
    FRONTEND_DIR = BASE_DIR / "frontend"
    DATABASE_PATH = Path(os.getenv("DATABASE_PATH", BASE_DIR / "database" / "ecommerce.db"))
    UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", BASE_DIR / "storage" / "uploads"))
    PROCESSED_DIR = Path(os.getenv("PROCESSED_DIR", BASE_DIR / "storage" / "processed"))
    EXPORTS_DIR = Path(os.getenv("EXPORTS_DIR", BASE_DIR / "storage" / "exports"))
    TEMP_DIR = Path(os.getenv("TEMP_DIR", BASE_DIR / "storage" / "temp"))

    # --- Upload limits -------------------------------------------------
    MAX_CONTENT_LENGTH = int(os.getenv("MAX_CONTENT_LENGTH", 100 * 1024 * 1024))  # 100 MB
    ALLOWED_EXTENSIONS = {"csv", "xlsx"}

    # Upload retention (days). Files older than this may be purged.
    UPLOAD_RETENTION_DAYS = int(os.getenv("UPLOAD_RETENTION_DAYS", 30))

    # --- Data quality thresholds --------------------------------------
    # Values above mean + N * std of unit_price are flagged as suspicious.
    PRICE_OUTLIER_STD_MULTIPLIER = 6.0

    # --- Spark ----------------------------------------------------------
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
    SPARK_APP_NAME = "CustomerSegmentationAnalytics"

    # --- Segmentation (Phase 8) ---------------------------------------
    # Rule-based RFM segmentation is always the authoritative, interpretable
    # label. K-Means clustering is OPTIONAL and off by default: on small/real
    # MVP cohorts it rarely adds reliable signal, and it must never override a
    # rule-based segment. Enable via env only after reviewing its evaluation.
    SEGMENTATION_ENABLE_KMEANS = os.getenv("SEGMENTATION_ENABLE_KMEANS", "0") == "1"
    KMEANS_RANDOM_SEED = int(os.getenv("KMEANS_RANDOM_SEED", 42))  # reproducible
    KMEANS_MAX_K = int(os.getenv("KMEANS_MAX_K", 5))
    # Below this many score-complete customers we skip clustering entirely
    # (inertia/elbow is not trustworthy on tiny cohorts).
    KMEANS_MIN_CUSTOMERS = int(os.getenv("KMEANS_MIN_CUSTOMERS", 10))

    # --- Misc -----------------------------------------------------------
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-local-single-user")
    JSON_SORT_KEYS = False

    # --- Security headers / CSP (Phase 11) ------------------------------
    # Emitted on HTML/JSON responses. See app/services/security_service.py.
    SECURITY_HEADERS_ENABLED = os.getenv("SECURITY_HEADERS_ENABLED", "1") == "1"
    # The ONLY external origin the frontend loads (Bootstrap/Bootstrap-Icons/
    # Chart.js). script-src stays strict ('self' + this host); inline scripts
    # are being externalized so 'unsafe-inline' is NOT required for scripts.
    CDN_ORIGIN = os.getenv("CDN_ORIGIN", "https://cdn.jsdelivr.net")
    # style-src needs 'unsafe-inline' because Bootstrap JS + Chart.js set element
    # styles at runtime (measured: zero inline style= attributes in our templates).
    CSP_STYLE_SOURCES = ["'self'", os.getenv("CDN_ORIGIN", "https://cdn.jsdelivr.net"), "'unsafe-inline'"]
    CSP_FRAME_ANCESTORS = os.getenv("CSP_FRAME_ANCESTORS", "'none'")
    X_FRAME_OPTIONS = os.getenv("X_FRAME_OPTIONS", "DENY")
    REFERRER_POLICY = os.getenv("REFERRER_POLICY", "no-referrer")


class DevelopmentConfig(Config):
    DEBUG = True


class TestingConfig(Config):
    TESTING = True
    DEBUG = True


class ProductionConfig(Config):
    DEBUG = False


_CONFIG_MAP = {
    "development": DevelopmentConfig,
    "testing": TestingConfig,
    "production": ProductionConfig,
}


def get_config(name: str | None = None) -> type[Config]:
    env = name or os.getenv("FLASK_ENV", "development")
    return _CONFIG_MAP.get(env, DevelopmentConfig)
