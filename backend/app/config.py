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

    # --- Misc -----------------------------------------------------------
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-local-single-user")
    JSON_SORT_KEYS = False


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
