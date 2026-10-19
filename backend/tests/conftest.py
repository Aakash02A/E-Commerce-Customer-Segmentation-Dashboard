"""Shared pytest fixtures.

Each test gets an isolated SQLite database and storage directory (created under
a per-test tmp_path) so persistence, FK, and history tests never leak between
cases. A helper builds an app against an EXISTING database path so tests can
prove persistence survives an application restart.
"""
import os
import tempfile
from pathlib import Path

# Config reads these at import time; create_app runs an initial init_db on the
# default path before tests override it per-case, so keep that default throwaway.
_DEFAULT_ROOT = tempfile.mkdtemp(prefix="seg_test_default_")
os.environ["FLASK_ENV"] = "testing"
os.environ["DATABASE_PATH"] = os.path.join(_DEFAULT_ROOT, "default.db")
os.environ["UPLOAD_DIR"] = os.path.join(_DEFAULT_ROOT, "uploads")
os.environ["PROCESSED_DIR"] = os.path.join(_DEFAULT_ROOT, "processed")
os.environ["EXPORTS_DIR"] = os.path.join(_DEFAULT_ROOT, "exports")
os.environ["TEMP_DIR"] = os.path.join(_DEFAULT_ROOT, "temp")

import pytest

from app import create_app
from app.database.connection import init_db

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(BACKEND_DIR)
SAMPLE_DIR = os.path.join(PROJECT_ROOT, "sample_data")


def _point_app_at(app, root):
    """Set an app's DB + storage paths under `root` and initialize the schema."""
    root = Path(root)
    app.config["DATABASE_PATH"] = root / "test.db"
    for key, name in [("UPLOAD_DIR", "uploads"), ("PROCESSED_DIR", "processed"),
                      ("EXPORTS_DIR", "exports"), ("TEMP_DIR", "temp")]:
        d = root / "storage" / name
        d.mkdir(parents=True, exist_ok=True)
        app.config[key] = d
    root.mkdir(parents=True, exist_ok=True)
    init_db(app)
    return app


def build_app(db_root):
    """Create a fresh app bound to a specific storage/db root (used for restarts)."""
    return _point_app_at(create_app("testing"), db_root)


@pytest.fixture
def app(tmp_path):
    yield _point_app_at(create_app("testing"), tmp_path / "app")


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def sample_csv_path():
    return os.path.join(SAMPLE_DIR, "sample_retail.csv")


@pytest.fixture
def sample_xlsx_path():
    return os.path.join(SAMPLE_DIR, "sample_retail.xlsx")


@pytest.fixture
def sample_multi_sheet_path():
    return os.path.join(SAMPLE_DIR, "sample_multi_sheet.xlsx")


def make_csv(tmp_path, text, name="data.csv"):
    p = tmp_path / name
    p.write_text(text, encoding="utf-8")
    return p


# --- PySpark / analysis fixtures & helpers --------------------------------
SAMPLE_STANDARD_MAPPING = {
    "Order_Ref": "order_id", "Units": "quantity", "Unit_Cost": "unit_price",
    "Client": "customer_id", "Purchase_Date": "order_date",
}


@pytest.fixture
def spark():
    """Function-scoped view of the shared Spark session.

    Uses the lazily-created singleton so the whole suite pays startup once, but
    a test that stops the session (shutdown test) lets later tests transparently
    recreate it.
    """
    from app.services import pyspark_service
    return pyspark_service.get_session(master="local[1]", app_name="pytest-spark")


def prepare_ready_upload(client, csv_path, mapping=None):
    """Run upload -> confirm mapping -> validate; return the ready upload id."""
    mapping = mapping or SAMPLE_STANDARD_MAPPING
    with open(csv_path, "rb") as fh:
        resp = client.post(
            "/api/uploads",
            data={"file": (fh, os.path.basename(csv_path))},
            content_type="multipart/form-data",
        )
    uid = resp.get_json()["data"]["upload_id"]
    client.post(f"/api/uploads/{uid}/mapping", json={"mapping": mapping})
    client.post(f"/api/uploads/{uid}/validate", json={})
    return uid


def write_eligible_csv(tmp_path, body, name="eligible.csv"):
    """Write a CSV in the eligible-artifact format (with header row)."""
    header = "customer_id,order_id,order_date,quantity,unit_price\n"
    p = tmp_path / name
    p.write_text(header + body, encoding="utf-8")
    return p

