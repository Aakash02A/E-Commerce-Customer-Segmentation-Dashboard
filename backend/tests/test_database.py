"""SQLite persistence, transaction, and data-integrity tests (Phase 5)."""
import sqlite3

import pytest

from app.database.connection import foreign_keys_enabled, get_db, transaction
from app.repositories import upload_repository
from conftest import build_app


def _sample_mapping():
    return {"Order_Ref": "order_id", "Units": "quantity", "Unit_Cost": "unit_price",
            "Client": "customer_id", "Purchase_Date": "order_date"}


def _full_pipeline(client, csv_path):
    """Upload -> confirm mapping -> validate; returns (upload_id, report, status)."""
    import os
    with open(csv_path, "rb") as fh:
        r = client.post("/api/uploads", data={"file": (fh, os.path.basename(csv_path))},
                        content_type="multipart/form-data")
    uid = r.get_json()["data"]["upload_id"]
    client.post(f"/api/uploads/{uid}/mapping", json={"mapping": _sample_mapping()})
    v = client.post(f"/api/uploads/{uid}/validate", json={})
    return uid, v.get_json()["data"]["report"], v.get_json()["data"]["status"]


# --- Schema integrity & FK ------------------------------------------------
def test_foreign_keys_enabled(app):
    assert foreign_keys_enabled(app) is True


def test_all_tables_present(app):
    with app.app_context():
        names = {r["name"] for r in get_db().execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"uploads", "data_quality_reports", "analyses", "customer_metrics"} <= names


def test_report_requires_existing_upload(app):
    """FK constraint blocks a quality report for a non-existent upload."""
    with app.app_context():
        db = get_db()
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO data_quality_reports (upload_id, report_json, total_rows,"
                " valid_rows, invalid_rows, passed) VALUES (?,?,?,?,?,?)",
                ("does-not-exist", "{}", 0, 0, 0, 0),
            )
        db.rollback()


# --- Repository write guards ---------------------------------------------
def test_update_rejects_unknown_column(client, sample_csv_path):
    uid, _, _ = _full_pipeline(client, sample_csv_path)
    with client.application.app_context():
        with pytest.raises(ValueError):
            upload_repository.update(uid, not_a_real_column="x")


def test_update_rejects_invalid_status(client, sample_csv_path):
    uid, _, _ = _full_pipeline(client, sample_csv_path)
    with client.application.app_context():
        with pytest.raises(ValueError):
            upload_repository.update(uid, status="bogus_state")


def test_record_validation_result_rejects_bad_status(client, sample_csv_path):
    uid, report, _ = _full_pipeline(client, sample_csv_path)
    with client.application.app_context():
        before = upload_repository.report_count(uid)
        with pytest.raises(ValueError):
            upload_repository.record_validation_result(uid, report, "not_a_status")
        # nothing added
        assert upload_repository.report_count(uid) == before


# --- Transaction atomicity -----------------------------------------------
def test_transaction_rolls_back_on_error(app):
    with app.app_context():
        db = get_db()
        db.execute(
            "INSERT INTO uploads (id, original_filename, stored_filename, file_path,"
            " file_size, extension, status) VALUES ('tx-test','a','b','c',1,'csv','uploaded')")
        db.commit()
        before = db.execute("SELECT COUNT(*) c FROM uploads").fetchone()["c"]

        with pytest.raises(RuntimeError):
            with transaction() as tdb:
                tdb.execute(
                    "INSERT INTO uploads (id, original_filename, stored_filename,"
                    " file_path, file_size, extension, status) VALUES ('tx-2','a','b','c',1,'csv','uploaded')")
                raise RuntimeError("boom")

        after = get_db().execute("SELECT COUNT(*) c FROM uploads").fetchone()["c"]
        assert after == before  # the second insert was rolled back


def test_record_validation_result_is_atomic(client, sample_csv_path):
    """Report + status write together and are readable in one consistent state."""
    uid, report, status = _full_pipeline(client, sample_csv_path)
    with client.application.app_context():
        row = upload_repository.get(uid)
        stored = upload_repository.latest_report(uid)
    assert row["status"] == status == "ready_for_analysis"
    assert stored["summary"]["valid_rows"] == report["summary"]["valid_rows"]
    assert stored["passed"] is True


# --- Persistence across restart ------------------------------------------
def test_persistence_survives_restart(tmp_path, sample_csv_path):
    """Data written by one app instance is fully readable after a fresh restart."""
    root = tmp_path / "shared"
    app1 = build_app(root)
    c1 = app1.test_client()
    uid, report, status = _full_pipeline(c1, sample_csv_path)

    # New app instance + new test client: nothing but the same DB/storage on disk.
    app2 = build_app(root)
    c2 = app2.test_client()

    # uploads row persists
    r = c2.get(f"/api/uploads/{uid}")
    assert r.status_code == 200
    assert r.get_json()["data"]["status"] == "ready_for_analysis"

    # mapping persists
    rm = c2.get(f"/api/uploads/{uid}/mapping")
    assert rm.get_json()["data"]["confirmed_mapping"] == _sample_mapping()

    # quality report persists and matches what was produced
    rv = c2.get(f"/api/uploads/{uid}/validate")
    persisted = rv.get_json()["data"]["report"]
    assert persisted["summary"] == report["summary"]
    assert persisted["passed"] is True

    # eligible artifact persists on disk
    with app2.app_context():
        row = upload_repository.get(uid)
    assert row["normalized_path"] and (root / "storage" / "processed").exists()


# --- Isolation between analyses ------------------------------------------
def test_two_uploads_are_isolated(tmp_path, sample_csv_path):
    app = build_app(tmp_path / "iso")
    client = app.test_client()
    uid1, _, _ = _full_pipeline(client, sample_csv_path)
    uid2, _, _ = _full_pipeline(client, sample_csv_path)
    assert uid1 != uid2

    # Corrupting/inspecting one does not affect the other's stored mapping.
    with app.app_context():
        upload_repository.update(uid1, status="failed")
        assert upload_repository.get(uid2)["status"] == "ready_for_analysis"
        assert upload_repository.get(uid1)["status"] == "failed"

    r = client.get(f"/api/uploads/{uid2}/validate")
    assert r.get_json()["data"]["report"]["passed"] is True


# --- History reflects persisted state ------------------------------------
def test_history_lists_persisted_datasets(client, sample_csv_path):
    _full_pipeline(client, sample_csv_path)
    r = client.get("/api/uploads")
    uploads = r.get_json()["data"]["uploads"]
    assert len(uploads) == 1
    assert uploads[0]["status"] == "ready_for_analysis"

    s = client.get("/api/uploads/summary").get_json()["data"]
    assert s["total_uploads"] == 1
    assert s["ready_for_analysis"] == 1
