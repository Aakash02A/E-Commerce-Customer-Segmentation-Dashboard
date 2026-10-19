"""Analysis integration tests: Spark -> SQLite persistence, isolation, errors."""
import os

from app.repositories import analysis_repository, upload_repository
from app.services import pyspark_service
from conftest import build_app, prepare_ready_upload

CUSTOM_MAPPING = {"Customer": "customer_id", "Invoice": "order_id",
                  "Date": "order_date", "Qty": "quantity", "Price": "unit_price"}


def _aggregate(client, uid):
    return client.post(f"/api/uploads/{uid}/aggregate", json={})


# --- Happy path ----------------------------------------------------------
def test_aggregate_ready_upload(client, sample_csv_path, spark):
    uid = prepare_ready_upload(client, sample_csv_path)
    resp = _aggregate(client, uid)
    assert resp.status_code == 201
    data = resp.get_json()["data"]
    assert data["status"] == "completed"
    assert data["customers_analyzed"] == data["transactions"]["distinct_customers"]
    assert data["customers_analyzed"] > 0
    assert data["transactions"]["total_spend"] is not None


def test_metrics_persisted_and_consistent(client, sample_csv_path, spark):
    uid = prepare_ready_upload(client, sample_csv_path)
    data = _aggregate(client, uid).get_json()["data"]
    aid = data["analysis_id"]
    with client.application.app_context():
        metrics = analysis_repository.get_metrics(aid)
        total_spend = sum(m["monetary"] for m in metrics)
        assert len(metrics) == data["customers_analyzed"]
        for m in metrics:
            assert m["frequency"] >= 1
            assert m["monetary"] is not None
            assert m["last_order_date"]
            assert m["segment"] is None          # Phase 7 territory
            assert m["r_score"] is None
        # sum of customer spend ~= dataset total (within rounding of cents)
        assert abs(total_spend - data["transactions"]["total_spend"]) <= 0.10
        row = analysis_repository.get(aid)
        assert row["upload_id"] == uid


def test_reference_date_is_last_purchase(client, sample_csv_path, spark):
    uid = prepare_ready_upload(client, sample_csv_path)
    data = _aggregate(client, uid).get_json()["data"]
    with client.application.app_context():
        row = analysis_repository.get(data["analysis_id"])
    assert row["reference_date"] == data["transactions"]["last_date"]


# --- Integrity guards ----------------------------------------------------
def test_aggregate_requires_ready_status(client, sample_csv_path, spark):
    # Upload only -> not ready -> 409, no analysis created
    with open(sample_csv_path, "rb") as fh:
        uid = client.post("/api/uploads", data={"file": (fh, os.path.basename(sample_csv_path))},
                          content_type="multipart/form-data").get_json()["data"]["upload_id"]
    resp = _aggregate(client, uid)
    assert resp.status_code == 409
    with client.application.app_context():
        assert analysis_repository.list_for_upload(uid) == []


def test_aggregate_unknown_upload_404(client):
    assert _aggregate(client, "nonexistent").status_code == 404


def test_no_processing_when_eligible_file_missing(client, sample_csv_path, spark):
    """Never fall back to the raw/normalized file: refuse if eligible is absent."""
    uid = prepare_ready_upload(client, sample_csv_path)
    report = upload_repository_latest(client, uid)
    os.remove(report["eligible_path"])
    resp = _aggregate(client, uid)
    assert resp.status_code == 409
    with client.application.app_context():
        assert analysis_repository.list_for_upload(uid) == []


def upload_repository_latest(client, uid):
    with client.application.app_context():
        return upload_repository.latest_report(uid)


# --- Error handling (no path/traceback leakage) --------------------------
def test_spark_failure_is_reported_safely(client, sample_csv_path, spark, monkeypatch):
    uid = prepare_ready_upload(client, sample_csv_path)

    def _boom(*a, **k):
        raise pyspark_service.SparkProcessingError("The analytics engine could not be started.")

    monkeypatch.setattr(pyspark_service, "run_aggregation", _boom)
    resp = _aggregate(client, uid)
    body = resp.get_json()
    assert resp.status_code == 500
    assert body["success"] is False
    text = resp.get_data(as_text=True)
    assert "Traceback" not in text
    assert "\\" not in text  # no Windows filesystem paths leaked
    aid = None
    with client.application.app_context():
        rows = analysis_repository.list_for_upload(uid)
        aid = rows[0]["id"]
        assert rows[0]["status"] == "failed"


# --- Isolation between two uploads ---------------------------------------
def test_two_uploads_do_not_reuse_each_others_results(client, tmp_path, spark):
    ds1 = tmp_path / "ds1.csv"
    ds1.write_text("Customer,Invoice,Date,Qty,Price\nA1,I1,2024-01-01,1,10.0\n", encoding="utf-8")
    ds2 = tmp_path / "ds2.csv"
    ds2.write_text("Customer,Invoice,Date,Qty,Price\nB7,I9,2024-03-03,2,5.0\n", encoding="utf-8")

    uid1 = prepare_ready_upload(client, str(ds1), CUSTOM_MAPPING)
    uid2 = prepare_ready_upload(client, str(ds2), CUSTOM_MAPPING)
    d1 = _aggregate(client, uid1).get_json()["data"]
    d2 = _aggregate(client, uid2).get_json()["data"]

    assert d1["analysis_id"] != d2["analysis_id"]
    with client.application.app_context():
        c1 = {m["customer_id"] for m in analysis_repository.get_metrics(d1["analysis_id"])}
        c2 = {m["customer_id"] for m in analysis_repository.get_metrics(d2["analysis_id"])}
        assert c1 == {"A1"} and c2 == {"B7"}
        assert analysis_repository.get(d1["analysis_id"])["upload_id"] == uid1
        assert analysis_repository.get(d2["analysis_id"])["upload_id"] == uid2


# --- Persistence across restart ------------------------------------------
def test_aggregate_results_survive_restart(tmp_path, sample_csv_path):
    root = tmp_path / "spark_restart"
    app1 = build_app(root)
    c1 = app1.test_client()
    uid = prepare_ready_upload(c1, sample_csv_path)
    aid = _aggregate(c1, uid).get_json()["data"]["analysis_id"]

    app2 = build_app(root)  # fresh app instance over the same DB/storage
    c2 = app2.test_client()
    listing = c2.get(f"/api/uploads/{uid}/analyses").get_json()["data"]["analyses"]
    assert any(a["id"] == aid and a["status"] == "completed" for a in listing)

    got = c2.get(f"/api/analyses/{aid}").get_json()["data"]
    assert got["status"] == "completed"
    assert got["customers_analyzed"] > 0
    assert got["summary"]["transactions"]["total_spend"] is not None
