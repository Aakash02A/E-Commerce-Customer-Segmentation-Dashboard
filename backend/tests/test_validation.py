"""Data-quality validation tests (Phase 4) + end-to-end pipeline flow."""
import io
import os

import pandas as pd

from app.services import data_quality_service as dq
from app.services import schema_normalization_service as sn
from conftest import make_csv


def _typed(**cols):
    df = pd.DataFrame(cols)
    if "order_date" in df:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    for c in ("quantity", "unit_price"):
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# --- unit: value handling -------------------------------------------------
def test_missing_customer_id_excluded():
    df = _typed(customer_id=["C1", None, "C3"],
                order_date=["2020-01-01", "2020-01-02", "2020-01-03"],
                quantity=["1", "2", "3"], unit_price=["1", "2", "3"])
    report = dq.validate(df, mapping={"customer_id": "customer_id"})
    assert report["passed"] is True
    assert report["summary"]["valid_rows"] == 2


def test_negative_quantity_is_return_excluded_by_default():
    df = _typed(customer_id=["C1", "C2"],
                order_date=["2020-01-01", "2020-01-02"],
                quantity=["5", "-2"], unit_price=["1", "1"])
    report = dq.validate(df, mapping={})
    assert report["summary"]["valid_rows"] == 1


def test_negative_quantity_kept_when_policy_keep():
    df = _typed(customer_id=["C1", "C2"],
                order_date=["2020-01-01", "2020-01-02"],
                quantity=["5", "-2"], unit_price=["1", "1"])
    report = dq.validate(df, mapping={}, rules={"negative_quantity_policy": "keep"})
    assert report["summary"]["valid_rows"] == 2


def test_invalid_date_excluded():
    df = _typed(customer_id=["C1", "C2"],
                order_date=["2020-13-99", "2020-01-02"],  # first is invalid
                quantity=["5", "2"], unit_price=["1", "1"])
    report = dq.validate(df, mapping={})
    assert report["summary"]["valid_rows"] == 1


def test_non_numeric_quantity_excluded():
    df = _typed(customer_id=["C1", "C2"],
                order_date=["2020-01-01", "2020-01-02"],
                quantity=["abc", "2"], unit_price=["1", "1"])
    report = dq.validate(df, mapping={})
    assert report["summary"]["valid_rows"] == 1


def test_cancelled_invoice_by_order_id_prefix():
    df = _typed(customer_id=["C1", "C2"],
                order_id=["489", "C500"],
                order_date=["2020-01-01", "2020-01-02"],
                quantity=["5", "2"], unit_price=["1", "1"])
    report = dq.validate(df, mapping={})
    assert report["summary"]["valid_rows"] == 1
    assert any(e["code"] == "cancelled_invoice" for e in report["excluded"])


def test_missing_required_field_blocks():
    df = pd.DataFrame({"customer_id": ["C1"], "quantity": [1]})
    report = dq.validate(df, mapping={})
    assert report["passed"] is False
    assert any("order_date" in (e.get("message") or "") for e in report["blocking_errors"])


def test_all_rows_invalid_blocks():
    df = _typed(customer_id=[None, None],
                order_date=["2020-01-01", "2020-01-02"],
                quantity=["1", "2"], unit_price=["1", "2"])
    report = dq.validate(df, mapping={})
    assert report["passed"] is False


def test_duplicates_kept_by_default():
    df = _typed(customer_id=["C1", "C1"], order_id=["1", "1"],
                order_date=["2020-01-01", "2020-01-01"],
                quantity=["2", "2"], unit_price=["1", "1"], product_id=["p", "p"])
    report = dq.validate(df, mapping={})
    assert report["summary"]["valid_rows"] == 2  # kept, not dropped


# --- end-to-end via the Flask API ----------------------------------------
def _upload(client, path):
    with open(path, "rb") as fh:
        return client.post("/api/uploads", data={"file": (fh, os.path.basename(path))},
                           content_type="multipart/form-data")


def test_full_pipeline_upload_map_validate(client, sample_csv_path):
    # 1. upload
    r = _upload(client, sample_csv_path)
    assert r.status_code == 201, r.get_json()
    uid = r.get_json()["data"]["upload_id"]

    # 2. confirm mapping
    mapping = {
        "Order_Ref": "order_id", "StockCode": "product_id",
        "Description": "product_description", "Units": "quantity",
        "Unit_Cost": "unit_price", "Client": "customer_id",
        "Country": "country", "Purchase_Date": "order_date",
    }
    r2 = client.post(f"/api/uploads/{uid}/mapping", json={"mapping": mapping})
    assert r2.status_code == 200, r2.get_json()
    assert r2.get_json()["data"]["status"] == "mapped"

    # 3. validate -> should pass with excluded bad rows
    r3 = client.post(f"/api/uploads/{uid}/validate", json={})
    body = r3.get_json()["data"]
    report = body["report"]
    assert report["passed"] is True
    assert report["summary"]["total_rows"] == 20
    # 3 bad rows: cancelled/return (C489451), missing customer (489453),
    # non-numeric qty (489454)
    assert report["summary"]["valid_rows"] == 17
    assert report["summary"]["invalid_rows"] == 3
    assert body["status"] == "ready_for_analysis"


def test_mapping_missing_required_blocked(client, sample_csv_path):
    r = _upload(client, sample_csv_path)
    uid = r.get_json()["data"]["upload_id"]
    # omit unit_price mapping -> required field missing
    bad = {"Order_Ref": "order_id", "Units": "quantity", "Client": "customer_id",
           "Purchase_Date": "order_date"}
    r2 = client.post(f"/api/uploads/{uid}/mapping", json={"mapping": bad})
    assert r2.status_code == 400
    assert "unit_price" in r2.get_json()["details"]["missing_required"] or \
        any("Unit Price" in e for e in r2.get_json()["details"]["errors"])


def test_validation_before_mapping_blocked(client, sample_csv_path):
    r = _upload(client, sample_csv_path)
    uid = r.get_json()["data"]["upload_id"]
    r2 = client.post(f"/api/uploads/{uid}/validate", json={})
    assert r2.status_code == 409


def test_report_persisted_and_retrievable(client, sample_csv_path):
    r = _upload(client, sample_csv_path)
    uid = r.get_json()["data"]["upload_id"]
    mapping = {"Order_Ref": "order_id", "Units": "quantity", "Unit_Cost": "unit_price",
               "Client": "customer_id", "Purchase_Date": "order_date"}
    client.post(f"/api/uploads/{uid}/mapping", json={"mapping": mapping})
    client.post(f"/api/uploads/{uid}/validate", json={})
    r2 = client.get(f"/api/uploads/{uid}/validate")
    assert r2.status_code == 200
    assert r2.get_json()["data"]["report"]["passed"] is True


def test_history_lists_datasets(client, sample_csv_path):
    _upload(client, sample_csv_path)
    r = client.get("/api/uploads")
    assert r.status_code == 200
    uploads = r.get_json()["data"]["uploads"]
    assert len(uploads) >= 1
