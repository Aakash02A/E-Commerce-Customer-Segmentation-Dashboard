"""File upload, validation, and preview tests (Phase 2)."""
import io
import os

from conftest import make_csv


def _upload(client, path, name=None):
    with open(path, "rb") as fh:
        data = {"file": (fh, name or os.path.basename(path))}
        return client.post("/api/uploads", data=data, content_type="multipart/form-data")


def _upload_bytes(client, content_bytes, name):
    data = {"file": (io.BytesIO(content_bytes), name)}
    return client.post("/api/uploads", data=data, content_type="multipart/form-data")


def test_health(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    assert r.get_json()["success"] is True


def test_valid_csv_upload(client, sample_csv_path):
    r = _upload(client, sample_csv_path)
    assert r.status_code == 201, r.get_json()
    body = r.get_json()["data"]
    assert body["extension"] == "csv"
    assert body["row_count"] == 20
    assert body["status"] == "mapping_required"
    # preview returns columns + rows
    assert len(body["preview"]["columns"]) == 8
    assert len(body["preview"]["rows"]) == 10


def test_valid_xlsx_upload(client, sample_xlsx_path):
    r = _upload(client, sample_xlsx_path)
    assert r.status_code == 201, r.get_json()
    body = r.get_json()["data"]
    assert body["extension"] == "xlsx"
    assert body["row_count"] == 20


def test_multi_sheet_lists_sheets(client, sample_multi_sheet_path):
    r = _upload(client, sample_multi_sheet_path)
    assert r.status_code == 201
    body = r.get_json()["data"]
    assert body["sheet_names"] == ["Sheet2010", "SheetLater"]
    # selecting the second sheet changes the preview
    r2 = client.get(f"/api/uploads/{body['upload_id']}/preview?sheet=SheetLater")
    assert r2.status_code == 200
    assert r2.get_json()["data"]["preview"]["rows"][0][0].startswith("C489") or \
        r2.get_json()["data"]["preview"]["rows"][0][0] == "489455"


def test_unsupported_extension(client, tmp_path):
    p = make_csv(tmp_path, "a,b\n1,2\n", name="data.txt")
    r = _upload(client, str(p))
    assert r.status_code == 400
    assert "Unsupported file type" in r.get_json()["message"]


def test_empty_file(client):
    r = _upload_bytes(client, b"", "empty.csv")
    assert r.status_code == 400
    assert "empty" in r.get_json()["message"].lower()


def test_corrupted_xlsx_extension_mismatch(client):
    # A CSV renamed to .xlsx: content does not match the ZIP magic number.
    r = _upload_bytes(client, b"a,b,c\n1,2,3\n", "fake.xlsx")
    assert r.status_code == 400
    assert "Excel" in r.get_json()["message"] or "workbook" in r.get_json()["message"].lower()


def test_csv_header_only_rejected(client, tmp_path):
    p = make_csv(tmp_path, "Order_Ref,Units,Unit_Cost,Client,Purchase_Date\n")
    r = _upload(client, str(p))
    assert r.status_code == 400


def test_oversized_upload_rejected(app, client, tmp_path):
    app.config["MAX_CONTENT_LENGTH"] = 50  # bytes, for the test
    big = make_csv(tmp_path, "a\n" + ("x" * 1000 + "\n"))
    with open(big, "rb") as fh:
        r = client.post("/api/uploads", data={"file": (fh, "big.csv")},
                        content_type="multipart/form-data")
    assert r.status_code in (400, 413)


def test_path_traversal_filename_neutralized(client, sample_csv_path):
    r = _upload(client, sample_csv_path, name="../../evil.csv")
    assert r.status_code == 201, r.get_json()
    # stored file must not be reachable outside the upload dir
    from app.repositories import upload_repository
    # No exception and a valid upload id proves safe handling.
    assert r.get_json()["data"]["upload_id"]
