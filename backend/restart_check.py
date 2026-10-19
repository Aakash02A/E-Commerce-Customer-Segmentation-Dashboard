"""Two-process persistence check (Phase 5).

Run with `python restart_check.py write` then `python restart_check.py read`
as separate OS processes to prove data survives a real application restart.
"""
import os
import sys

SCRATCH = os.path.join(os.path.dirname(__file__), ".restart_check")
os.environ["DATABASE_PATH"] = os.path.join(SCRATCH, "db.sqlite")
os.environ["UPLOAD_DIR"] = os.path.join(SCRATCH, "uploads")
os.environ["PROCESSED_DIR"] = os.path.join(SCRATCH, "processed")
os.environ["EXPORTS_DIR"] = os.path.join(SCRATCH, "exports")
os.environ["TEMP_DIR"] = os.path.join(SCRATCH, "temp")
for _d in [SCRATCH] + [os.environ[k] for k in
                       ("UPLOAD_DIR", "PROCESSED_DIR", "EXPORTS_DIR", "TEMP_DIR")]:
    os.makedirs(_d, exist_ok=True)

from app import create_app  # noqa: E402

MAPPING = {"Order_Ref": "order_id", "Units": "quantity", "Unit_Cost": "unit_price",
           "Client": "customer_id", "Purchase_Date": "order_date"}
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE = os.path.join(PROJECT_ROOT, "sample_data", "sample_retail.csv")


def write():
    app = create_app("testing")
    c = app.test_client()
    with open(SAMPLE, "rb") as fh:
        r = c.post("/api/uploads", data={"file": (fh, os.path.basename(SAMPLE))},
                   content_type="multipart/form-data")
    uid = r.get_json()["data"]["upload_id"]
    c.post(f"/api/uploads/{uid}/mapping", json={"mapping": MAPPING})
    c.post(f"/api/uploads/{uid}/validate", json={})
    with open(os.path.join(SCRATCH, "id.txt"), "w") as fh:
        fh.write(uid)
    print(f"[write] persisted upload {uid}, status=ready_for_analysis")


def read():
    uid = open(os.path.join(SCRATCH, "id.txt")).read().strip()
    app = create_app("testing")  # brand new process, new connections
    c = app.test_client()
    detail = c.get(f"/api/uploads/{uid}").get_json()["data"]
    report = c.get(f"/api/uploads/{uid}/validate").get_json()["data"]["report"]
    mapping = c.get(f"/api/uploads/{uid}/mapping").get_json()["data"]["confirmed_mapping"]
    print(f"[read]  upload {uid}")
    print(f"[read]  status    = {detail['status']}")
    print(f"[read]  mapping   = {mapping}")
    print(f"[read]  report    = valid={report['summary']['valid_rows']}/"
          f"{report['summary']['total_rows']} passed={report['passed']}")
    assert detail["status"] == "ready_for_analysis"
    assert mapping == MAPPING
    assert report["passed"] is True
    print("[read]  OK: persistence survived a real process restart")


if __name__ == "__main__":
    (write if sys.argv[1] == "write" else read)()
