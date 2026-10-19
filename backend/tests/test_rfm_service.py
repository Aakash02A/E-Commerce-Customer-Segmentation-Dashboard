"""Phase 7 RFM tests: deterministic scoring, edge cases, persistence, isolation."""
from app.repositories import analysis_repository
from app.services import rfm_service
from conftest import prepare_ready_upload

CUSTOM_MAPPING = {"Customer": "customer_id", "Invoice": "order_id",
                  "Date": "order_date", "Qty": "quantity", "Price": "unit_price"}


def _aggregate(client, uid):
    return client.post(f"/api/uploads/{uid}/aggregate", json={})


def _by_id(updates):
    return {u["customer_id"]: u for u in updates}


# --- Scoring methodology (pure unit tests) -------------------------------
def test_recency_direction_is_inverted():
    # A purchased recently -> higher R score; B long ago -> lower R score.
    aggs = [
        {"customer_id": "A", "frequency": 1, "monetary": 10, "last_order_date": "2024-12-30"},
        {"customer_id": "B", "frequency": 1, "monetary": 10, "last_order_date": "2024-01-01"},
    ]
    r = _by_id(rfm_service.calculate_rfm("2024-12-31", aggs))
    assert r["A"]["recency_days"] < r["B"]["recency_days"]
    assert r["A"]["r_score"] > r["B"]["r_score"]


def test_frequency_and_monetary_higher_is_better():
    aggs = [
        {"customer_id": "L", "frequency": 1, "monetary": 5, "last_order_date": "2024-01-01"},
        {"customer_id": "H", "frequency": 10, "monetary": 500, "last_order_date": "2024-01-01"},
    ]
    r = _by_id(rfm_service.calculate_rfm("2024-02-01", aggs))
    assert r["H"]["f_score"] > r["L"]["f_score"]
    assert r["H"]["m_score"] > r["L"]["m_score"]
    # Identical recency -> tie on R score.
    assert r["H"]["r_score"] == r["L"]["r_score"]


def test_ties_receive_identical_scores():
    aggs = [{"customer_id": f"C{i}", "frequency": 3, "monetary": 30,
             "last_order_date": "2024-06-01"} for i in range(5)]
    scores = {(u["r_score"], u["f_score"], u["m_score"])
              for u in rfm_service.calculate_rfm("2024-06-05", aggs)}
    assert len(scores) == 1


def test_scoring_is_order_independent():
    base = [
        {"customer_id": "A", "frequency": 1, "monetary": 10, "last_order_date": "2024-01-01"},
        {"customer_id": "B", "frequency": 4, "monetary": 90, "last_order_date": "2024-05-05"},
        {"customer_id": "C", "frequency": 2, "monetary": 40, "last_order_date": "2024-07-20"},
        {"customer_id": "D", "frequency": 7, "monetary": 210, "last_order_date": "2024-03-11"},
        {"customer_id": "E", "frequency": 3, "monetary": 15, "last_order_date": "2024-12-01"},
    ]
    forward = _by_id(rfm_service.calculate_rfm("2024-12-31", base))
    reverse = _by_id(rfm_service.calculate_rfm("2024-12-31", list(reversed(base))))
    for cid in forward:
        assert (forward[cid]["r_score"], forward[cid]["f_score"], forward[cid]["m_score"]) \
            == (reverse[cid]["r_score"], reverse[cid]["f_score"], reverse[cid]["m_score"])


def test_single_customer_gets_neutral_middle_bin():
    r = rfm_service.calculate_rfm("2024-12-31", [
        {"customer_id": "A", "frequency": 2, "monetary": 20, "last_order_date": "2024-12-01"},
    ])[0]
    assert (r["r_score"], r["f_score"], r["m_score"]) == (3, 3, 3)
    assert r["recency_days"] == 30


def test_none_and_zero_values_are_safe():
    aggs = [
        {"customer_id": "MISS", "frequency": None, "monetary": None, "last_order_date": None},
        {"customer_id": "ZERO", "frequency": 0, "monetary": 0.0, "last_order_date": "2024-01-01"},
        {"customer_id": "OK", "frequency": 5, "monetary": 100, "last_order_date": "2024-05-01"},
    ]
    r = _by_id(rfm_service.calculate_rfm("2024-06-01", aggs))
    assert r["MISS"]["recency_days"] is None
    assert r["MISS"]["r_score"] is None and r["MISS"]["f_score"] is None
    # Valid customers still get scores within the 1..5 range.
    for cid in ("ZERO", "OK"):
        for k in ("r_score", "f_score", "m_score"):
            assert r[cid][k] in range(1, 6)
    assert r["OK"]["m_score"] >= r["ZERO"]["m_score"]


def test_reference_date_falls_back_to_max_purchase():
    aggs = [
        {"customer_id": "OLD", "frequency": 1, "monetary": 1, "last_order_date": "2024-01-01"},
        {"customer_id": "NEW", "frequency": 1, "monetary": 1, "last_order_date": "2024-03-01"},
    ]
    r = _by_id(rfm_service.calculate_rfm(None, aggs))
    assert r["NEW"]["recency_days"] == 0
    assert r["OLD"]["recency_days"] > 0


def test_negative_recency_is_clamped_to_zero():
    r = rfm_service.calculate_rfm("2024-01-01", [
        {"customer_id": "FUTURE", "frequency": 1, "monetary": 1, "last_order_date": "2024-06-01"},
    ])[0]
    assert r["recency_days"] == 0


def test_empty_cohort_returns_no_updates():
    assert rfm_service.calculate_rfm("2024-01-01", []) == []


# --- Integration: persist against a real aggregated analysis --------------
def test_rfm_scores_persisted_for_completed_analysis(client, sample_csv_path, spark):
    uid = prepare_ready_upload(client, sample_csv_path)
    agg = _aggregate(client, uid).get_json()["data"]
    aid = agg["analysis_id"]

    resp = client.post(f"/api/analyses/{aid}/rfm", json={})
    assert resp.status_code == 200
    data = resp.get_json()["data"]
    assert data["customers_scored"] == agg["customers_analyzed"]

    with client.application.app_context():
        metrics = analysis_repository.get_metrics(aid)
        assert len(metrics) == data["customers_scored"]
        for m in metrics:
            assert m["recency_days"] is not None and m["recency_days"] >= 0
            for col in ("r_score", "f_score", "m_score"):
                assert m[col] in range(1, 6)
            # Phase 8 territory: segmentation must remain untouched here.
            assert m["segment"] is None
            assert m["cluster_label"] is None


def test_rfm_metrics_endpoint(client, sample_csv_path, spark):
    uid = prepare_ready_upload(client, sample_csv_path)
    aid = _aggregate(client, uid).get_json()["data"]["analysis_id"]
    client.post(f"/api/analyses/{aid}/rfm", json={})
    body = client.get(f"/api/analyses/{aid}/metrics").get_json()["data"]
    assert body["reference_date"]
    assert len(body["metrics"]) > 0
    assert all(row["r_score"] in range(1, 6) for row in body["metrics"])


def test_rfm_rejects_unknown_analysis(client):
    assert client.post("/api/analyses/does-not-exist/rfm", json={}).status_code == 404


def test_rfm_requires_completed_analysis(client, sample_csv_path, spark):
    uid = prepare_ready_upload(client, sample_csv_path)
    with client.application.app_context():
        aid = analysis_repository.create_analysis(uid, settings={})  # status: pending
    resp = client.post(f"/api/analyses/{aid}/rfm", json={})
    assert resp.status_code == 409


def test_rfm_does_not_cross_contaminate_analyses(client, tmp_path, spark):
    ds1 = tmp_path / "ds1.csv"
    ds1.write_text("Customer,Invoice,Date,Qty,Price\n"
                   "A1,I1,2024-01-01,1,10.0\nA1,I2,2024-02-02,2,20.0\n", encoding="utf-8")
    ds2 = tmp_path / "ds2.csv"
    ds2.write_text("Customer,Invoice,Date,Qty,Price\n"
                   "B7,I9,2024-03-03,5,50.0\n", encoding="utf-8")

    uid1 = prepare_ready_upload(client, str(ds1), CUSTOM_MAPPING)
    uid2 = prepare_ready_upload(client, str(ds2), CUSTOM_MAPPING)
    aid1 = _aggregate(client, uid1).get_json()["data"]["analysis_id"]
    aid2 = _aggregate(client, uid2).get_json()["data"]["analysis_id"]

    # Score ONLY analysis 1.
    assert client.post(f"/api/analyses/{aid1}/rfm", json={}).status_code == 200

    with client.application.app_context():
        scored = analysis_repository.get_metrics(aid1)
        assert all(m["r_score"] in range(1, 6) for m in scored)
        # Analysis 2 must be completely untouched (no RFM leak across analyses).
        for m in analysis_repository.get_metrics(aid2):
            assert m["r_score"] is None
            assert m["f_score"] is None
            assert m["m_score"] is None
            assert m["recency_days"] is None


def test_rfm_scores_survive_restart(tmp_path, sample_csv_path):
    from conftest import build_app
    root = tmp_path / "rfm_restart"
    app1 = build_app(root)
    c1 = app1.test_client()
    uid = prepare_ready_upload(c1, sample_csv_path)
    aid = _aggregate(c1, uid).get_json()["data"]["analysis_id"]
    c1.post(f"/api/analyses/{aid}/rfm", json={})

    app2 = build_app(root)
    c2 = app2.test_client()
    body = c2.get(f"/api/analyses/{aid}/metrics").get_json()["data"]
    assert all(row["r_score"] in range(1, 6) for row in body["metrics"])
