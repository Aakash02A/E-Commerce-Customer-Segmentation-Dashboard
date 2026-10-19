"""Phase 9 dashboard tests: metrics, segments, RFM distributions, isolation,
incomplete stages, empty/small datasets, errors, and API contract regression.

All analytics-side tests use the shared Spark session (the ``spark`` fixture) and
derive expectations from the aggregate/RFM/segment responses themselves -- never
hardcoded fake numbers.
"""
import math

from app.repositories import analysis_repository
from conftest import prepare_ready_upload

# 5-column dataset in the SAMPLE_STANDARD_MAPPING shape (Client/Order_Ref/...).
STD_HEADER = "Client,Order_Ref,Purchase_Date,Units,Unit_Cost\n"


def _write(tmp_path, body, name="data.csv"):
    p = tmp_path / name
    p.write_text(STD_HEADER + body, encoding="utf-8")
    return str(p)


def _aggregate(client, uid):
    return client.post(f"/api/uploads/{uid}/aggregate", json={}).get_json()["data"]


def _run_pipeline(client, csv_path, mapping=None):
    """upload -> map -> validate -> aggregate -> rfm -> segment; return analysis id."""
    uid = prepare_ready_upload(client, csv_path, mapping)
    agg = _aggregate(client, uid)
    aid = agg["analysis_id"]
    client.post(f"/api/analyses/{aid}/rfm", json={})
    client.post(f"/api/analyses/{aid}/segment", json={})
    return aid


def _dashboard(client, aid):
    resp = client.get(f"/api/analyses/{aid}/dashboard")
    assert resp.status_code == 200
    return resp.get_json()["data"]


# --- List endpoint (no fake data) ---------------------------------------
def test_no_analyses_returns_empty_list(client):
    data = client.get("/api/analyses").get_json()["data"]
    assert data["analyses"] == []


# --- Overview correctness -----------------------------------------------
def test_overview_matches_aggregate(client, sample_csv_path, spark):
    uid = prepare_ready_upload(client, sample_csv_path)
    agg = _aggregate(client, uid)
    aid = agg["analysis_id"]
    tx = agg["transactions"]

    d = _dashboard(client, aid)
    ov = d["overview"]
    assert ov["available"] is True
    assert ov["total_customers"] == tx["distinct_customers"]
    assert ov["total_orders"] == tx["distinct_orders"]
    assert ov["total_revenue"] == tx["total_spend"]
    assert ov["total_quantity"] == tx["total_quantity"]
    assert ov["date_range"] == {"first": tx["first_date"], "last": tx["last_date"]}
    # Average order value is exactly revenue / distinct orders.
    assert ov["average_order_value"] == round(tx["total_spend"] / tx["distinct_orders"], 2)


def test_overview_revenue_equals_customer_metric_sum(client, sample_csv_path, spark):
    aid = _run_pipeline(client, sample_csv_path)
    d = _dashboard(client, aid)
    with client.application.app_context():
        metric_sum = sum(m["monetary"] for m in analysis_repository.get_metrics(aid))
    assert abs(d["overview"]["total_revenue"] - metric_sum) <= 0.10


# --- Segment counts and percentages -------------------------------------
def test_segment_counts_and_percentages(client, sample_csv_path, spark):
    aid = _run_pipeline(client, sample_csv_path)
    sb = _dashboard(client, aid)["segment_breakdown"]
    assert sb["available"] is True

    with client.application.app_context():
        flags = analysis_repository.stage_flags(aid)
    assert sb["segmented_customers"] == flags["segmented"]

    total = sb["segmented_customers"]
    counts = [s["count"] for s in sb["segments"]]
    assert sum(counts) == total
    # Every percentage is count/total, and the parts sum to ~100% (rounding).
    for s in sb["segments"]:
        assert s["percentage"] == round(s["count"] * 100 / total, 1)
        assert s["description"]  # documented, non-empty
    assert abs(sum(s["percentage"] for s in sb["segments"]) - 100.0) <= 1.0
    # Every returned segment is one of the canonical rule labels.
    from app.services import segmentation_service
    for s in sb["segments"]:
        assert s["segment"] in segmentation_service.SEGMENTS


# --- RFM distributions ---------------------------------------------------
def test_rfm_distributions_cover_all_scored_customers(client, sample_csv_path, spark):
    aid = _run_pipeline(client, sample_csv_path)
    rfm = _dashboard(client, aid)["rfm"]
    assert rfm["scores_available"] is True
    with client.application.app_context():
        scored = analysis_repository.stage_flags(aid)["rfm_scored"]

    for metric in ("recency", "frequency", "monetary"):
        buckets = rfm["distributions"][metric]
        assert all(1 <= b["score"] <= 5 for b in buckets)
        assert sum(b["count"] for b in buckets) == scored

    # Monetary/frequency summaries are available; avg within [min, max].
    for key in ("frequency", "monetary", "total_quantity"):
        s = rfm["summaries"][key]
        assert s["available"] is True
        assert s["min"] <= s["avg"] <= s["max"]


# --- Incomplete stages: aggregate only ----------------------------------
def test_dashboard_without_rfm_or_segments(client, sample_csv_path, spark):
    uid = prepare_ready_upload(client, sample_csv_path)
    aid = _aggregate(client, uid)["analysis_id"]

    d = _dashboard(client, aid)
    assert d["stages"]["aggregated"] is True
    assert d["stages"]["rfm_scored"] is False
    assert d["stages"]["segmented"] is False
    assert d["overview"]["available"] is True          # base metrics present
    assert d["segment_breakdown"]["available"] is False  # not available (not zero)
    assert d["rfm"]["scores_available"] is False
    assert d["rfm"]["distributions"] == {}
    # recency_days is only written by the RFM step, so it is not available yet.
    assert d["rfm"]["summaries"]["recency_days"]["available"] is False
    assert d["rfm"]["summaries"]["monetary"]["available"] is True


# --- Customers: pagination / sort / search / filter ----------------------
def test_customers_pagination_and_sort(client, sample_csv_path, spark):
    aid = _run_pipeline(client, sample_csv_path)
    d = _dashboard(client, aid)
    total = d["stages"]["total_customers"]

    resp = client.get(f"/api/analyses/{aid}/customers?page_size=2&sort=monetary&direction=desc")
    data = resp.get_json()["data"]
    assert data["total"] == total
    assert len(data["items"]) <= 2
    assert data["pages"] == math.ceil(total / 2)
    money = [it["monetary"] for it in data["items"]]
    assert money == sorted(money, reverse=True)


def test_customers_search_hit_and_miss(client, sample_csv_path, spark):
    aid = _run_pipeline(client, sample_csv_path)
    with client.application.app_context():
        a_customer = analysis_repository.get_metrics(aid)[0]["customer_id"]

    hit = client.get(f"/api/analyses/{aid}/customers?search={a_customer}").get_json()["data"]
    assert hit["total"] == 1
    assert hit["items"][0]["customer_id"] == a_customer

    miss = client.get(f"/api/analyses/{aid}/customers?search=___no_such___").get_json()["data"]
    assert miss["total"] == 0 and miss["items"] == []


def test_customers_segment_filter_is_honest(client, sample_csv_path, spark):
    aid = _run_pipeline(client, sample_csv_path)
    segs = _dashboard(client, aid)["segment_breakdown"]["segments"]
    target = segs[0]["segment"]
    data = client.get(f"/api/analyses/{aid}/customers?segment={target}").get_json()["data"]
    assert data["total"] == segs[0]["count"]
    assert all(it["segment"] == target for it in data["items"])

    # A filter that matches nothing yields a clean empty page, not an error.
    empty = client.get(f"/api/analyses/{aid}/customers?segment=NoSuchSegment").get_json()["data"]
    assert empty["total"] == 0 and empty["items"] == []


# --- Analysis isolation ---------------------------------------------------
def test_dashboard_is_scoped_to_one_analysis(client, tmp_path, spark):
    ds1 = _write(tmp_path, "A1,I1,2024-01-01,2,10.0\nA1,I2,2024-02-01,1,5.0\n", "ds1.csv")
    ds2 = _write(tmp_path, "B7,I9,2024-03-03,3,7.0\n", "ds2.csv")
    a1 = _run_pipeline(client, ds1)
    a2 = _run_pipeline(client, ds2)

    ids1 = {it["customer_id"] for it in
            client.get(f"/api/analyses/{a1}/customers?page_size=100").get_json()["data"]["items"]}
    ids2 = {it["customer_id"] for it in
            client.get(f"/api/analyses/{a2}/customers?page_size=100").get_json()["data"]["items"]}
    assert ids1 == {"A1"} and ids2 == {"B7"}
    assert _dashboard(client, a1)["overview"]["total_customers"] == 1
    assert _dashboard(client, a2)["overview"]["total_customers"] == 1


def test_single_customer_cohort(client, tmp_path, spark):
    ds = _write(tmp_path, "Solo,I1,2024-01-01,4,9.0\n", "solo.csv")
    aid = _run_pipeline(client, ds)
    d = _dashboard(client, aid)
    assert d["overview"]["total_customers"] == 1
    # Neutral middle bin for a cohort of one.
    cust = d["rfm"]  # noqa: F841 (presence check)
    one = client.get(f"/api/analyses/{aid}/customers").get_json()["data"]["items"][0]
    assert one["r_score"] == 3 and one["f_score"] == 3 and one["m_score"] == 3
    assert d["segment_breakdown"]["segments"][0]["percentage"] == 100.0


# --- Error handling -------------------------------------------------------
def test_dashboard_unknown_analysis_404(client):
    assert client.get("/api/analyses/doesnotexist/dashboard").status_code == 404
    assert client.get("/api/analyses/doesnotexist/customers").status_code == 404


def test_customers_bad_pagination_400(client, sample_csv_path, spark):
    aid = _run_pipeline(client, sample_csv_path)
    assert client.get(f"/api/analyses/{aid}/customers?page=abc").status_code == 400


def test_dashboard_no_leakage_of_paths_or_traces(client, sample_csv_path, spark):
    aid = _run_pipeline(client, sample_csv_path)
    body = client.get(f"/api/analyses/{aid}/dashboard").get_data(as_text=True)
    assert "Traceback" not in body and "\\" not in body


# --- API contract regression ---------------------------------------------
def test_metrics_endpoint_still_works(client, sample_csv_path, spark):
    aid = _run_pipeline(client, sample_csv_path)
    data = client.get(f"/api/analyses/{aid}/metrics").get_json()["data"]
    assert data["analysis_id"] == aid
    assert len(data["metrics"]) > 0
    assert data["reference_date"]


def test_list_analyses_reflects_created_analyses(client, tmp_path, spark):
    a1 = _run_pipeline(client, _write(tmp_path, "A1,I1,2024-01-01,1,10.0\n", "d1.csv"))
    items = client.get("/api/analyses").get_json()["data"]["analyses"]
    assert any(a["analysis_id"] == a1 and a["status"] == "completed" for a in items)
