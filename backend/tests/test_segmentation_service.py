"""Phase 8 segmentation tests: rule boundaries, K-Means, persistence, isolation."""
from itertools import product

from app.repositories import analysis_repository
from app.services import segmentation_service as seg
from conftest import build_app, prepare_ready_upload

CUSTOM_MAPPING = {"Customer": "customer_id", "Invoice": "order_id",
                  "Date": "order_date", "Qty": "quantity", "Price": "unit_price"}

ALL_LABELS = set(seg.SEGMENTS)


def _aggregate(client, uid):
    return client.post(f"/api/uploads/{uid}/aggregate", json={})


def _rfm(client, aid):
    return client.post(f"/api/analyses/{aid}/rfm", json={})


def _segment(client, aid):
    return client.post(f"/api/analyses/{aid}/segment", json={})


# --- Rule boundaries (explicit expected label per condition) --------------
def test_every_documented_segment_boundary():
    cases = [
        # Champions: Active, high F and M
        ((4, 4, 4), seg.CHAMPIONS), ((5, 5, 5), seg.CHAMPIONS),
        # Loyal: Active with F>=3 (but not champ), or Warm with F&M high
        ((4, 3, 1), seg.LOYAL), ((4, 4, 3), seg.LOYAL), ((3, 4, 4), seg.LOYAL),
        # Potential Loyalists: Active M>=4 low F, or Warm mid
        ((4, 2, 4), seg.POTENTIAL_LOYALISTS), ((3, 2, 3), seg.POTENTIAL_LOYALISTS),
        # Recent Customers: Active, low F and M
        ((4, 2, 2), seg.RECENT), ((5, 1, 1), seg.RECENT),
        # Cannot Lose Them: Cold, high F and M
        ((2, 4, 4), seg.CANNOT_LOSE_THEM), ((1, 5, 5), seg.CANNOT_LOSE_THEM),
        # At Risk: Cold, F>=3 or M>=3
        ((2, 3, 1), seg.AT_RISK), ((2, 1, 4), seg.AT_RISK),
        # Hibernating: Cold, low F and M
        ((2, 2, 2), seg.HIBERNATING), ((1, 1, 1), seg.HIBERNATING),
        # Others: Warm with low F and M
        ((3, 2, 2), seg.OTHERS),
    ]
    for (r, f, m), expected in cases:
        assert seg.assign_segment(r, f, m) == expected, (r, f, m)


def test_missing_scores_fall_back_to_others():
    assert seg.assign_segment(None, 3, 3) == seg.OTHERS
    assert seg.assign_segment(3, None, 3) == seg.OTHERS
    assert seg.assign_segment(3, 3, None) == seg.OTHERS
    assert seg.assign_segment(None, None, None) == seg.OTHERS


def test_full_grid_exactly_one_label_and_all_reachable():
    seen = set()
    for r, f, m in product(range(1, 6), range(1, 6), range(1, 6)):
        label = seg.assign_segment(r, f, m)
        assert isinstance(label, str) and label in ALL_LABELS  # exactly one, valid
        seen.add(label)
    assert seen == ALL_LABELS  # every category is reachable, none missing


def test_assign_segments_returns_one_per_customer():
    rows = [{"customer_id": f"C{i}", "r_score": r, "f_score": f, "m_score": m}
            for i, (r, f, m) in enumerate(product(range(1, 6), repeat=3))]
    assignments = seg.assign_segments(rows)
    assert len(assignments) == len(rows)
    assert {a["customer_id"] for a in assignments} == {r["customer_id"] for r in rows}
    assert all(a["segment"] in ALL_LABELS for a in assignments)


def test_segment_counts_totals_match_population():
    rows = [{"customer_id": str(i), "r_score": 4, "f_score": 4, "m_score": 4} for i in range(7)]
    counts = seg.segment_counts(seg.assign_segments(rows))
    assert sum(counts.values()) == 7
    assert counts.get(seg.CHAMPIONS) == 7


# --- K-Means (optional, separate, self-evaluating) ------------------------
def _feature_rows(n, base=0.0, spread=1.0):
    rows = []
    for i in range(n):
        rows.append({"customer_id": f"C{i}", "frequency": base + (i % 5) * spread,
                     "monetary": base + (i % 3) * spread + i * 0.5,
                     "recency_days": (i * 3) % 30 + 1})
    return rows


def test_kmeans_skips_small_cohort():
    result = seg.run_kmeans(_feature_rows(6), min_customers=10)
    assert result["ran"] is False
    assert result["reason"] == "insufficient_customers"


def test_kmeans_skips_identical_vectors():
    rows = [{"customer_id": f"C{i}", "frequency": 2, "monetary": 10, "recency_days": 5}
            for i in range(15)]
    result = seg.run_kmeans(rows, min_customers=10)
    assert result["ran"] is False
    assert result["reason"] == "insufficient_distinct_feature_vectors"


def test_kmeans_is_reproducible():
    rows = _feature_rows(20)
    a = seg.run_kmeans(rows, min_customers=10, seed=7)
    b = seg.run_kmeans(rows, min_customers=10, seed=7)
    assert a["ran"] and b["ran"]
    assert a["cluster_by_customer"] == b["cluster_by_customer"]
    assert a["k"] == b["k"]


def test_kmeans_excludes_rows_with_missing_features():
    rows = _feature_rows(12)
    rows[0]["monetary"] = None  # dropped from clustering
    result = seg.run_kmeans(rows, min_customers=10)
    if result["ran"]:
        assert "C0" not in result["cluster_by_customer"]


def test_kmeans_selects_k_by_evaluation_not_arbitrary():
    result = seg.run_kmeans(_feature_rows(25), min_customers=10, k_max=5)
    assert result["ran"] is True
    assert result["k"] in result["inertia_curve"]
    assert 2 <= result["k"] <= 5
    assert len(result["inertia_curve"]) >= 1


# --- Integration: full pipeline + persistence + isolation -----------------
def _ready_aggregated_analysis(client, sample_csv_path):
    uid = prepare_ready_upload(client, sample_csv_path)
    aid = _aggregate(client, uid).get_json()["data"]["analysis_id"]
    _rfm(client, aid)
    return aid


def test_segment_requires_rfm_first(client, sample_csv_path, spark):
    uid = prepare_ready_upload(client, sample_csv_path)
    aid = _aggregate(client, uid).get_json()["data"]["analysis_id"]
    # No RFM run yet -> segmentation must refuse (409), not guess.
    resp = _segment(client, aid)
    assert resp.status_code == 409


def test_segmentation_persists_one_segment_each(client, sample_csv_path, spark):
    aid = _ready_aggregated_analysis(client, sample_csv_path)
    resp = _segment(client, aid)
    assert resp.status_code == 200
    data = resp.get_json()["data"]
    assert data["customers_segmented"] > 0

    with client.application.app_context():
        metrics = analysis_repository.get_metrics(aid)
        assert len(metrics) == data["customers_segmented"]
        for m in metrics:
            assert m["segment"] in ALL_LABELS          # exactly one, valid
            # RFM scores must have survived the segmentation write.
            assert m["r_score"] in range(1, 6)
        assert sum(data["segment_counts"].values()) == len(metrics)


def test_segmentation_rerun_no_duplicates_or_stale(client, sample_csv_path, spark):
    aid = _ready_aggregated_analysis(client, sample_csv_path)
    first = _segment(client, aid).get_json()["data"]
    second = _segment(client, aid).get_json()["data"]
    assert first["segment_counts"] == second["segment_counts"]
    with client.application.app_context():
        assert analysis_repository.metrics_count(aid) == second["customers_segmented"]
        # still exactly one segment per row after rerun
        for m in analysis_repository.get_metrics(aid):
            assert m["segment"] in ALL_LABELS


def test_segmentation_cross_analysis_isolation(client, tmp_path, spark):
    ds1 = tmp_path / "ds1.csv"
    ds1.write_text("Customer,Invoice,Date,Qty,Price\n"
                   "A1,I1,2024-01-01,1,10.0\nA1,I2,2024-06-06,3,30.0\n", encoding="utf-8")
    ds2 = tmp_path / "ds2.csv"
    ds2.write_text("Customer,Invoice,Date,Qty,Price\n"
                   "B7,I9,2024-03-03,5,50.0\n", encoding="utf-8")

    uid1 = prepare_ready_upload(client, str(ds1), CUSTOM_MAPPING)
    uid2 = prepare_ready_upload(client, str(ds2), CUSTOM_MAPPING)
    aid1 = _aggregate(client, uid1).get_json()["data"]["analysis_id"]
    aid2 = _aggregate(client, uid2).get_json()["data"]["analysis_id"]
    _rfm(client, aid1)
    _rfm(client, aid2)

    _segment(client, aid1)  # segment only analysis 1
    with client.application.app_context():
        assert all(m["segment"] in ALL_LABELS for m in analysis_repository.get_metrics(aid1))
        # analysis 2 must be completely untouched
        for m in analysis_repository.get_metrics(aid2):
            assert m["segment"] is None
            assert m["cluster_label"] is None


def test_segmentation_unknown_analysis_404(client):
    assert _segment(client, "does-not-exist").status_code == 404


def test_segmentation_preserves_rules_even_when_kmeans_enabled(client, sample_csv_path, spark):
    aid = _ready_aggregated_analysis(client, sample_csv_path)
    client.application.config["SEGMENTATION_ENABLE_KMEANS"] = True
    try:
        data = _segment(client, aid).get_json()["data"]
        # Rule-based segments are always present, regardless of clustering outcome.
        assert data["customers_segmented"] > 0
        assert "ran" in data["clustering"] and "reason" in data["clustering"]
        with client.application.app_context():
            for m in analysis_repository.get_metrics(aid):
                assert m["segment"] in ALL_LABELS
                # cluster_label is stored in a TEXT column; accept None or an
                # integer-valued cluster id regardless of storage type.
                assert m["cluster_label"] is None or str(m["cluster_label"]).lstrip("-").isdigit()
    finally:
        client.application.config["SEGMENTATION_ENABLE_KMEANS"] = False


def test_segmentation_survives_restart(tmp_path, sample_csv_path):
    root = tmp_path / "seg_restart"
    app1 = build_app(root)
    c1 = app1.test_client()
    uid = prepare_ready_upload(c1, sample_csv_path)
    aid = _aggregate(c1, uid).get_json()["data"]["analysis_id"]
    _rfm(c1, aid)
    _segment(c1, aid)

    app2 = build_app(root)
    c2 = app2.test_client()
    body = c2.get(f"/api/analyses/{aid}/metrics").get_json()["data"]
    assert body["metrics"]
    for row in body["metrics"]:
        assert row["segment"] in ALL_LABELS
