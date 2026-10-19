"""Customer segmentation service (Phase 8).

Turns persisted Phase 7 RFM scores into an interpretable, business-facing
segment label for every customer, and (optionally) a K-Means cluster id that is
kept *separate* from and *never overrides* the rule-based segment.

Like ``rfm_service``, this module is pure: no Flask, no SQLite, no Spark. It
receives RFM rows and returns label rows; persistence lives in the repositories
and HTTP handling in routes. Dashboard charts, exports and recommendations are
later phases and are NOT implemented here.

Rule-based segmentation (documented, deterministic)
---------------------------------------------------
Inputs are the RFM scores R (Recency), F (Frequency), M (Monetary), each 1..5
where **5 is best** (see ``rfm_service``: lower recency -> higher R). The labels
below are *proposed marketing categories*, not universal truths.

Customers are first split into mutually exclusive **recency tiers**, then a
frequency/monetary ladder picks one label. Because each tier's ladder is an
ordered if/elif chain over a partition, every scored customer receives **exactly
one** segment with no overlap:

  * Active  (R >= 4)
      - F >= 4 and M >= 4  -> Champions
      - F >= 3             -> Loyal Customers
      - M >= 4             -> Potential Loyalists
      - otherwise          -> Recent Customers
  * Warm    (R == 3)
      - F >= 4 and M >= 4  -> Loyal Customers
      - F >= 3 or M >= 3   -> Potential Loyalists
      - otherwise          -> Others
  * Cold    (R <= 2)
      - F >= 4 and M >= 4  -> Cannot Lose Them
      - F >= 3 or M >= 3   -> At Risk
      - otherwise          -> Hibernating

Any customer missing an R, F or M score (None) is unclassified and gets the
``Others`` fallback rather than being dropped or crashing.
"""
from __future__ import annotations

import numpy as np

# Canonical, documented segment labels (proposed business categories).
CHAMPIONS = "Champions"
LOYAL = "Loyal Customers"
RECENT = "Recent Customers"
POTENTIAL_LOYALISTS = "Potential Loyalists"
AT_RISK = "At Risk"
CANNOT_LOSE_THEM = "Cannot Lose Them"
HIBERNATING = "Hibernating"
OTHERS = "Others"

SEGMENTS = (
    CHAMPIONS, LOYAL, RECENT, POTENTIAL_LOYALISTS,
    AT_RISK, CANNOT_LOSE_THEM, HIBERNATING, OTHERS,
)


# --- Rule-based segments -------------------------------------------------
def assign_segment(r_score, f_score, m_score) -> str:
    """Return exactly one interpretable segment for a customer's RFM scores.

    Missing scores fall back to ``Others`` (safe, non-destructive).
    """
    if r_score is None or f_score is None or m_score is None:
        return OTHERS

    if r_score >= 4:                       # Active
        if f_score >= 4 and m_score >= 4:
            return CHAMPIONS
        if f_score >= 3:
            return LOYAL
        if m_score >= 4:
            return POTENTIAL_LOYALISTS
        return RECENT
    if r_score == 3:                        # Warm
        if f_score >= 4 and m_score >= 4:
            return LOYAL
        if f_score >= 3 or m_score >= 3:
            return POTENTIAL_LOYALISTS
        return OTHERS
    # Cold (r_score <= 2)
    if f_score >= 4 and m_score >= 4:
        return CANNOT_LOSE_THEM
    if f_score >= 3 or m_score >= 3:
        return AT_RISK
    return HIBERNATING


def assign_segments(rows: list[dict]) -> list[dict]:
    """Map every RFM row to a segment. Returns [{customer_id, segment}]."""
    return [
        {
            "customer_id": r["customer_id"],
            "segment": assign_segment(r.get("r_score"), r.get("f_score"), r.get("m_score")),
        }
        for r in rows
    ]


def segment_counts(assignments: list[dict]) -> dict:
    counts: dict[str, int] = {}
    for a in assignments:
        counts[a["segment"]] = counts.get(a["segment"], 0) + 1
    return counts


# --- Optional K-Means (dependency-free, numpy) ---------------------------
def _standardize(arr: np.ndarray) -> np.ndarray:
    """z-score each column; zero-variance columns are left at 0 (sd -> 1)."""
    mu = arr.mean(axis=0)
    sd = arr.std(axis=0)
    sd = np.where(sd == 0, 1.0, sd)
    return (arr - mu) / sd


def _kmeans(arr: np.ndarray, k: int, seed: int, max_iter: int = 50):
    """Deterministic Lloyd's algorithm with a seeded random init.

    Returns (labels, centroids, inertia). Initial centroids are k *distinct*
    rows so a duplicated-vector cohort cannot collapse to fewer clusters.
    """
    rng = np.random.default_rng(seed)
    n = arr.shape[0]
    unique_rows = np.unique(arr, axis=0)
    if unique_rows.shape[0] < k:
        return None
    init = rng.choice(unique_rows.shape[0], size=k, replace=False)
    centroids = unique_rows[init].astype(float).copy()
    labels = np.full(n, -1)

    for _ in range(max_iter):
        d = ((arr[:, None, :] - centroids[None, :, :]) ** 2).sum(axis=2)
        new_labels = d.argmin(axis=1)
        if np.array_equal(new_labels, labels):
            break
        labels = new_labels
        for c in range(k):
            members = arr[labels == c]
            if members.size:
                centroids[c] = members.mean(axis=0)

    inertia = float(((arr - centroids[labels]) ** 2).sum())
    return labels, centroids, inertia


def run_kmeans(rows: list[dict], features=("frequency", "monetary", "recency_days"),
               k_max: int = 5, min_customers: int = 10, seed: int = 42) -> dict:
    """Optional, self-evaluating K-Means over RFM-derived numeric features.

    Deliberately conservative and *separate* from rule-based segments:

    * Needs >= ``min_customers`` customers with complete numeric features, and
      at least 3 distinct feature vectors, else it is skipped with a reason.
    * Skips when every feature has zero variance (nothing to separate).
    * Selects ``k`` by the elbow (largest relative drop in inertia) over
      k = 2..min(k_max, distinct) -- never an arbitrary fixed count -- and
      rejects degenerate runs that collapse to fewer than ``k`` clusters.
    * Never modifies or overrides a segment label; its output is only an
      auxiliary ``cluster_by_customer`` map.

    Returns an evaluation dict (ran flag, reason, k, inertia_curve).
    """
    skipped = {"ran": False, "method": "kmeans", "reason": "", "k": None,
               "inertia_curve": {}, "cluster_by_customer": {}}

    feat_rows = []
    for r in rows:
        vec = [r.get(f) for f in features]
        if any(v is None for v in vec):
            continue
        try:
            feat_rows.append((r["customer_id"], [float(v) for v in vec]))
        except (TypeError, ValueError):
            continue

    if len(feat_rows) < min_customers:
        skipped["reason"] = "insufficient_customers"
        skipped["customer_count"] = len(feat_rows)
        return skipped

    ids = [i for i, _ in feat_rows]
    arr = np.array([v for _, v in feat_rows], dtype=float)

    distinct = len({tuple(np.round(row, 9)) for row in arr})
    if distinct < 3:
        skipped["reason"] = "insufficient_distinct_feature_vectors"
        skipped["distinct_vectors"] = distinct
        return skipped

    if np.all(arr.std(axis=0) == 0):
        skipped["reason"] = "zero_variance_features"
        return skipped

    scaled = _standardize(arr)
    max_k = int(min(k_max, distinct, arr.shape[0]))

    inertia_curve: dict[int, float] = {}
    best = {}
    for k in range(2, max_k + 1):
        result = _kmeans(scaled, k, seed=seed)
        if result is None:
            continue
        labels, centroids, inertia = result
        if len(set(labels.tolist())) < k:      # degenerate / collapsed run
            continue
        inertia_curve[k] = round(inertia, 6)
        best[k] = (labels, inertia)

    if not inertia_curve:
        skipped["reason"] = "no_valid_clustering"
        return skipped

    # Elbow: take the largest k whose step gives the peak relative drop in
    # inertia (favors the simpler, still-explanatory clustering).
    ks = sorted(inertia_curve)
    if len(ks) == 1:
        chosen = ks[0]
    else:
        rel_drops = []
        for prev, curr in zip(ks, ks[1:]):
            denom = inertia_curve[prev] or 1.0
            rel_drops.append((denom - inertia_curve[curr]) / denom)
        peak = max(rel_drops)
        chosen = max(ks[i + 1] for i, dr in enumerate(rel_drops)
                     if dr >= peak - 1e-12)

    labels, _ = best[chosen]
    cluster_by_customer = {ids[i]: int(labels[i]) for i in range(len(ids))}

    return {
        "ran": True,
        "method": "kmeans",
        "reason": "",
        "k": chosen,
        "inertia_curve": inertia_curve,
        "cluster_by_customer": cluster_by_customer,
        "features": list(features),
    }
