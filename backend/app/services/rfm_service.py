"""RFM analysis service (Phase 7).

Deterministically scores customers on Recency / Frequency / Monetary using the
persisted Phase 6 customer aggregates. This is a pure computation module: it
receives aggregate dicts and returns scored rows. All database access stays in
the repositories and all HTTP handling in routes. Segmentation (segment labels,
clusters) and recommendations are Phase 8+ and are intentionally NOT computed
here.

Scoring methodology (documented, deterministic)
-----------------------------------------------
* Quintile bins 1..5 (``n_bins`` configurable, default 5). Higher score = better.
* For each metric a customer's score is based on the cohort's *average percentile
  rank* of that value:
      pct   = (count(values < v) + count(values <= v)) / 2 / n
      score = floor(pct * n_bins) + 1   (capped at n_bins)
* Ties (identical values) always receive the identical score, and results are
  independent of input ordering, so scoring is fully deterministic and
  reproducible.
* Frequency and Monetary: higher raw value is better -> higher score.
* Recency (days since last purchase): FEWER days is better, so the direction is
  inverted (score = n_bins + 1 - ascending_score); the most recent customer gets
  the highest R score.
* Small cohorts compress toward the middle; a single customer receives the
  neutral middle bin (3). This is expected and documented, not an error.
* None / missing metric values are excluded from the pool and receive a None
  score. Negative recency (a purchase dated after the reference) is clamped to 0.
"""
from __future__ import annotations

from datetime import date, datetime

DEFAULT_BINS = 5


def parse_date(value):
    """Best-effort parse of an ISO date/datetime (or date/datetime object)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()
    if not s:
        return None
    s = s.replace("T", " ").split(" ")[0]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _days_between(reference: date, last_purchase: date) -> int:
    """Recency in whole days, clamped to 0 (never negative)."""
    delta = (reference - last_purchase).days
    return delta if delta >= 0 else 0


def _quintile_scores(values: list, n_bins: int = DEFAULT_BINS,
                     higher_is_better: bool = True) -> list:
    """Assign 1..n_bins scores by average percentile rank.

    Tie-safe (equal values -> equal scores) and order-independent.
    """
    pool = [v for v in values if v is not None]
    n = len(pool)
    out: list = []
    for v in values:
        if v is None:
            out.append(None)
            continue
        if n == 0:
            out.append(None)
            continue
        less = sum(1 for x in pool if x < v)
        leq = sum(1 for x in pool if x <= v)
        pct = ((less + leq) / 2.0) / n
        score = int(pct * n_bins) + 1
        if score > n_bins:
            score = n_bins
        if not higher_is_better:
            score = n_bins + 1 - score
        out.append(score)
    return out


def calculate_rfm(reference_date, aggregates: list[dict],
                  n_bins: int = DEFAULT_BINS) -> list[dict]:
    """Compute RFM values + scores for a cohort of customer aggregates.

    Each aggregate: {customer_id, frequency, monetary, last_order_date}.
    ``reference_date`` (or the max last_order_date as a fallback) anchors
    Recency. Returns a list of {customer_id, recency_days, r_score, f_score,
    m_score}.
    """
    if not aggregates:
        return []

    ref = parse_date(reference_date)
    last_dates = [parse_date(a.get("last_order_date")) for a in aggregates]
    if ref is None:
        valid_dates = [d for d in last_dates if d is not None]
        ref = max(valid_dates) if valid_dates else None

    recency_vals = [
        _days_between(ref, d) if (ref is not None and d is not None) else None
        for d in last_dates
    ]
    freq_vals = [a.get("frequency") for a in aggregates]
    monetary_vals = [a.get("monetary") for a in aggregates]

    r_scores = _quintile_scores(recency_vals, n_bins, higher_is_better=False)
    f_scores = _quintile_scores(freq_vals, n_bins, higher_is_better=True)
    m_scores = _quintile_scores(monetary_vals, n_bins, higher_is_better=True)

    updates = []
    for i, a in enumerate(aggregates):
        updates.append({
            "customer_id": a["customer_id"],
            "recency_days": recency_vals[i],
            "r_score": r_scores[i],
            "f_score": f_scores[i],
            "m_score": m_scores[i],
        })
    return updates
