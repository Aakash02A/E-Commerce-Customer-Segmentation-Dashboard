"""Phase 11 — rate limiting (Task 3).

Two layers of coverage:

* Unit tests for :class:`SlidingWindowLimiter` with an injected clock, proving
  the exact sliding-window semantics (admission boundary, window expiry,
  Retry-After math, per-key isolation, thread safety) without sleeping.
* Integration tests through the real Flask request pipeline, proving the guard
  only limits mutating ``/api/`` requests, returns a consistent JSON 429 with a
  ``Retry-After`` header, keeps tiers and client IPs independent, respects the
  enable flag / fail-fast validation, and still carries the security headers.
"""
import threading

import pytest

from app import create_app
from app.services.rate_limit_service import (
    SlidingWindowLimiter,
    _tier_for_request,
    register_rate_limiting,
    validate_rate_limit_config,
)

# --------------------------------------------------------------------------
# Unit: the sliding-window core
# --------------------------------------------------------------------------

def test_allows_up_to_limit_then_denies():
    limiter = SlidingWindowLimiter()
    results = [limiter.hit("k", limit=3, window_seconds=60, now=100.0)[0] for _ in range(4)]
    assert results == [True, True, True, False]


def test_remaining_decreases_within_budget():
    limiter = SlidingWindowLimiter()
    rem = [limiter.hit("k", 3, 60, now=100.0)[2] for _ in range(3)]
    assert rem == [2, 1, 0]


def test_window_expiry_re_admits():
    limiter = SlidingWindowLimiter()
    assert limiter.hit("k", 2, 60, now=0.0)[0] is True
    assert limiter.hit("k", 2, 60, now=10.0)[0] is True
    # at t=30 the window (t-60 .. t) still contains both -> denied
    assert limiter.hit("k", 2, 60, now=30.0)[0] is False
    # at t=61 the first entry (0.0) has slid out (61-60=1 > 0) -> one admitted
    assert limiter.hit("k", 2, 60, now=61.0)[0] is True


def test_retry_after_counts_down_from_oldest_entry():
    limiter = SlidingWindowLimiter()
    limiter.hit("k", 2, 60, now=0.0)
    limiter.hit("k", 2, 60, now=5.0)
    allowed, retry_after, remaining = limiter.hit("k", 2, 60, now=20.0)
    assert allowed is False and remaining == 0
    # oldest is at 0.0; it leaves the window in 60-20=40s -> ceil(40)=40
    assert retry_after == 40


def test_retry_after_is_at_least_one_second():
    limiter = SlidingWindowLimiter()
    limiter.hit("k", 1, 60, now=0.0)
    # hit right at the boundary: window - (now - oldest) == 0 -> floored to 1
    allowed, retry_after, _ = limiter.hit("k", 1, 60, now=0.0)
    assert allowed is False
    assert retry_after >= 1


def test_keys_are_isolated():
    limiter = SlidingWindowLimiter()
    assert limiter.hit("a", 1, 60, now=0.0)[0] is True
    assert limiter.hit("a", 1, 60, now=0.0)[0] is False
    # a different key has its own fresh budget
    assert limiter.hit("b", 1, 60, now=0.0)[0] is True


def test_reset_clears_counters():
    limiter = SlidingWindowLimiter()
    limiter.hit("k", 1, 60, now=0.0)
    assert limiter.hit("k", 1, 60, now=0.0)[0] is False
    limiter.reset()
    assert limiter.hit("k", 1, 60, now=0.0)[0] is True


def test_concurrent_hits_admit_exactly_the_limit():
    """The lock keeps the budget exact under concurrent threads."""
    limiter = SlidingWindowLimiter()
    limit = 10
    admitted = []
    lock = threading.Lock()

    def worker():
        allowed, _, _ = limiter.hit("shared", limit, 3600)
        if allowed:
            with lock:
                admitted.append(1)

    threads = [threading.Thread(target=worker) for _ in range(40)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(admitted) == limit


# --------------------------------------------------------------------------
# Unit: bounded memory cleanup (Fix 1)
# --------------------------------------------------------------------------

def test_cleanup_removes_expired_keys():
    limiter = SlidingWindowLimiter()
    limiter.hit("a", 5, 60, now=0.0)
    limiter.hit("b", 5, 60, now=0.0)
    # A hit far past the window triggers one sweep; a/b (only entry at t=0) are
    # fully expired and their keys are reclaimed.
    limiter.hit("c", 5, 60, now=100.0)
    keys = set(limiter._events)
    assert "c" in keys
    assert "a" not in keys and "b" not in keys


def test_cleanup_preserves_active_keys():
    limiter = SlidingWindowLimiter()
    limiter.hit("a", 5, 60, now=0.0)     # will be expired
    limiter.hit("b", 5, 60, now=50.0)    # still inside the window at t=100
    limiter.hit("c", 5, 60, now=100.0)   # triggers sweep (cutoff = 100-60 = 40)
    assert "b" in limiter._events        # newest (50) > cutoff -> kept
    assert "a" not in limiter._events    # newest (0) <= cutoff -> removed


def test_cleanup_never_drops_within_window_events():
    limiter = SlidingWindowLimiter()
    for _ in range(3):
        assert limiter.hit("k", 3, 60, now=0.0)[0] is True
    # A later hit still inside the window must not disturb k's exhausted budget.
    limiter.hit("other", 3, 60, now=50.0)
    assert limiter.hit("k", 3, 60, now=55.0)[0] is False


def test_cleanup_is_opportunistic_not_per_request():
    """No sweep runs while still within one interval, so idle keys accumulate
    only transiently and the store is not walked on every request."""
    limiter = SlidingWindowLimiter()
    limiter.hit("k", 1, 60, now=0.0)
    for t in range(1, 59):               # every call is < interval from last_gc
        limiter.hit(f"noise{t}", 1, 60, now=float(t))
    assert "k" in limiter._events        # nothing swept yet (window not elapsed)


def test_concurrent_access_with_active_cleanup_is_race_free():
    """Exercise the sweep concurrently with admissions; the shared lock must
    prevent any mutation-during-iteration or lost-update failure."""
    limiter = SlidingWindowLimiter()
    errors = []

    def worker(tid):
        try:
            for i in range(50):          # advancing now forces gc every call
                limiter.hit(f"k{tid}", 5, 1.0, now=float(i * 2))
        except Exception as exc:         # noqa: BLE001 - captured for the assert
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors


# --------------------------------------------------------------------------
# Unit: config validation
# --------------------------------------------------------------------------

def _cfg(**overrides):
    base = {
        "RATE_LIMIT_WINDOW_SECONDS": 60,
        "RATE_LIMIT_EXPENSIVE": 10,
        "RATE_LIMIT_STANDARD": 30,
    }
    base.update(overrides)
    return base


def test_default_rate_config_is_valid():
    assert validate_rate_limit_config(_cfg()) == []


@pytest.mark.parametrize("overrides", [
    {"RATE_LIMIT_WINDOW_SECONDS": 0},
    {"RATE_LIMIT_WINDOW_SECONDS": -5},
    {"RATE_LIMIT_WINDOW_SECONDS": "60"},
    {"RATE_LIMIT_EXPENSIVE": 0},
    {"RATE_LIMIT_EXPENSIVE": -1},
    {"RATE_LIMIT_STANDARD": 0},
    {"RATE_LIMIT_STANDARD": None},
])
def test_invalid_rate_config_is_rejected(overrides):
    assert validate_rate_limit_config(_cfg(**overrides)) != []


# --------------------------------------------------------------------------
# Integration: the before_request guard on a live app
# --------------------------------------------------------------------------

def _app_with_limits(expensive=10, standard=30, window=60, enabled=True):
    app = create_app("testing")
    app.config.update(
        RATE_LIMIT_ENABLED=enabled,
        RATE_LIMIT_WINDOW_SECONDS=window,
        RATE_LIMIT_EXPENSIVE=expensive,
        RATE_LIMIT_STANDARD=standard,
    )
    return app


def test_startup_rejects_invalid_rate_config():
    app = create_app("testing")
    app.config["RATE_LIMIT_EXPENSIVE"] = 0
    with pytest.raises(ValueError):
        register_rate_limiting(app)


def test_health_check_is_never_limited():
    app = _app_with_limits(expensive=1, standard=1)
    client = app.test_client()
    codes = [client.get("/api/health").status_code for _ in range(5)]
    assert codes == [200, 200, 200, 200, 200]


def test_html_page_get_is_not_limited():
    app = _app_with_limits(expensive=1, standard=1)
    client = app.test_client()
    for _ in range(4):
        assert client.get("/").status_code == 200


def test_expensive_endpoint_returns_json_429_with_retry_after():
    app = _app_with_limits(expensive=2)
    client = app.test_client()
    # first two calls pass the guard and reach the view (nonexistent analysis -> not 429)
    assert client.post("/api/analyses/nope/rfm").status_code != 429
    assert client.post("/api/analyses/nope/rfm").status_code != 429
    resp = client.post("/api/analyses/nope/rfm")
    assert resp.status_code == 429
    body = resp.get_json()
    assert body["success"] is False
    assert body["details"]["scope"] == "expensive"
    retry = resp.headers.get("Retry-After")
    assert retry is not None and int(retry) >= 1
    assert resp.headers["X-RateLimit-Limit"] == "2"


def test_429_still_carries_security_headers():
    app = _app_with_limits(expensive=1)
    client = app.test_client()
    client.post("/api/analyses/nope/rfm")
    resp = client.post("/api/analyses/nope/rfm")
    assert resp.status_code == 429
    csp = resp.headers.get("Content-Security-Policy", "")
    assert "script-src 'self' https://cdn.jsdelivr.net" in csp


def test_tiers_are_independent():
    app = _app_with_limits(expensive=1, standard=5)
    client = app.test_client()
    client.post("/api/analyses/nope/rfm")          # exhausts expensive tier
    assert client.post("/api/analyses/nope/rfm").status_code == 429
    # standard tier has its own budget and is unaffected
    assert client.post("/api/uploads/nope/mapping").status_code != 429


def test_client_ips_have_separate_counters():
    app = _app_with_limits(expensive=1)
    client = app.test_client()
    a = {"environ_base": {"REMOTE_ADDR": "10.0.0.1"}}
    b = {"environ_base": {"REMOTE_ADDR": "10.0.0.2"}}
    assert client.post("/api/analyses/nope/rfm", **a).status_code != 429
    assert client.post("/api/analyses/nope/rfm", **a).status_code == 429
    # a different source IP is untouched
    assert client.post("/api/analyses/nope/rfm", **b).status_code != 429


def test_disabled_limiter_never_blocks():
    app = _app_with_limits(expensive=1, enabled=False)
    client = app.test_client()
    codes = [client.post("/api/analyses/nope/rfm").status_code for _ in range(5)]
    assert 429 not in codes


# --------------------------------------------------------------------------
# Integration + classifier: route-classification hardening (Fix 2)
#
# Two complementary layers:
#  * Matched routes go through the real client, so the *endpoint branch* of the
#    classifier is exercised (heavy endpoints land in the expensive tier by
#    ``request.endpoint`` -- immune to raw-path spelling).
#  * Path-spelling rules are locked deterministically by calling the classifier
#    directly inside a request context, avoiding dependence on Werkzeug's
#    redirect internals for slash/case variants.
# Two server-style raw-PATH_INFO cases (leading double slash, unmatched path)
# are additionally driven through the real pipeline because they are the ones the
# Flask ``before_request`` hook reliably sees (verified: routing to a matched
# endpoint or deferring the 404 both still run the guard).
# --------------------------------------------------------------------------

def _raw_post(app, path, remote_addr="10.9.9.9"):
    """Issue a POST with a server-style raw PATH_INFO (as waitress/gunicorn
    deliver), bypassing the test client's URL-reference normalization."""
    import io
    env = {
        "REQUEST_METHOD": "POST", "PATH_INFO": path, "QUERY_STRING": "",
        "SERVER_NAME": "localhost", "SERVER_PORT": "80",
        "SERVER_PROTOCOL": "HTTP/1.1", "HTTP_HOST": "localhost",
        "wsgi.input": io.BytesIO(b""), "wsgi.url_scheme": "http",
        "CONTENT_LENGTH": "0", "REMOTE_ADDR": remote_addr,
    }
    box = {}
    app(env, lambda status, headers: box.setdefault("status", int(status.split()[0])))
    return box["status"]


@pytest.mark.parametrize("path,scope", [
    ("/api/uploads", "expensive"),
    ("/api/uploads/nope/aggregate", "expensive"),
    ("/api/analyses/nope/rfm", "expensive"),
    ("/api/analyses/nope/segment", "expensive"),
    ("/api/uploads/nope/mapping", "standard"),
    ("/api/uploads/nope/validate", "standard"),
])
def test_every_mutating_endpoint_is_limited_in_its_tier(path, scope):
    app = _app_with_limits(expensive=1, standard=1)
    client = app.test_client()
    assert client.post(path).status_code != 429          # first call allowed
    resp = client.post(path)                             # second exhausts it
    assert resp.status_code == 429
    assert resp.get_json()["details"]["scope"] == scope


@pytest.mark.parametrize("method,path,expected", [
    # Matched heavy endpoints -- expensive tier.
    ("POST", "/api/uploads", "expensive"),
    ("POST", "/api/uploads/x/aggregate", "expensive"),
    ("POST", "/api/analyses/x/rfm", "expensive"),
    ("POST", "/api/analyses/x/segment", "expensive"),
    # Cheaper writes -- standard tier.
    ("POST", "/api/uploads/x/mapping", "standard"),
    ("POST", "/api/uploads/x/validate", "standard"),
    # Spelling must never demote a heavy endpoint: a marker survives a slash.
    ("POST", "/api/analyses/x/rfm/", "expensive"),
    ("POST", "/api//uploads/x/rfm", "expensive"),
    # Unknown / other mutating API traffic stays protected (standard).
    ("POST", "/api/does-not-exist", "standard"),
    ("PUT", "/api/whatever", "standard"),
    ("DELETE", "/api/whatever", "standard"),
    # Never limited: safe methods, health, non-API, static, case-mismatch.
    ("GET", "/api/uploads", None),
    ("HEAD", "/api/analyses/x/rfm", None),
    ("POST", "/api/health", None),
    ("POST", "/history", None),
    ("POST", "/static/js/home.js", None),
    ("POST", "/API/uploads", None),
])
def test_classifier_tier_decision(method, path, expected):
    app = create_app("testing")
    with app.test_request_context(path, method=method):
        assert _tier_for_request() == expected


def test_leading_double_slash_upload_is_classified_expensive():
    # expensive=1, standard=50: a fast 429 proves "//api/uploads" is normalized
    # and matched to uploads.create_upload (expensive), not the loose standard.
    app = _app_with_limits(expensive=1, standard=50)
    codes = [_raw_post(app, "//api/uploads") for _ in range(2)]
    assert codes[1] == 429


def test_unmatched_api_post_is_guarded_through_pipeline():
    app = _app_with_limits(expensive=50, standard=1)
    codes = [_raw_post(app, "/api/does-not-exist") for _ in range(2)]
    assert codes[1] == 429


def test_case_mismatched_path_is_not_limited_and_routes_nothing():
    # '/API/uploads' matches no route (case-sensitive) and fails the /api/
    # prefix, so it is a cheap 404 that is deliberately never counted.
    app = _app_with_limits(expensive=1, standard=1)
    codes = [_raw_post(app, "/API/uploads") for _ in range(5)]
    assert 429 not in codes


def test_forwarded_header_is_not_trusted_for_identity():
    app = _app_with_limits(expensive=1)
    client = app.test_client()
    # Same socket peer, spoofed distinct X-Forwarded-For values -> one shared
    # bucket, so the second request is still blocked (the header is ignored).
    assert client.post("/api/analyses/nope/rfm",
                       headers={"X-Forwarded-For": "1.2.3.4"}).status_code != 429
    assert client.post("/api/analyses/nope/rfm",
                       headers={"X-Forwarded-For": "5.6.7.8"}).status_code == 429

