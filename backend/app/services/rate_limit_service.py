"""In-memory sliding-window rate limiting (Phase 11, Task 3).

Protects the resource-intensive, mutating API endpoints from runaway clients
while leaving ordinary reads untouched. This is deliberately a dependency-free,
per-process limiter sized for the single-node MVP described in the project
context.

Scope
-----
Only requests that (a) use an unsafe method (``POST``/``PUT``/``PATCH``/
``DELETE``) and (b) target a path under ``/api/`` are counted. That single rule
excludes, by construction: the ``/api/health`` check (a GET), every static
asset, all HTML page routes, and every read-only ``GET`` API call -- so normal
browsing and dashboard polling are never throttled. Within the mutating set,
two tiers keep the tightest budget on the most expensive work. A *matched*
request is classified from Flask's resolved ``url_rule``/``endpoint`` (so the
heavy endpoints land in the ``expensive`` tier regardless of raw-path spelling);
an *unmatched* mutating request (unknown path, slash-case redirect) falls back to
the normalized ``request.path`` so the API surface is still guarded:

* ``expensive`` -- the upload endpoint and the three Spark jobs (matched via
  ``uploads.create_upload`` / ``analyses.aggregate`` / ``analyses.run_rfm`` /
  ``analyses.run_segment``).
* ``standard``  -- every other mutating API write (``.../mapping``,
  ``.../validate``) and unmatched ``/api/`` traffic.

Important limitations (must be understood before scaling out)
-------------------------------------------------------------
* **Per-process, in-memory.** Counters live in this worker's RAM. They RESET on
  process restart and are NOT shared between worker processes or app instances.
  With N workers an effective limit ``L`` becomes up to ``N*L`` globally, and a
  client may be throttled differently depending on which worker handles each
  request. This is acceptable for the single-worker MVP; a multi-worker or
  multi-host deployment needs a shared store (e.g. Redis) -- which is explicitly
  out of scope here to avoid adding a dependency.
* **Client identity is the socket peer** (``request.remote_addr``). We do NOT
  trust ``X-Forwarded-For`` by default because it is trivially spoofable when
  there is no trusted upstream proxy. If deployed behind one, wire Flask's
  ``ProxyFix`` middleware (configured with the correct hop count) so
  ``remote_addr`` reflects the real client; do not naively read the header.
* The 1-worker/4-thread topology assumed for Phase 11 means requests from one
  client can land on any thread; every counter mutation is therefore guarded by
  a lock so the window stays exact under concurrency.
* **Bounded memory.** Per-client deques are pruned in place on each hit, and an
  opportunistic global sweep (at most once per window, run under the same lock)
  deletes keys whose newest entry has aged out -- so an idle client's key does
  not linger forever. The sweep never removes an entry that is still inside its
  window, so it cannot open a hole in an active client's budget.
"""
from __future__ import annotations

import math
import threading
import time
from collections import defaultdict, deque

from flask import Flask, jsonify, request

# Methods that change state / consume heavy resources. Safe methods (GET/HEAD/
# OPTIONS) are never limited, which is what keeps reads and health checks free.
_UNSAFE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

# The authoritative, Flask-registered endpoints whose handlers do heavy work
# (disk ingest + Spark jobs). Matched requests are classified by ``request.
# endpoint`` so the tier is immune to spelling tricks (slashes, casing, etc.).
# Names come straight from ``app.url_map`` -- see the Task 3 review.
_EXPENSIVE_ENDPOINTS = frozenset({
    "uploads.create_upload",
    "analyses.aggregate",
    "analyses.run_rfm",
    "analyses.run_segment",
})

# Path fragments used ONLY as a fallback to keep a *matched-tier* guarantee for
# requests that never resolved to an endpoint (unknown /api/ paths, trailing-
# slash or double-slash redirects, method-not-allowed). Werkzeug has already
# normalized ``request.path`` by the time before_request runs.
_EXPENSIVE_MARKERS = ("aggregate", "/rfm", "/segment")


def _monotonic() -> float:
    return time.monotonic()


class SlidingWindowLimiter:
    """Exact sliding-window-log counter, safe to share across threads.

    A pure, clock-injectable core so window expiry and ``Retry-After`` math are
    unit-testable without sleeping. One deque of request timestamps per key;
    entries older than the window are evicted on each check.
    """

    def __init__(self, clock=_monotonic):
        self._clock = clock
        self._lock = threading.Lock()
        self._events: dict[str, deque[float]] = defaultdict(deque)
        # Memory-bound bookkeeping: the largest window seen (so a sweep never
        # evicts an entry that is still live under any configured window) and the
        # last sweep time (so the global sweep is opportunistic, not per-request).
        self._max_window = 0.0
        self._last_gc: float | None = None

    def reset(self) -> None:
        """Drop all counters. Exposed for tests and administrative reuse."""
        with self._lock:
            self._events.clear()
            self._last_gc = None

    def _maybe_gc(self, now: float) -> None:
        """Opportunistic, bounded global sweep to reclaim idle client keys.

        Called from :meth:`hit` while the instance lock is already held, so it
        cannot race with an admission decision. It runs at most once per window
        (``_max_window``) -- so the amortized cost stays O(active keys) rather
        than touching the whole store on every request -- and it only ever:
        * deletes a key whose newest timestamp has already aged out of the
          window (hence the whole deque is expired), or is empty; and
        * leaves every key with at least one in-window entry fully intact.
        Expired entries at the FRONT of surviving deques are not touched here;
        they are trimmed lazily by the per-key eviction in :meth:`hit`.
        """
        if self._last_gc is None:
            self._last_gc = now
            return
        interval = self._max_window or 1.0
        if now - self._last_gc < interval:
            return
        cutoff = now - interval
        for key in [k for k, dq in self._events.items() if not dq or dq[-1] <= cutoff]:
            del self._events[key]
        self._last_gc = now

    def hit(self, key: str, limit: int, window_seconds: float, now: float | None = None):
        """Record one request for ``key`` and decide admission.

        Returns ``(allowed, retry_after_seconds, remaining)``. ``retry_after`` is
        the whole-seconds wait until the oldest counted request leaves the
        window (>=1 when denied); ``remaining`` is how many are left in the
        budget after this decision (0 when denied). When ``now`` is omitted the
        injected clock is used (tests pass an explicit timestamp).
        """
        if now is None:
            now = self._clock()
        with self._lock:
            if window_seconds > self._max_window:
                self._max_window = float(window_seconds)
            self._maybe_gc(now)
            q = self._events[key]
            cutoff = now - window_seconds
            # Evict timestamps that have slid out of the window.
            while q and q[0] <= cutoff:
                q.popleft()
            if len(q) >= limit:
                # Denied: wait until the oldest entry ages out of the window.
                retry_after = window_seconds - (now - q[0])
                retry_after = max(1, math.ceil(retry_after))
                return False, retry_after, 0
            q.append(now)
            return True, 0, max(0, limit - len(q))


def validate_rate_limit_config(config) -> list[str]:
    """Return human-readable problems with the rate-limit config (empty = ok).

    Mirrors the security-config validation style: a shipped default always
    passes, and a nonsensical override (zero/negative/non-integer window or
    limit) is caught at startup rather than silently disabling protection.
    """
    problems: list[str] = []
    window = config.get("RATE_LIMIT_WINDOW_SECONDS")
    if not isinstance(window, int) or isinstance(window, bool) or window <= 0:
        problems.append(f"RATE_LIMIT_WINDOW_SECONDS must be a positive integer: {window!r}")
    for name in ("RATE_LIMIT_EXPENSIVE", "RATE_LIMIT_STANDARD"):
        val = config.get(name)
        if not isinstance(val, int) or isinstance(val, bool) or val <= 0:
            problems.append(f"{name} must be a positive integer: {val!r}")
    return problems


def _tier_for_request() -> str | None:
    """Classify the current request as 'expensive', 'standard', or None.

    ``None`` means "do not limit": safe methods, non-``/api/`` paths (static +
    HTML pages), and the health check all fall here so ordinary reads stay free.

    Matched requests are classified from Flask's resolved ``url_rule``/``endpoint``
    (reliable, immune to raw-path spelling). Unmatched mutating requests fall back
    to the normalized ``request.path`` so the API surface is still protected from
    brute-forcing without ever limiting non-API traffic.
    """
    if request.method not in _UNSAFE_METHODS:
        return None

    rule = request.url_rule
    if rule is not None:
        # A concrete route matched. Only API routes are candidates for limiting.
        if not rule.rule.startswith("/api/") or rule.rule == "/api/health":
            return None
        return "expensive" if request.endpoint in _EXPENSIVE_ENDPOINTS else "standard"

    # No route matched (unknown path, a trailing-/double-slash redirect, or a
    # method-not-allowed). Guard only clearly-API paths; anything else is free.
    path = request.path
    if not path.startswith("/api/") or path == "/api/health":
        return None
    if path == "/api/uploads" or any(marker in path for marker in _EXPENSIVE_MARKERS):
        return "expensive"
    return "standard"


def register_rate_limiting(app: Flask) -> None:
    """Validate the rate-limit config and attach the ``before_request`` guard.

    Fails fast at startup on an invalid configuration (same philosophy as the
    security headers). The limiter instance lives on ``app.extensions`` so a
    fresh app -- as every test builds -- starts from an empty slate, and so an
    operator can reset counters without a restart.

    Limits and the window are read from ``app.config`` on each request, not
    captured at registration, so they can be tuned per environment (and in
    tests) without re-registering the hook.
    """
    problems = validate_rate_limit_config(app.config)
    if problems:
        raise ValueError(
            "Invalid rate-limit configuration; refusing to start with a broken "
            "policy:\n  - " + "\n  - ".join(problems))

    limiter = SlidingWindowLimiter()
    app.extensions["rate_limiter"] = limiter

    @app.before_request
    def _enforce_rate_limit():
        if not app.config.get("RATE_LIMIT_ENABLED", True):
            return None
        tier = _tier_for_request()
        if tier is None:
            return None

        limit = (app.config["RATE_LIMIT_EXPENSIVE"] if tier == "expensive"
                 else app.config["RATE_LIMIT_STANDARD"])
        window = app.config["RATE_LIMIT_WINDOW_SECONDS"]
        client_ip = request.remote_addr or "unknown"

        allowed, retry_after, remaining = limiter.hit(f"{tier}:{client_ip}", limit, window)
        if allowed:
            return None

        # Consistent with the app's JSON error envelope (see response_utils).
        # The security after_request hook still decorates this body, so the 429
        # carries the same CSP / hardening headers as any other JSON response.
        response = jsonify({
            "success": False,
            "message": "Too many requests. Please slow down and retry later.",
            "details": {
                "retry_after_seconds": retry_after,
                "limit": limit,
                "window_seconds": window,
                "scope": tier,
            },
        })
        response.status_code = 429
        response.headers["Retry-After"] = str(retry_after)
        response.headers["X-RateLimit-Limit"] = str(limit)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        return response
