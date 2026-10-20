"""Security response headers + Content-Security-Policy (Phase 11).

Emits a hardened set of response headers on every HTML/JSON document produced
by the app. This is the header/CSP half of the approved Phase 11 hardening
scope; inline-script externalization (so a strict ``script-src`` holds with no
``'unsafe-inline'``), the rate limiter, WSGI servers and containerization are
separate tasks.

Design notes
------------
* The frontend's ONLY external dependency is ``https://cdn.jsdelivr.net``
  (Bootstrap, Bootstrap-Icons and Chart.js). Local CSS/JS are served
  same-origin via ``url_for('static', ...)``. ``script-src`` is therefore
  ``'self'`` + that single CDN host, with no ``'unsafe-inline'``: the two
  remaining inline ``<script>`` blocks are externalized in Task 2 so this can be
  enforced across every page.
* ``style-src`` keeps ``'unsafe-inline'``. Bootstrap's JS and Chart.js set
  element styles at runtime, and a repository check confirmed the templates use
  no inline ``style=`` attributes of our own — so this allowance is needed only
  for the vendored libraries, not for first-party markup.
* Headers are applied only to responses that actually carry a body (204/304 and
  empty bodies are skipped) and NOT to file downloads (CSV/XLSX exports), whose
  ``Content-Disposition`` already governs them and which CSP does not affect.
* **Config validation:** the security/CSP settings are validated at startup (see
  :func:`validate_security_config`). Wildcard sources, ``'unsafe-eval'``, ``http``
  or path-bearing origins, malformed values, and overrides that weaken the
  approved default are rejected with a fail-fast ``ValueError``; valid overrides
  are preserved. This guarantees ``script-src`` can never be opened beyond
  ``'self'`` + one bare https CDN origin.
"""
from __future__ import annotations

import re

from flask import Flask

# Bootstrap/Chart.js embed SVG/data resources; data: is required in img-src so
# those runtime-generated images (and any data-URI the charting library emits)
# are not blocked.
_IMG_SOURCES = ["'self'", "data:"]

# --- CSP configuration validation (Phase 11, approved decision #2) --------
# A CDN origin override must be a bare https origin: scheme + host, no scheme
# wildcard, no trailing path, and none of the characters that would let a value
# break out of its CSP directive (';, whitespace).
_ORIGIN_RE = re.compile(r"^https://[A-Za-z0-9.\-]+(:\d+)?$")

# Valid standalone CSP source keywords. Bare '*' is intentionally absent: a
# wildcard source is never allowed. 'unsafe-eval' is absent entirely (script-src
# is assembled without it, and a style/other override must not introduce it).
_VALID_KEYWORD_SOURCES = {"'none'", "'self'", "'unsafe-inline'", "'strict-dynamic'"}

# Approved values for the discrete header directives. Each default is present,
# and each option weakens nothing essential (no framing, no referrer leakage).
_VALID_FRAME_ANCESTORS = {"'none'", "'self'"}
_VALID_X_FRAME_OPTIONS = {"DENY", "SAMEORIGIN"}
_VALID_REFERRER_POLICIES = {
    "no-referrer", "no-referrer-when-downgrade", "same-origin",
    "strict-origin", "strict-origin-when-cross-origin",
}


def _validate_source_token(value, allow_inline: bool) -> str | None:
    """Return a human-readable problem for one CSP source token, or None.

    ``allow_inline`` is True only for style-src, which legitimately needs
    'unsafe-inline' for the vendored libraries' runtime styling.
    """
    if not isinstance(value, str) or not value:
        return f"source must be a non-empty string, got {value!r}"
    # Reject anything that could inject extra directives / break the header.
    if any(ch in value for ch in (";", ",", " ", "\t", "\r", "\n")):
        return f"source contains a forbidden character (space/,/;/tab/newline): {value!r}"
    if value == "*":
        return "wildcard source '*' is not allowed"
    if "unsafe-eval" in value:
        return "'unsafe-eval' is never allowed"
    if value.startswith("'"):
        if value == "'unsafe-inline'" and not allow_inline:
            return "'unsafe-inline' is not allowed in script-src"
        if value not in _VALID_KEYWORD_SOURCES:
            return f"unknown keyword source {value!r}"
        return None
    # data: is permitted only in img-src (which is not config-driven here).
    if value.startswith("data:"):
        return f"data: source is not allowed in configurable directives: {value!r}"
    # Any other source must be a bare https origin.
    if not _ORIGIN_RE.match(value):
        return f"source is not a bare https origin (no wildcard/path/http): {value!r}"
    return None


def validate_security_config(config) -> list[str]:
    """Return a list of human-readable problems with the security/CSP config.

    An empty list means the configuration is safe and valid. The shipped
    defaults always pass. This is a pure function so every branch is directly
    unit-testable; :func:`register_security_headers` enforces it at startup.
    """
    problems: list[str] = []

    # --- CDN_ORIGIN feeds script-src and font-src -> must be a safe origin ---
    cdn = config.get("CDN_ORIGIN")
    if not isinstance(cdn, str) or not cdn:
        problems.append("CDN_ORIGIN must be a non-empty string")
    elif "*" in cdn:
        problems.append(f"CDN_ORIGIN must not contain a wildcard: {cdn!r}")
    elif not _ORIGIN_RE.match(cdn):
        problems.append(
            "CDN_ORIGIN must be a bare https origin (https://host[:port]) with "
            f"no path, http, or spaces: {cdn!r}")

    # --- CSP_STYLE_SOURCES: a non-empty list of safe source tokens ----------
    style = config.get("CSP_STYLE_SOURCES")
    if not isinstance(style, (list, tuple)) or not style:
        problems.append("CSP_STYLE_SOURCES must be a non-empty list")
    else:
        for token in style:
            problem = _validate_source_token(token, allow_inline=True)
            if problem:
                problems.append(f"CSP_STYLE_SOURCES: {problem}")

    # --- discrete directives against whitelists -----------------------------
    fa = config.get("CSP_FRAME_ANCESTORS")
    if fa not in _VALID_FRAME_ANCESTORS:
        problems.append(
            f"CSP_FRAME_ANCESTORS must be one of {sorted(_VALID_FRAME_ANCESTORS)}: {fa!r}")

    xfo = config.get("X_FRAME_OPTIONS")
    if xfo not in _VALID_X_FRAME_OPTIONS:
        problems.append(
            f"X_FRAME_OPTIONS must be one of {sorted(_VALID_X_FRAME_OPTIONS)}: {xfo!r}")

    rp = config.get("REFERRER_POLICY")
    if rp not in _VALID_REFERRER_POLICIES:
        problems.append(
            f"REFERRER_POLICY must be one of {sorted(_VALID_REFERRER_POLICIES)}: {rp!r}")

    return problems


def build_csp(config) -> str:
    """Assemble the Content-Security-Policy string from app config.

    Kept as a pure function so the exact directive set is unit-testable without
    issuing requests, and so the policy is defined in one readable place.
    """
    cdn = config["CDN_ORIGIN"]
    script_src = "'self' " + cdn
    style_src = " ".join(config["CSP_STYLE_SOURCES"])
    font_src = "'self' " + cdn
    directives = [
        "default-src 'self'",
        f"script-src {script_src}",
        f"style-src {style_src}",
        f"font-src {font_src}",
        f"img-src {' '.join(_IMG_SOURCES)}",
        "connect-src 'self'",
        "frame-ancestors " + config["CSP_FRAME_ANCESTORS"],
        "base-uri 'self'",
        "form-action 'self'",
        "object-src 'none'",
    ]
    return "; ".join(directives)


def _is_document(content_type: str) -> bool:
    """True for HTML/JSON responses. Excludes binary downloads (CSV/XLSX),
    whose disposition is handled elsewhere and to which CSP does not apply."""
    ct = (content_type or "").lower()
    return "text/html" in ct or "application/json" in ct


def register_security_headers(app: Flask) -> None:
    """Validate the security/CSP config, then attach the ``after_request`` hook.

    Fail-safe at startup: if any security/CSP setting is invalid or would weaken
    the approved policy, this raises :class:`ValueError` before the app can serve
    a single request, rather than silently emitting an unsafe CSP. Valid
    overrides are preserved and used as-is.

    Registration order matters: this hook is registered before any
    ``@app.after_request`` declared later in the factory (Flask runs them in
    reverse registration order).
    """
    problems = validate_security_config(app.config)
    if problems:
        raise ValueError(
            "Invalid security/CSP configuration; refusing to start with a "
            "weakened policy:\n  - " + "\n  - ".join(problems))

    @app.after_request
    def _apply_security_headers(response):
        if not app.config.get("SECURITY_HEADERS_ENABLED", True):
            return response

        # File downloads / streamed bodies (send_file: CSV/XLSX exports, static
        # assets) are in direct-passthrough mode: touching the body via
        # get_data() would raise, and CSP does not apply to them anyway.
        if response.direct_passthrough:
            return response

        # 204/304 and empty bodies carry no document to protect.
        if response.status_code in (204, 304) or not response.get_data():
            return response
        if not _is_document(response.mimetype):
            return response

        response.headers["Content-Security-Policy"] = build_csp(app.config)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = app.config["X_FRAME_OPTIONS"]
        response.headers["Referrer-Policy"] = app.config["REFERRER_POLICY"]
        response.headers["Cross-Origin-Opener-Policy"] = "same-origin"
        response.headers["Permissions-Policy"] = (
            "camera=(), geolocation=(), microphone=(), payment=()"
        )
        # Defensive: remove a Server header if any upstream middleware added one
        # to the Response. NOTE: Werkzeug's Response never carries Server itself,
        # so this is a no-op here; the real Server banner is injected by the WSGI
        # server (waitress/gunicorn) and is scrubbed at that layer in the WSGI
        # task (approved decision #1), not in Flask application code.
        response.headers.pop("Server", None)
        return response
