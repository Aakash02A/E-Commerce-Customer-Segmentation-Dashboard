"""Phase 11 — security response headers + CSP (Task 1).

Verifies the hardened header set is emitted on HTML and JSON documents, that the
CSP is compatible with the CDN-based frontend while keeping ``script-src`` strict
(no ``'unsafe-inline'``), and that empty/download responses are left alone. These
are pure header assertions using the Flask test client; they neither require
Spark nor depend on inline-script externalization (that is Task 2), since the
test client does not enforce CSP.
"""
from app.services.security_service import build_csp

_PAGES = [
    "/", "/upload", "/mapping", "/validation", "/dashboard",
    "/segments", "/customers", "/history", "/settings",
]


def _document_headers(client, path):
    resp = client.get(path)
    assert resp.status_code == 200, f"{path} -> {resp.status_code}"
    return resp.headers


def test_all_pages_carry_security_headers(client):
    for path in _PAGES:
        h = _document_headers(client, path)
        assert "Content-Security-Policy" in h, f"missing CSP on {path}"
        assert h["X-Content-Type-Options"] == "nosniff", path
        assert h["X-Frame-Options"] == "DENY", path
        assert h["Referrer-Policy"] == "no-referrer", path
        assert h["Cross-Origin-Opener-Policy"] == "same-origin", path
        assert "camera=()" in h["Permissions-Policy"], path


def test_script_src_is_strict_without_unsafe_inline(client):
    csp = _document_headers(client, "/dashboard")["Content-Security-Policy"]
    script = next(d.strip() for d in csp.split(";") if d.strip().startswith("script-src"))
    # The strict script-src must NOT permit inline scripts; the frontend's two
    # inline blocks are externalized in Task 2 so this can hold everywhere.
    assert "'unsafe-inline'" not in script
    assert "'self'" in script
    assert "https://cdn.jsdelivr.net" in script


def test_csp_style_src_allows_runtime_inline(client):
    # Bootstrap/Chart.js set element styles at runtime; style-src must allow it.
    csp = _document_headers(client, "/dashboard")["Content-Security-Policy"]
    style = next(d.strip() for d in csp.split(";") if d.strip().startswith("style-src"))
    assert "'unsafe-inline'" in style
    assert "https://cdn.jsdelivr.net" in style


def test_csp_required_directives_present(client):
    csp = _document_headers(client, "/dashboard")["Content-Security-Policy"]
    for directive in ("default-src 'self'", "frame-ancestors 'none'",
                      "base-uri 'self'", "form-action 'self'", "object-src 'none'",
                      "connect-src 'self'"):
        assert directive in csp, directive


def test_json_api_response_has_headers(client):
    resp = client.get("/api/health")
    assert resp.status_code == 200
    assert "Content-Security-Policy" in resp.headers
    assert resp.headers["X-Content-Type-Options"] == "nosniff"


def test_json_error_response_has_headers(client):
    resp = client.get("/api/analyses/does-not-exist")
    assert resp.status_code == 404
    assert "Content-Security-Policy" in resp.headers
    assert resp.headers["X-Content-Type-Options"] == "nosniff"


def test_static_asset_is_served_and_not_html_governed(client):
    # CSP is scoped to documents (HTML/JSON); a JS asset response must still be a
    # usable 200 and is intentionally not treated as an HTML document.
    resp = client.get("/static/js/main.js")
    assert resp.status_code == 200
    assert resp.mimetype != "text/html"


def test_response_carries_no_server_banner_from_the_app(client):
    # Flask/Werkzeug never place Server on the Response object (verified: a bare
    # werkzeug Response has no Server header, and the test client injects none),
    # so the security hook's defensive pop is a no-op. The real Server banner is
    # added by the WSGI server layer and is scrubbed there in the WSGI task.
    resp = client.get("/")
    assert "Server" not in resp.headers
    assert "Werkzeug" not in resp.headers.get("Server", "")


def test_default_csp_has_no_wildcard_or_unsafe_eval(client):
    # The shipped default policy must never open script-src to the world or
    # allow eval, regardless of style-src's deliberate inline allowance.
    csp = _document_headers(client, "/dashboard")["Content-Security-Policy"]
    assert "*" not in csp
    assert "unsafe-eval" not in csp


def test_default_config_yields_safe_script_src(app):
    # Guard the shipped defaults specifically: a mis-set CDN_ORIGIN could widen
    # script-src, so assert the default app config produces a strict policy.
    csp = build_csp(app.config)
    script = next(d.strip() for d in csp.split(";") if d.strip().startswith("script-src"))
    assert script == "script-src 'self' https://cdn.jsdelivr.net"


def test_headers_absent_when_feature_disabled(client):
    client.application.config["SECURITY_HEADERS_ENABLED"] = False
    resp = client.get("/")
    assert "Content-Security-Policy" not in resp.headers
    client.application.config["SECURITY_HEADERS_ENABLED"] = True


def test_build_csp_is_pure_and_config_driven(app):
    # A custom CDN origin must flow into script/font-src without side effects.
    cfg = {
        "CDN_ORIGIN": "https://cdn.example.com",
        "CSP_STYLE_SOURCES": ["'self'", "https://cdn.example.com", "'unsafe-inline'"],
        "CSP_FRAME_ANCESTORS": "'none'",
    }
    csp = build_csp(cfg)
    assert "script-src 'self' https://cdn.example.com" in csp
    assert "font-src 'self' https://cdn.example.com" in csp
    assert "unsafe-inline" not in csp.split("script-src")[1].split(";")[0]
