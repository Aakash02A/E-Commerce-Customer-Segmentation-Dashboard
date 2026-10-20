"""Phase 11 — inline-script externalization (Task 2).

The strict CSP uses ``script-src 'self' https://cdn.jsdelivr.net`` with no
'unsafe-inline'. That is only enforceable across every page if NO template ships
an inline <script> block or an inline event handler (onclick=..., etc.), because
a browser would refuse to run either under that policy. These tests lock that
invariant in at the source (templates) and verify the two extracted page scripts
are served as real same-origin JS files.
"""
import glob
import os
import re

# backend/tests -> backend -> project root (frontend/ is a sibling of backend/).
_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_BACKEND_DIR = os.path.dirname(_TESTS_DIR)
_PROJECT_ROOT = os.path.dirname(_BACKEND_DIR)
_TEMPLATE_GLOB = os.path.join(_PROJECT_ROOT, "frontend", "templates", "**", "*.html")

# A bare "<script>" opening tag (no attributes) is an inline block; external
# scripts are always "<script src=...>".
_INLINE_SCRIPT = re.compile(r"<script\s*>", re.IGNORECASE)
# An inline event handler attribute, e.g. onclick="..." / onLoad='...'.
_INLINE_HANDLER = re.compile(r"\son\w+\s*=", re.IGNORECASE)


def _templates():
    return glob.glob(_TEMPLATE_GLOB, recursive=True)


def test_template_glob_found():
    # Guard against a wrong path silently making the next tests vacuous.
    assert len(_templates()) >= 12


def test_no_inline_script_blocks_in_any_template():
    offenders = []
    for path in _templates():
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
        if _INLINE_SCRIPT.search(text):
            offenders.append(os.path.relpath(path, _PROJECT_ROOT))
    assert offenders == [], f"templates with an inline <script>: {offenders}"


def test_no_inline_event_handlers_in_any_template():
    offenders = []
    for path in _templates():
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
        if _INLINE_HANDLER.search(text):
            offenders.append(os.path.relpath(path, _PROJECT_ROOT))
    assert offenders == [], f"templates with an inline event handler: {offenders}"


def test_home_serves_external_script(client):
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "js/home.js" in body
    assert not _INLINE_SCRIPT.search(body)


def test_history_serves_external_script(client):
    resp = client.get("/history")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "js/history.js" in body
    assert not _INLINE_SCRIPT.search(body)


def test_home_js_is_served_as_same_origin_script(client):
    for name in ("home.js", "history.js"):
        resp = client.get(f"/static/js/{name}")
        assert resp.status_code == 200, name
        assert resp.mimetype in ("text/javascript", "application/javascript"), name


def test_strict_script_src_holds_on_externalized_pages(client):
    # The pages we just fixed must carry the strict script-src (no unsafe-inline).
    for path in ("/", "/history"):
        csp = client.get(path).headers["Content-Security-Policy"]
        script = next(d.strip() for d in csp.split(";") if d.strip().startswith("script-src"))
        assert "'unsafe-inline'" not in script
        assert "'unsafe-eval'" not in script
