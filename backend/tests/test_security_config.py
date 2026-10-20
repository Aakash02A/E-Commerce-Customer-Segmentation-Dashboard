"""Phase 11 — security/CSP configuration validation (Task 1 follow-up).

Covers approved decision #2: strict validation of security/CSP config overrides.
Valid configuration is preserved and used as-is; wildcard/unsafe/malformed values
and overrides that would weaken the approved default are rejected, and the app
fails fast at startup rather than emitting an unsafe Content-Security-Policy.
"""
import pytest

from app.services.security_service import (
    build_csp,
    register_security_headers,
    validate_security_config,
)


def _cfg(**overrides):
    """A valid baseline config (mirrors the shipped defaults) with overrides."""
    base = {
        "SECURITY_HEADERS_ENABLED": True,
        "CDN_ORIGIN": "https://cdn.jsdelivr.net",
        "CSP_STYLE_SOURCES": ["'self'", "https://cdn.jsdelivr.net", "'unsafe-inline'"],
        "CSP_FRAME_ANCESTORS": "'none'",
        "X_FRAME_OPTIONS": "DENY",
        "REFERRER_POLICY": "no-referrer",
    }
    base.update(overrides)
    return base


def _script_src(csp: str) -> str:
    return next(d.strip() for d in csp.split(";") if d.strip().startswith("script-src"))


# --- The shipped defaults are valid and safe ------------------------------
def test_defaults_validate_clean():
    assert validate_security_config(_cfg()) == []


def test_default_script_src_is_preserved_exactly():
    csp = build_csp(_cfg())
    assert _script_src(csp) == "script-src 'self' https://cdn.jsdelivr.net"
    assert "unsafe-inline" not in _script_src(csp)
    assert "unsafe-eval" not in csp


# --- Valid overrides are preserved ----------------------------------------
def test_valid_cdn_override_is_preserved():
    cfg = _cfg(CDN_ORIGIN="https://cdn.example.com",
               CSP_STYLE_SOURCES=["'self'", "https://cdn.example.com", "'unsafe-inline'"])
    assert validate_security_config(cfg) == []
    assert _script_src(build_csp(cfg)) == "script-src 'self' https://cdn.example.com"


def test_valid_optional_port_override():
    assert validate_security_config(_cfg(CDN_ORIGIN="https://cdn.example.com:8443")) == []


def test_sameorigin_xframe_and_strict_referrer_are_valid():
    assert validate_security_config(
        _cfg(X_FRAME_OPTIONS="SAMEORIGIN", REFERRER_POLICY="strict-origin")) == []


# --- Unsafe / invalid CDN origins are rejected ------------------------------
@pytest.mark.parametrize("bad", [
    "*",                       # bare wildcard
    "https://*",               # wildcard host
    "http://cdn.jsdelivr.net",  # insecure scheme
    "//cdn.jsdelivr.net",      # scheme-relative
    "cdn.jsdelivr.net",        # missing scheme
    "https://cdn.jsdelivr.net/assets",  # has a path
    "https://cdn.jsdelivr.net '",        # injection attempt (space + quote)
    "https://cdn; default-src *",        # directive injection via ';'
    "javascript:alert(1)",     # dangerous scheme
    "",                         # empty
])
def test_unsafe_cdn_origin_rejected(bad):
    problems = validate_security_config(_cfg(CDN_ORIGIN=bad))
    assert any("CDN_ORIGIN" in p for p in problems), problems


# --- Unsafe / invalid style sources are rejected ----------------------------
@pytest.mark.parametrize("bad_style", [
    [],                                                    # empty
    "*",                                                   # bare wildcard token
    ["*", "'self'"],                                       # wildcard among tokens
    ["'self'", "https://cdn.jsdelivr.net", "'unsafe-eval'"],  # eval never allowed
    ["'self'", "http://cdn.jsdelivr.net"],                 # insecure scheme token
    ["'self'", "https://cdn.jsdelivr.net/x y"],            # space (breaks directive)
    ["'self'", "'unknown-keyword'"],                       # not a valid keyword
])
def test_unsafe_style_sources_rejected(bad_style):
    problems = validate_security_config(_cfg(CSP_STYLE_SOURCES=bad_style))
    assert problems, bad_style


def test_non_list_style_sources_rejected():
    assert validate_security_config(_cfg(CSP_STYLE_SOURCES="'self'"))


# --- Discrete directives validated against whitelists -----------------------
def test_wildcard_frame_ancestors_rejected():
    assert any("CSP_FRAME_ANCESTORS" in p
               for p in validate_security_config(_cfg(CSP_FRAME_ANCESTORS="*")))


def test_empty_xframe_rejected():
    assert any("X_FRAME_OPTIONS" in p
               for p in validate_security_config(_cfg(X_FRAME_OPTIONS="")) )


def test_unsafe_referrer_rejected():
    assert any("REFERRER_POLICY" in p
               for p in validate_security_config(_cfg(REFERRER_POLICY="unsafe-url")))


# --- Fail-fast at startup: register_security_headers refuses unsafe config --
# (Config reads env vars once at import time, so the correct way to exercise the
# startup guard is to mutate the live app.config and call the registrar, exactly
# as create_app does.)
def test_register_raises_on_unsafe_cdn(app):
    app.config["CDN_ORIGIN"] = "*"
    with pytest.raises(ValueError, match="CDN_ORIGIN"):
        register_security_headers(app)


def test_register_raises_on_wildcard_style_source(app):
    app.config["CSP_STYLE_SOURCES"] = ["*", "'self'"]
    with pytest.raises(ValueError, match="CSP_STYLE_SOURCES"):
        register_security_headers(app)


def test_register_raises_on_unsafe_referrer(app):
    app.config["REFERRER_POLICY"] = "unsafe-url"
    with pytest.raises(ValueError, match="REFERRER_POLICY"):
        register_security_headers(app)


def test_register_accepts_and_preserves_valid_override(app):
    app.config["CDN_ORIGIN"] = "https://cdn.example.com"
    app.config["CSP_STYLE_SOURCES"] = [
        "'self'", "https://cdn.example.com", "'unsafe-inline'"]
    register_security_headers(app)  # must not raise
    # A valid override is preserved end-to-end, not silently reverted.
    assert _script_src(build_csp(app.config)) == "script-src 'self' https://cdn.example.com"


def test_validation_runs_on_live_app_config(app):
    # The shipped app config (a plain dict) validates clean.
    assert isinstance(app.config, dict)
    assert validate_security_config(app.config) == []
