"""Production launch helper (Phase 11, Task 1) — Waitress server.

Starts the Flask app on Waitress, a production-quality pure-Python WSGI server
that runs natively on Windows. This is the intended native-Windows production
startup path; ``run.py`` (Flask's development server with the reloader/debugger)
is for local development only and must not be used to serve production traffic.

The documented ``ident=""`` adjustment tells Waitress to emit **no** ``Server``
response header (verified against a live server), which is the supported way to
suppress the server banner at the WSGI layer — Flask's ``after_request`` cannot
remove a header the server adds after the app returns.

On Linux/Docker the preferred server is Gunicorn (see ``wsgi.py`` for the
command); Gunicorn does not expose a supported switch to drop its ``Server``
header, so that banner is handled at a trusted reverse proxy — see the
deployment docs.

Usage:
    python serve.py

Environment:
    FLASK_RUN_HOST     bind host   (default 127.0.0.1)
    FLASK_RUN_PORT     bind port   (default 5000)
    WAITRESS_THREADS   worker pool (default 4)
    FLASK_ENV          config name (default production)
"""
import os

from waitress import serve

from app import create_app


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        print(f"Invalid {name}={raw!r}; using default {default}")
        return default
    return value if value > 0 else default


def main() -> None:
    host = os.getenv("FLASK_RUN_HOST", "127.0.0.1")
    port = _env_int("FLASK_RUN_PORT", 5000)
    threads = _env_int("WAITRESS_THREADS", 4)
    env = os.getenv("FLASK_ENV", "production")

    app = create_app(env)
    print(
        f"Serving with Waitress on http://{host}:{port} "
        f"(env={env}, threads={threads}, Server header suppressed)"
    )
    # ident="" -> Waitress adds no Server header; debug/reloader never used.
    serve(app, host=host, port=port, threads=threads, ident="")


if __name__ == "__main__":
    main()
