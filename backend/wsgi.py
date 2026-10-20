"""Production WSGI entrypoint (Phase 11, Task 1).

Exposes the Flask app for real WSGI servers instead of Flask's development
server (``run.py``). The app is created once at import; the server then handles
requests against this ``app`` object.

    Windows (Waitress):  waitress-serve --host=127.0.0.1 --port=5000 wsgi:app
    Linux/Docker (Gunicorn):
        gunicorn --bind 0.0.0.0:5000 --workers 1 --threads 4 --timeout 120 wsgi:app

Defaults to the ``production`` config (DEBUG off, no reloader). Set ``FLASK_ENV``
to override. Host/port are supplied by the server command line, not hardcoded.
"""
import os

from app import create_app

app = create_app(os.getenv("FLASK_ENV", "production"))
