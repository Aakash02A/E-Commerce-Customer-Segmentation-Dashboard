"""Flask application factory."""
import logging

from flask import Flask, jsonify, render_template, request

from app.config import FRONTEND_DIR, get_config
from app.database.connection import close_db, init_db
from app.utils.logging_utils import configure_logging

log = logging.getLogger("segmentation")


def create_app(config_name: str | None = None) -> Flask:
    app = Flask(
        __name__,
        template_folder=str(FRONTEND_DIR / "templates"),
        static_folder=str(FRONTEND_DIR / "static"),
        static_url_path="/static",
    )
    app.config.from_object(get_config(config_name))

    # Ensure storage directories exist.
    app.config["DATABASE_PATH"].parent.mkdir(parents=True, exist_ok=True)
    for key in ("UPLOAD_DIR", "PROCESSED_DIR", "EXPORTS_DIR", "TEMP_DIR"):
        app.config[key].mkdir(parents=True, exist_ok=True)

    init_db(app)
    app.teardown_appcontext(close_db)
    configure_logging(app)

    register_blueprints(app)
    register_error_handlers(app)

    @app.get("/api/health")
    def health():
        return jsonify({"success": True, "status": "ok", "service": "customer-segmentation-dashboard"})

    return app


def register_blueprints(app: Flask):
    from app.routes.pages import pages_bp
    from app.routes.uploads import uploads_bp
    from app.routes.mappings import mappings_bp
    from app.routes.validation import validation_bp
    from app.routes.analyses import analyses_bp

    app.register_blueprint(pages_bp)
    app.register_blueprint(uploads_bp)
    app.register_blueprint(mappings_bp)
    app.register_blueprint(validation_bp)
    app.register_blueprint(analyses_bp)


def register_error_handlers(app: Flask):
    """Centralized error handling: JSON for API routes, HTML pages otherwise."""

    def wants_json() -> bool:
        return request.path.startswith("/api/")

    @app.errorhandler(400)
    @app.errorhandler(404)
    @app.errorhandler(413)
    @app.errorhandler(500)
    def handle_error(err):
        code = err.code if hasattr(err, "code") else 500
        if wants_json():
            names = {400: "Bad request", 404: "Not found", 413: "File too large", 500: "Internal server error"}
            msg = names.get(code, "Error")
            if getattr(err, "description", None):
                msg = str(err.description)
            return jsonify({"success": False, "message": msg, "details": None}), code
        template = {400: "errors/400.html", 404: "errors/404.html"}.get(code, "errors/500.html")
        return render_template(template), code

    @app.errorhandler(Exception)
    def handle_uncaught(err):
        from werkzeug.exceptions import HTTPException

        if isinstance(err, HTTPException):
            return handle_error(err)
        log.exception("Unhandled error on %s", request.path)
        if wants_json():
            return jsonify({"success": False, "message": "Internal server error", "details": None}), 500
        return render_template("errors/500.html"), 500
