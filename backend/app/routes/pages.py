"""Page routes: render the multi-page HTML interface.

Each page is a thin shell; actual data is loaded by JavaScript from the JSON API.
"""
from flask import Blueprint, render_template

pages_bp = Blueprint("pages", __name__)


@pages_bp.get("/")
def home():
    return render_template("index.html")


@pages_bp.get("/upload")
def upload():
    return render_template("upload.html")


@pages_bp.get("/mapping")
def mapping():
    return render_template("column_mapping.html")


@pages_bp.get("/validation")
def validation():
    return render_template("data_validation.html")


@pages_bp.get("/dashboard")
def dashboard():
    return render_template("dashboard.html")


@pages_bp.get("/segments")
def segments():
    return render_template("customer_segments.html")


@pages_bp.get("/customers")
def customers():
    return render_template("customer_details.html")


@pages_bp.get("/history")
def history():
    return render_template("analysis_history.html")


@pages_bp.get("/settings")
def settings():
    return render_template("settings.html")
