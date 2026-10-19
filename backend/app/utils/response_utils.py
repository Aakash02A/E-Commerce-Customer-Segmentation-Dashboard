"""Consistent JSON response helpers."""
from flask import jsonify


def success_response(data=None, message="ok", status=200, **extra):
    payload = {"success": True, "message": message, "data": data}
    payload.update(extra)
    return jsonify(payload), status


def error_response(message, status=400, details=None, **extra):
    payload = {"success": False, "message": message, "details": details}
    payload.update(extra)
    return jsonify(payload), status
