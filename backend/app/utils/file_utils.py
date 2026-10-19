"""Safe file handling utilities: extension checks and path-traversal prevention."""
import os
import re
import uuid


def safe_stored_filename(original_filename: str, allowed_extensions: set[str]) -> tuple[str, str]:
    """Return (extension, generated_safe_filename).

    Never trusts the uploaded filename for the on-disk name: a random UUID is
    used as the stem so path traversal, collisions and executable names are all
    neutralized. The extension is validated against the whitelist.
    """
    ext = (os.path.splitext(original_filename or "")[1] or "").lower().lstrip(".")
    if ext not in allowed_extensions:
        raise ValueError(f"Unsupported file extension: {ext or 'none'}")
    # Extension is whitelisted; build a fully random, safe name.
    return ext, f"{uuid.uuid4().hex}.{ext}"


def sanitize_display_name(name: str) -> str:
    """Strip directory components and unsafe chars from a name for display."""
    base = os.path.basename(name or "")
    base = re.sub(r"[^A-Za-z0-9._\- ()]", "_", base)
    return base[:200] or "dataset"


def within_directory(path, directory) -> bool:
    """Return True if `path` resolves inside `directory` (defense in depth)."""
    try:
        resolved = os.path.realpath(path)
        root = os.path.realpath(directory)
        return resolved == root or resolved.startswith(root + os.sep)
    except (OSError, ValueError):
        return False
