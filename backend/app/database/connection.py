"""SQLite connection helpers using the standard library sqlite3."""
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from flask import current_app, g


DB_KEY = "sqlite_db"


def _configure(conn: sqlite3.Connection) -> sqlite3.Connection:
    """Apply row factory + integrity pragmas consistently to a connection."""
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    # Wait briefly instead of failing immediately on concurrent writes.
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def get_db() -> sqlite3.Connection:
    """Return a connection for the current application context."""
    if not hasattr(g, DB_KEY):
        db_path = Path(current_app.config["DATABASE_PATH"])
        db_path.parent.mkdir(parents=True, exist_ok=True)
        setattr(g, DB_KEY, _configure(sqlite3.connect(str(db_path))))
    return getattr(g, DB_KEY)


def close_db(exc=None):
    db = g.pop(DB_KEY, None)
    if db is not None:
        try:
            if exc is not None:
                db.rollback()
            db.close()
        except sqlite3.Error:
            pass


@contextmanager
def transaction():
    """Group several statements into one atomic unit of work.

    Commits on success; rolls back the whole unit if any statement raises, so
    callers never leave the database in a half-applied state.
    """
    db = get_db()
    try:
        yield db
        db.commit()
    except Exception:
        try:
            db.rollback()
        except sqlite3.Error:
            pass
        raise


def init_db(app):
    """Create tables from schema.sql and apply additive column migrations."""
    db_path = Path(app.config["DATABASE_PATH"])
    db_path.parent.mkdir(parents=True, exist_ok=True)
    schema_file = Path(__file__).parent / "schema.sql"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(schema_file.read_text(encoding="utf-8"))
        _ensure_columns(conn)
        conn.commit()
    finally:
        conn.close()


# Additive column migrations for tables that already exist in older databases.
# CREATE TABLE IF NOT EXISTS never changes an existing table, so new optional
# columns must be added explicitly. Keep entries to nullable/DEFAULT columns
# only (safe, non-destructive additions).
_COLUMN_MIGRATIONS = {
    "customer_metrics": {
        "total_quantity": "REAL",
        "first_order_date": "TEXT",
    },
}


def _has_column(conn, table: str, column: str) -> bool:
    return any(row[1] == column for row in conn.execute(f"PRAGMA table_info({table})"))


def _ensure_columns(conn):
    for table, columns in _COLUMN_MIGRATIONS.items():
        for column, sql_type in columns.items():
            if not _has_column(conn, table, column):
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {sql_type}")



def foreign_keys_enabled(app) -> bool:
    """Diagnostic: confirm the app's connection factory enables FK enforcement."""
    db_path = Path(app.config["DATABASE_PATH"])
    conn = _configure(sqlite3.connect(str(db_path)))
    try:
        return bool(conn.execute("PRAGMA foreign_keys").fetchone()[0])
    finally:
        conn.close()

