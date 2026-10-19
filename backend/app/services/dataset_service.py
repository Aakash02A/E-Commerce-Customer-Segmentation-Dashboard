"""Load the working DataFrame for an upload from its stored file."""
from __future__ import annotations

import pandas as pd

from app.services import file_reader_service
from app.services import schema_normalization_service


def load_raw_dataframe(row) -> pd.DataFrame:
    """Read the uploaded file (respecting the selected sheet) as strings."""
    sheet = row["selected_sheet"] if "selected_sheet" in row.keys() else None
    return file_reader_service.read_dataframe(row["file_path"], row["extension"], sheet)


def load_normalized_typed(path: str) -> pd.DataFrame:
    """Load a persisted normalized CSV and re-apply canonical type coercion.

    Reading back with dtype=str (blank -> NA) then normalizing keeps the
    pipeline idempotent and dependency-free (no parquet engine required).
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])
    df = df.replace(r"^\s*$", pd.NA, regex=True)
    return schema_normalization_service.normalize_types(df)
