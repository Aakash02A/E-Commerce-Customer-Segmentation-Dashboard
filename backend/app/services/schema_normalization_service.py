"""Schema normalization: coerce the mapped DataFrame into typed canonical form.

Runs after the user confirms mappings and before validation. Converts
quantity/unit_price to numeric, order_date to datetime, and trims identifier /
text fields. Produces parse-tracking columns so the quality service can report
exactly which rows failed to convert, without destructively editing the source.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with standard fields converted to proper dtypes.

    Adds internal helper columns (prefixed ``_``) recording parse success so
    the data-quality layer can distinguish 'missing' from 'unparseable'.
    """
    out = df.copy()

    if "order_date" in out.columns:
        parsed = pd.to_datetime(out["order_date"], errors="coerce", format="mixed", dayfirst=False)
        out["_order_date_parsed"] = parsed
        out["_order_date_failed"] = out["order_date"].notna() & parsed.isna()
        out["order_date"] = parsed

    for col in ("quantity", "unit_price"):
        if col in out.columns:
            numeric = pd.to_numeric(
                out[col].astype("string").str.replace(",", "", regex=False).str.strip(),
                errors="coerce",
            )
            out[f"_{col}_numeric"] = numeric
            # failed = had a value but it did not parse to a number
            out[f"_{col}_failed"] = out[col].notna() & numeric.isna()
            out[col] = numeric

    for col in ("customer_id", "order_id", "product_id", "product_description",
                "country", "transaction_status"):
        if col in out.columns:
            out[col] = out[col].astype("string").str.strip()

    return out


def coerce_summary(df_typed: pd.DataFrame) -> dict:
    """Small helper describing detected dtypes for display/audit."""
    return {
        col: (str(df_typed[col].dtype) if col in df_typed else None)
        for col in ["customer_id", "order_id", "order_date", "quantity", "unit_price"]
    }
