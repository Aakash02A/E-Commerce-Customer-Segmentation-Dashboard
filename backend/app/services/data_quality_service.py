"""Data quality validation and reporting (Phase 4 gate).

Given a typed, normalized DataFrame (see schema_normalization_service) this
module runs structural and value checks, applies configurable business rules,
and produces a quality report. It also returns the subset of "eligible" rows
that may proceed to the PySpark analysis stage.

Business rules are intentionally NON-destructive and documented: negative
quantities/prices are treated as returns/refunds (kept but flagged), duplicate
rows are reported rather than auto-removed, and cancelled transactions can be
excluded. The exact rules applied are recorded in the report for audit.
"""
from __future__ import annotations

import pandas as pd

# --- Configurable handling policies ---------------------------------------
DEFAULT_RULES = {
    # Cancelled / returned transactions: "exclude" (drop from analysis) or "keep".
    "cancelled_policy": "exclude",
    # Negative quantity (returns): "exclude" or "keep".
    "negative_quantity_policy": "exclude",
    # Zero quantity: always reported; excluded from revenue eligible set.
    "zero_quantity_policy": "exclude",
    # Exact duplicate transaction rows: "keep" (may be legitimate) or "drop".
    "duplicate_policy": "keep",
    # Missing customer_id: required field -> always blocks/limits eligible rows.
    "missing_customer_id_policy": "block",
    # Missing/invalid date: required field -> limits eligible rows.
    "invalid_date_policy": "block",
}

VALID_POLICY_VALUES = {"exclude", "keep", "drop", "block"}


def _normalize_rules(rules: dict | None) -> dict:
    merged = dict(DEFAULT_RULES)
    if rules:
        for key, value in rules.items():
            if key in merged and value in VALID_POLICY_VALUES:
                merged[key] = value
    return merged


def validate(df_typed: pd.DataFrame, mapping: dict, rules: dict | None = None,
             price_outlier_std: float = 6.0) -> dict:
    """Validate a typed normalized dataset and return a quality report.

    Returns:
      {
        passed: bool,
        blocking_errors: [ {field, message} ],
        warnings: [ {code, message, count, example_rows} ],
        excluded: [ {code, message, count} ],
        summary: { total_rows, valid_rows, invalid_rows, duplicate_candidates,
                   missing_value_counts, distinct_customers, ... },
        applied_rules: dict,
        eligible_dataframe: pd.DataFrame,   # rows cleared for analysis
      }
    """
    rules = _normalize_rules(rules)
    total_rows = int(len(df_typed))
    warnings, excluded = [], []
    blocking_errors = []

    if total_rows == 0:
        return _fail([{"field": "dataset",
                       "message": "The dataset has no data rows to analyze."}],
                     rules, df_typed, total_rows, warnings, excluded)

    # --- Structural: required fields must exist after mapping ------------
    required = ["customer_id", "order_date", "quantity", "unit_price"]
    present = set(c for c in required if c in df_typed.columns)
    missing_fields = [f for f in required if f not in present]
    if "order_id" not in df_typed.columns:
        warnings.append({
            "code": "missing_order_id",
            "message": "No Order/Invoice ID column was mapped. Frequency will fall "
                       "back to counting transaction rows, which can overstate order "
                       "counts when orders have multiple line items.",
            "count": total_rows, "example_rows": [],
        })
    if missing_fields:
        return _fail([{"field": f, "message": f"Required field '{f}' is missing from "
                       "the mapped dataset."} for f in missing_fields],
                     rules, df_typed, total_rows, warnings, excluded)

    # --- Value-level row masks ------------------------------------------
    valid_mask = pd.Series(True, index=df_typed.index)

    # Missing customer_id
    cust_missing = df_typed["customer_id"].isna()
    _add_issue(cust_missing, warnings, excluded, rules["missing_customer_id_policy"],
               "missing_customer_id", "Rows with a missing Customer ID (excluded from analysis)",
               valid_mask)

    # Invalid / missing dates
    date_missing = df_typed["order_date"].isna()
    _add_issue(date_missing, warnings, excluded, rules["invalid_date_policy"],
               "invalid_or_missing_date", "Rows with a missing or unparseable order date",
               valid_mask)

    # Non-numeric or missing quantity / price
    for col, code, label in [("quantity", "invalid_quantity", "quantity"),
                             ("unit_price", "invalid_price", "price")]:
        bad = df_typed[col].isna()
        _add_issue(bad, warnings, excluded, "exclude",
                   code, f"Rows with a non-numeric or missing {label}", valid_mask)

    # Negative quantity (returns)
    neg_qty = df_typed["quantity"] < 0
    _add_issue(neg_qty, warnings, excluded, rules["negative_quantity_policy"],
               "negative_quantity",
               "Rows with a negative quantity (possible returns/refunds)", valid_mask)

    # Zero quantity
    zero_qty = df_typed["quantity"] == 0
    _add_issue(zero_qty, warnings, excluded, rules["zero_quantity_policy"],
               "zero_quantity", "Rows with zero quantity", valid_mask)

    # Negative price
    neg_price = df_typed["unit_price"] < 0
    _add_issue(neg_price, warnings, excluded, "exclude",
               "negative_price", "Rows with a negative unit price", valid_mask)

    # Cancelled transactions (via mapped transaction_status), name-based CANCELLATION like UK data
    if "transaction_status" in df_typed.columns:
        cancelled = _detect_cancelled(df_typed["transaction_status"])
        _add_issue(cancelled, warnings, excluded, rules["cancelled_policy"],
                   "cancelled_transaction", "Rows flagged as cancelled transactions", valid_mask)
    # order_id beginning with 'C' is the classic cancellation marker
    if "order_id" in df_typed.columns:
        cancelled_oid = df_typed["order_id"].astype("string").str.strip().str.upper().str.startswith("C")
        _add_issue(cancelled_oid.fillna(False), warnings, excluded, rules["cancelled_policy"],
                   "cancelled_invoice", "Invoices whose ID starts with 'C' (cancelled)", valid_mask)

    # Duplicate transaction rows
    dup_subset = [c for c in ("order_id", "customer_id", "order_date", "quantity",
                              "unit_price", "product_id") if c in df_typed.columns]
    if dup_subset and rules["duplicate_policy"] == "drop":
        dups = df_typed.duplicated(subset=dup_subset, keep="first")
        dup_count = int(dups.sum())
        _add_issue(dups, warnings, excluded, "drop",
                   "duplicate_rows", "Exact duplicate transaction rows (dropped)", valid_mask)
    else:
        dup_count = int(df_typed.duplicated(subset=dup_subset, keep=False).sum()) if dup_subset else 0
        if dup_count:
            warnings.append({"code": "duplicate_candidates", "count": dup_count,
                             "message": "Detected duplicate transaction rows; kept because "
                                        "duplicates may be legitimate repeated lines.",
                             "example_rows": []})

    # Suspicious unit prices (extreme outliers), reported as warnings only
    if df_typed["unit_price"].notna().any():
        outlier = _price_outliers(df_typed["unit_price"], price_outlier_std)
        if int(outlier.sum()):
            warnings.append({"code": "price_outlier", "count": int(outlier.sum()),
                             "message": "Unit prices far outside the normal range were detected "
                                        "and should be reviewed.",
                             "example_rows": _example(df_typed[outlier])})

    eligible = df_typed[valid_mask].copy()
    valid_rows = int(len(eligible))
    invalid_rows = total_rows - valid_rows

    # --- Blocking decision ----------------------------------------------
    # Analysis is blocked only by fundamentally unusable data: missing required
    # fields (already returned above) or zero eligible rows.
    if valid_rows == 0:
        return _fail([{"field": "eligible_rows",
                       "message": "No valid transaction rows remain after applying the "
                                  "data-quality rules. Analysis is blocked."}],
                     rules, df_typed, total_rows, warnings, excluded,
                     summary=_summarize(df_typed, valid_mask, total_rows, dup_count))

    summary = _summarize(df_typed, valid_mask, total_rows, dup_count)
    return {
        "passed": True,
        "blocking_errors": [],
        "warnings": warnings,
        "excluded": excluded,
        "summary": summary,
        "applied_rules": rules,
        "eligible_dataframe": eligible,
    }


# --- helpers ---------------------------------------------------------------
def _add_issue(mask, warnings, excluded, policy, code, message, valid_mask):
    mask = mask.fillna(False).astype(bool)
    count = int(mask.sum())
    if count == 0:
        return
    if policy in ("exclude", "drop", "block"):
        valid_mask &= ~mask
        excluded.append({"code": code, "message": message, "count": count})
    else:  # keep
        warnings.append({"code": code, "message": message + " (kept in analysis)",
                         "count": count, "example_rows": []})


def _detect_cancelled(series: pd.Series) -> pd.Series:
    tokens = ("cancel", "returned", "refund", "void", "canceled")
    s = series.astype("string").str.lower().str.strip()
    return s.apply(lambda v: bool(v) and any(t in v for t in tokens)).fillna(False)


def _price_outliers(price: pd.Series, std_mult: float) -> pd.Series:
    q1, q3 = price.quantile(0.25), price.quantile(0.75)
    iqr = q3 - q1
    if pd.isna(iqr) or iqr == 0:
        mean, std = price.mean(), price.std()
        if pd.isna(std) or std == 0:
            return pd.Series(False, index=price.index)
        return price > (mean + std_mult * std)
    upper = q3 + 3 * iqr
    return price > upper


def _example(df: pd.DataFrame, n: int = 5) -> list:
    return df.head(n).astype(object).where(lambda x: x.notna(), None).values.tolist()


def _summarize(df, valid_mask, total_rows, dup_count) -> dict:
    eligible = df[valid_mask]
    missing_counts = {}
    for c in ("customer_id", "order_id", "order_date", "quantity", "unit_price"):
        if c in df.columns:
            missing_counts[c] = int(df[c].isna().sum())
    date_range = None
    if "order_date" in eligible.columns and eligible["order_date"].notna().any():
        date_range = {
            "min": str(eligible["order_date"].min()),
            "max": str(eligible["order_date"].max()),
        }
    return {
        "total_rows": total_rows,
        "valid_rows": int(valid_mask.sum()),
        "invalid_rows": total_rows - int(valid_mask.sum()),
        "duplicate_candidates": int(dup_count),
        "missing_value_counts": missing_counts,
        "distinct_customers": int(eligible["customer_id"].nunique()) if "customer_id" in eligible else 0,
        "order_date_range": date_range,
    }


def _fail(blocking_errors, rules, df, total_rows, warnings, excluded, summary=None) -> dict:
    return {
        "passed": False,
        "blocking_errors": blocking_errors,
        "warnings": warnings,
        "excluded": excluded,
        "summary": summary or {"total_rows": total_rows, "valid_rows": 0,
                               "invalid_rows": total_rows, "duplicate_candidates": 0,
                               "missing_value_counts": {}, "distinct_customers": 0,
                               "order_date_range": None},
        "applied_rules": rules,
        "eligible_dataframe": df.iloc[0:0],
    }
