"""Automatic column detection and mapping to a standard internal schema.

This service is independent of any particular dataset's column names: it
matches uploaded columns to a fixed standard schema using a configurable
synonym dictionary plus value/pattern inspection, assigns conservative
confidence scores, and (after user confirmation) renames columns into the
canonical schema used by the analytics pipeline.
"""
from __future__ import annotations

import re

import pandas as pd

# --- Standard internal schema ---------------------------------------------
# requirement: "required" | "recommended" | "optional"
STANDARD_FIELDS = [
    {"field": "customer_id", "label": "Customer ID", "requirement": "required", "kind": "id"},
    {"field": "order_id", "label": "Order / Invoice ID", "requirement": "recommended", "kind": "id"},
    {"field": "order_date", "label": "Order Date", "requirement": "required", "kind": "date"},
    {"field": "quantity", "label": "Quantity", "requirement": "required", "kind": "numeric"},
    {"field": "unit_price", "label": "Unit Price", "requirement": "required", "kind": "numeric"},
    {"field": "product_id", "label": "Product ID", "requirement": "optional", "kind": "id"},
    {"field": "product_description", "label": "Product Description", "requirement": "optional", "kind": "text"},
    {"field": "country", "label": "Country", "requirement": "optional", "kind": "categorical"},
    {"field": "transaction_status", "label": "Transaction Status", "requirement": "optional", "kind": "categorical"},
]

FIELD_INDEX = {f["field"]: f for f in STANDARD_FIELDS}
REQUIRED_FIELDS = [f["field"] for f in STANDARD_FIELDS if f["requirement"] == "required"]

# --- Synonym dictionary (configurable) ------------------------------------
# Keys are standard fields; values are normalized synonym tokens.
SYNONYMS = {
    "customer_id": [
        "customerid", "customer", "client", "clientid", "buyer", "buyercode",
        "account", "accountid", "customercode", "custid", "memberid", "user",
    ],
    "order_id": [
        "invoiceno", "invoice", "invoicenumber", "orderid", "order", "transactionid",
        "transaction", "receipt", "ordernumber", "saleid", "documentno", "orderref",
    ],
    "order_date": [
        "invoicedate", "orderdate", "purchasedate", "transactiontime", "transactiondate",
        "date", "saletime", "createdat", "paymentdate", "timestamp", "duetdate",
    ],
    "quantity": [
        "quantity", "qty", "units", "quantities", "quantitysold", "unitssold",
        "count", "items", "noofitems", "pieces",
    ],
    "unit_price": [
        "unitprice", "price", "unitcost", "sellingprice", "cost", "rate",
        "unitrate", "priceperunit", "productprice", "itemprice",
    ],
    "product_id": [
        "productid", "stockcode", "itemid", "sku", "productcode", "itemcode",
        "partno", "article", "product",
    ],
    "product_description": [
        "description", "productname", "itemname", "productdescription", "title",
        "itemdescription", "name", "producttitle",
    ],
    "country": [
        "country", "region", "market", "shipcountry", "billingcountry",
        "customerlocation", "location", "nation",
    ],
    "transaction_status": [
        "status", "transactionstatus", "orderstatus", "state", "paymentstatus",
        "cancellation", "cancelled",
    ],
}

# Columns whose name strongly suggests they are NOT unit price (totals, etc.).
_AMBIGUOUS_PRICE_TOKENS = ["total", "amount", "subtotal", "grandtotal", "linevalue",
                           "revenue", "discount", "shipping", "tax", "gross", "net"]


def normalize_name(name: str) -> str:
    """Lowercase and strip all non-alphanumeric characters for matching."""
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _token_match(norm_name: str, synonyms: list[str]) -> float:
    """Return 0..1 name-match score against a synonym list."""
    if not norm_name:
        return 0.0
    if norm_name in synonyms:
        return 1.0
    # exact match after removing plural 's'
    if norm_name.rstrip("s") in [s.rstrip("s") for s in synonyms]:
        return 0.9
    # synonym is a substring (e.g. 'customer_id_full' contains 'customerid')
    for s in synonyms:
        if s and s in norm_name:
            return 0.75
        if norm_name and norm_name in s:
            return 0.6
    return 0.0


def _classify_values(series: pd.Series, kind: str) -> float:
    """Return 0..1 evidence score that a column's values fit the target kind.

    Inspects a bounded sample so scoring stays cheap on large files.
    """
    sample = series.dropna().astype(str)
    if sample.empty:
        return 0.0
    sample = sample.head(50)

    if kind == "date":
        return _score_dates(sample)
    if kind == "numeric":
        return _score_numeric(sample)
    if kind == "id":
        # IDs: mostly unique, non-null tokens; short-ish values.
        uniq_ratio = sample.nunique() / max(len(sample), 1)
        return 0.6 if uniq_ratio >= 0.5 else 0.25
    if kind == "categorical":
        uniq = sample.nunique()
        return 0.6 if 1 < uniq <= 50 else 0.2
    return 0.3  # text


def _score_numeric(sample: pd.Series) -> float:
    cleaned = sample.str.replace(",", "", regex=False).str.strip()
    numeric = pd.to_numeric(cleaned, errors="coerce")
    ratio = numeric.notna().mean()
    return 1.0 if ratio >= 0.9 else (0.5 if ratio >= 0.5 else 0.0)


def _score_dates(sample: pd.Series) -> float:
    parsed = pd.to_datetime(sample, errors="coerce", format="mixed", dayfirst=False)
    ratio = parsed.notna().mean()
    # Guard: pure integers (e.g. quantities) can sometimes parse as dates; require
    # non-numeric-looking values to score high.
    all_numeric = (pd.to_numeric(sample, errors="coerce").notna()).mean() >= 0.9
    if all_numeric:
        return 0.1
    return 1.0 if ratio >= 0.7 else (0.4 if ratio >= 0.3 else 0.0)


def detect_mapping(df: pd.DataFrame) -> list[dict]:
    """Suggest a standard field for each uploaded column.

    Returns a list (one entry per source column) with:
      column, suggested_field, confidence (0..1), match_status,
      detected_kind, sample_values, requirement_of_suggestion.
    Also guarantees no two confident columns claim the same required field.
    """
    rows: list[dict] = []
    for col in df.columns:
        norm = normalize_name(col)
        best_field, best_score = None, 0.0
        for field, syns in SYNONYMS.items():
            name_score = _token_match(norm, syns)
            if name_score <= 0:
                continue
            kind = FIELD_INDEX[field]["kind"]
            value_score = _classify_values(df[col], kind)
            # Penalize price-name collisions (e.g. "Total_Price" for unit_price).
            if field == "unit_price" and any(t in norm for t in _AMBIGUOUS_PRICE_TOKENS):
                value_score *= 0.3
            combined = round(0.6 * name_score + 0.4 * value_score, 3)
            if combined > best_score:
                best_score, best_field = combined, field

        # If no name match, try a value-only hint for the strongest unclaimed field.
        suggested = best_field
        confidence = best_score
        detected_kind = _infer_kind(df[col])

        rows.append({
            "column": col,
            "suggested_field": suggested,
            "confidence": confidence,
            "match_status": _status(confidence),
            "detected_kind": detected_kind,
            "sample_values": _sample_values(df[col]),
        })

    _resolve_conflicts(rows, df)
    return rows


def _status(confidence: float) -> str:
    if confidence >= 0.8:
        return "high"
    if confidence >= 0.5:
        return "medium"
    if confidence > 0:
        return "low"
    return "unmapped"


def _infer_kind(series: pd.Series) -> str:
    if _score_dates(series.dropna().astype(str).head(50)) >= 0.7 and \
            _score_numeric(series.dropna().astype(str).head(50)) < 0.5:
        return "date"
    if _score_numeric(series.dropna().astype(str).head(50)) >= 0.9:
        return "numeric"
    uniq = series.dropna().nunique()
    total = len(series.dropna()) or 1
    if uniq / total >= 0.7:
        return "id"
    if uniq <= 50:
        return "categorical"
    return "text"


def _sample_values(series: pd.Series, n: int = 3) -> list[str]:
    vals = series.dropna().astype(str).head(n).tolist()
    return [v[:40] for v in vals]


def _resolve_conflicts(rows: list[dict], df: pd.DataFrame):
    """Ensure a required field is claimed by at most one column.

    When two columns suggest the same field, keep the higher-confidence one
    and demote the other to unmapped (flagged for user resolution).
    """
    by_field: dict[str, list[dict]] = {}
    for r in rows:
        if r["suggested_field"]:
            by_field.setdefault(r["suggested_field"], []).append(r)
    for field, claimers in by_field.items():
        if len(claimers) <= 1:
            continue
        claimers.sort(key=lambda x: x["confidence"], reverse=True)
        for loser in claimers[1:]:
            loser["suggested_field"] = None
            loser["confidence"] = 0.0
            loser["match_status"] = "conflict"


def validate_mapping(df: pd.DataFrame, mapping: dict[str, str]) -> dict:
    """Validate a user-confirmed mapping (source column -> standard field).

    Returns {ok, errors, missing_required, unused_fields, duplicate_targets}.
    Values in mapping are standard field names or "" / None for unused.
    """
    errors: list[str] = []
    target_to_source: dict[str, str] = {}
    columns = set(df.columns)

    for source, target in mapping.items():
        if source not in columns:
            errors.append(f"Source column '{source}' no longer exists in the dataset.")
            continue
        if not target:
            continue
        if target not in FIELD_INDEX:
            errors.append(f"Unknown standard field '{target}'.")
            continue
        if target in target_to_source:
            errors.append(
                f"Two columns ('{target_to_source[target]}' and '{source}') both map "
                f"to '{target}'. Each standard field must come from one column."
            )
        else:
            target_to_source[target] = source

    missing_required = [f for f in REQUIRED_FIELDS if f not in target_to_source]
    if missing_required:
        labels = ", ".join(FIELD_INDEX[f]["label"] for f in missing_required)
        errors.append(f"Missing required fields: {labels}.")

    return {
        "ok": len(errors) == 0,
        "errors": errors,
        "missing_required": missing_required,
        "mapped_fields": list(target_to_source.keys()),
        "duplicate_targets": [t for t in target_to_source if
                              sum(1 for v in mapping.values() if v == t) > 1],
    }


def normalize_dataframe(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    """Rename mapped columns to the standard schema and drop unmapped ones.

    Preserves column order by standard field. Unmapped (optional, unused)
    source columns are dropped from the normalized output; the original file
    remains untouched on disk for recovery/audit.
    """
    keep = {source: target for source, target in mapping.items() if target}
    normalized = df[list(keep.keys())].rename(columns=keep)
    ordered = [f for f in FIELD_INDEX if f in normalized.columns]
    return normalized[ordered].copy()
