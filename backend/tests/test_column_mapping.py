"""Column detection + mapping tests (Phase 3)."""
import pandas as pd

from app.services import column_mapping_service as cms


def _df(**cols):
    return pd.DataFrame(cols)


def test_detects_standard_names():
    df = _df(
        CustomerID=["C1", "C2", "C3"],
        InvoiceNo=["1", "2", "3"],
        InvoiceDate=["2020-01-01", "2020-01-02", "2020-01-03"],
        Quantity=["5", "6", "7"],
        UnitPrice=["1.5", "2.0", "3.0"],
    )
    detected = {d["column"]: d for d in cms.detect_mapping(df)}
    assert detected["CustomerID"]["suggested_field"] == "customer_id"
    assert detected["InvoiceDate"]["suggested_field"] == "order_date"
    assert detected["Quantity"]["suggested_field"] == "quantity"
    assert detected["UnitPrice"]["suggested_field"] == "unit_price"


def test_detects_alternative_names_and_case():
    df = _df(
        Client=["C1", "C2", "C3"],
        Order_Date=["2020-01-01", "2020-02-01", "2020-03-01"],
        UNITS=["5", "6", "7"],
        Unit_Cost=["1.5", "2.0", "3.0"],
        Order_Ref=["A1", "A2", "A3"],
    )
    detected = {d["column"]: d for d in cms.detect_mapping(df)}
    assert detected["Client"]["suggested_field"] == "customer_id"
    assert detected["Order_Date"]["suggested_field"] == "order_date"
    assert detected["UNITS"]["suggested_field"] == "quantity"
    assert detected["Unit_Cost"]["suggested_field"] == "unit_price"


def test_conflict_resolved_to_higher_confidence():
    df = _df(
        CustomerID=["C1", "C2", "C3", "C4"],
        CustomerID_Backup=["C1", "C2", "C3", "C4"],
        InvoiceDate=["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"],
        Quantity=["1", "2", "3", "4"],
        UnitPrice=["1", "2", "3", "4"],
    )
    detected = {d["column"]: d for d in cms.detect_mapping(df)}
    claimed = [d["column"] for d in detected.values() if d["suggested_field"] == "customer_id"]
    assert len(claimed) == 1  # only one column may claim a required field
    loser = [c for c in ("CustomerID", "CustomerID_Backup") if c not in claimed]
    assert detected[loser[0]]["match_status"] == "conflict"


def test_validate_mapping_flags_missing_required():
    df = _df(Foo=["1", "2"], Bar=["3", "4"])
    result = cms.validate_mapping(df, {"Foo": "customer_id"})
    assert result["ok"] is False
    assert set(result["missing_required"]) == {"order_date", "quantity", "unit_price"}


def test_validate_mapping_flags_duplicate_target():
    df = _df(A=["1"], B=["2"])
    result = cms.validate_mapping(df, {"A": "quantity", "B": "quantity",
                                       "customer_id_missing": ""})
    # duplicate target for a single field must error
    assert result["ok"] is False
    assert any("both map" in e for e in result["errors"])


def test_validate_mapping_ok_with_full_mapping():
    df = _df(
        Client=["C1", "C2"],
        Purchase_Date=["2020-01-01", "2020-01-02"],
        Units=["5", "6"],
        Unit_Cost=["1.5", "2.0"],
        Order_Ref=["A", "B"],
    )
    mapping = {
        "Client": "customer_id",
        "Purchase_Date": "order_date",
        "Units": "quantity",
        "Unit_Cost": "unit_price",
        "Order_Ref": "order_id",
    }
    result = cms.validate_mapping(df, mapping)
    assert result["ok"] is True, result["errors"]


def test_normalize_dataframe_renames_to_schema():
    df = _df(Client=["C1"], Purchase_Date=["2020-01-01"], Units=["5"],
             Unit_Cost=["1.5"], Ignored=["x"])
    mapping = {"Client": "customer_id", "Purchase_Date": "order_date",
               "Units": "quantity", "Unit_Cost": "unit_price", "Ignored": ""}
    out = cms.normalize_dataframe(df, mapping)
    assert list(out.columns) == ["customer_id", "order_date", "quantity", "unit_price"]
    assert "customer_id" in out.columns
    assert "Ignored" not in out.columns
