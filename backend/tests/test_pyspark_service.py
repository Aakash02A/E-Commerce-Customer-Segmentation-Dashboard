"""PySpark processing-layer tests (Phase 6)."""
import pytest

from app.services import pyspark_service
from app.services.pyspark_service import SparkProcessingError
from conftest import write_eligible_csv

DATASET_A = (
    "C1,O1,2024-01-01 00:00:00,2,10.0\n"
    "C1,O2,2024-02-01 00:00:00,1,5.5\n"
    "C2,O3,2024-01-15 00:00:00,3,4.0\n"
    "C2,O3,2024-01-15 00:00:00,2,4.0\n"
)


def _agg(spark, path):
    df, has_oid = pyspark_service.load_eligible(spark, path)
    transactions = pyspark_service.summarize_transactions(df, has_oid)
    customers = pyspark_service.collect_customer_metrics(
        pyspark_service.aggregate_customers(df, has_oid))
    return transactions, {c["customer_id"]: c for c in customers}


# --- Session lifecycle ---------------------------------------------------
def test_session_starts_and_reports_version(spark):
    assert spark.version.startswith("4")
    assert pyspark_service.is_session_active() is True


def test_session_shutdown_and_recreate():
    s1 = pyspark_service.get_session(master="local[1]", app_name="shutdown-test")
    assert s1.version
    pyspark_service.stop_session()
    assert pyspark_service.is_session_active() is False
    # a subsequent call transparently recreates a working session
    s2 = pyspark_service.get_session(master="local[1]", app_name="shutdown-test")
    assert s2.version
    pyspark_service.stop_session()


# --- Line totals + customer aggregates -----------------------------------
def test_line_totals_and_customer_aggregates(spark, tmp_path):
    path = write_eligible_csv(tmp_path, DATASET_A)
    transactions, cust = _agg(spark, path)

    assert transactions == {
        "transaction_count": 4,
        "distinct_customers": 2,
        "distinct_orders": 3,
        "total_quantity": 8.0,
        "total_spend": 45.5,
        "first_date": "2024-01-01",
        "last_date": "2024-02-01",
    }
    assert cust["C1"]["order_count"] == 2
    assert cust["C1"]["total_quantity"] == 3.0
    assert cust["C1"]["total_spend"] == 25.5
    assert cust["C1"]["first_purchase_date"] == "2024-01-01"
    assert cust["C1"]["last_purchase_date"] == "2024-02-01"
    # two lines share order O3 -> distinct order count collapses to 1
    assert cust["C2"]["order_count"] == 1
    assert cust["C2"]["total_quantity"] == 5.0
    assert cust["C2"]["total_spend"] == 20.0


def test_line_total_is_quantity_times_price(spark, tmp_path):
    path = write_eligible_csv(tmp_path, "C1,O1,2024-01-01 00:00:00,3,4.5\n")
    df, _ = pyspark_service.load_eligible(spark, path)
    row = df.select("line_total").collect()[0]
    assert abs(row["line_total"] - 13.5) < 1e-9


# --- Date / numeric parsing ----------------------------------------------
def test_dates_parse_to_calendar_date_without_timezone_shift(spark, tmp_path):
    # late-in-the-day timestamp must NOT roll over to the next date
    path = write_eligible_csv(tmp_path, "C9,O9,2024-03-10 23:30:00,1,1.0\n")
    transactions, cust = _agg(spark, path)
    assert cust["C9"]["first_purchase_date"] == "2024-03-10"
    assert cust["C9"]["last_purchase_date"] == "2024-03-10"
    assert transactions["first_date"] == "2024-03-10"


def test_bare_date_format_supported(spark, tmp_path):
    path = write_eligible_csv(tmp_path, "C1,O1,2024-05-06,2,2.0\n")
    _, cust = _agg(spark, path)
    assert cust["C1"]["first_purchase_date"] == "2024-05-06"


def test_decimal_numeric_strings_are_coerced(spark, tmp_path):
    path = write_eligible_csv(tmp_path, "C1,O1,2024-01-01 00:00:00,2.5,4.0\n")
    _, cust = _agg(spark, path)
    assert cust["C1"]["total_quantity"] == 2.5
    assert cust["C1"]["total_spend"] == 10.0


# --- Robustness / invalid input ------------------------------------------
def test_missing_required_column_raises(spark, tmp_path):
    p = tmp_path / "no_qty.csv"
    p.write_text("customer_id,order_id,order_date,unit_price\nC1,O1,2024-01-01,1.0\n",
                 encoding="utf-8")
    with pytest.raises(SparkProcessingError):
        pyspark_service.run_aggregation(str(p), master="local[1]")


def test_missing_file_raises():
    with pytest.raises(SparkProcessingError):
        pyspark_service.run_aggregation("Z:/does/not/exist_eligible.csv", master="local[1]")


def test_empty_dataset_yields_zero_aggregates(spark, tmp_path):
    path = write_eligible_csv(tmp_path, "")  # header only
    transactions, cust = _agg(spark, path)
    assert transactions["transaction_count"] == 0
    assert transactions["distinct_customers"] == 0
    assert transactions["total_spend"] is None
    assert transactions["first_date"] is None
    assert cust == {}


def test_unparseable_rows_are_dropped_safely(spark, tmp_path):
    body = (
        "C1,O1,2024-01-01 00:00:00,2,10.0\n"
        "C1,O2,not-a-date,1,5.0\n"          # invalid date -> excluded
        ",O3,2024-01-05 00:00:00,1,5.0\n"   # blank customer -> excluded
    )
    path = write_eligible_csv(tmp_path, body)
    transactions, cust = _agg(spark, path)
    assert transactions["transaction_count"] == 1
    assert cust["C1"]["total_spend"] == 20.0


# --- Isolation between two datasets --------------------------------------
def test_two_datasets_aggregate_independently(spark, tmp_path):
    a = write_eligible_csv(tmp_path, DATASET_A, "a.csv")
    b = write_eligible_csv(tmp_path,
                           "X9,BO1,2025-01-01 00:00:00,7,2.0\n", "b.csv")
    ta, ca = _agg(spark, a)
    tb, cb = _agg(spark, b)
    assert set(ca) == {"C1", "C2"}
    assert set(cb) == {"X9"}
    assert tb["total_spend"] == 14.0
    assert ta["distinct_customers"] == 2
