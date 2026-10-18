
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from src.data_quality import validate_transactions


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("TrustGuardTests")
        .master("local[1]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_df(spark):
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("transaction_date", StringType(), True),
    ])

    rows = [
        ("TX001", "C001", 2, 100.0, "2026-10-01"),
        (None, "C002", 1, 50.0, "2026-10-02"),
        ("TX003", None, 1, 75.0, "2026-10-03"),
        ("TX004", "C004", 0, 20.0, "2026-10-04"),
        ("TX005", "C005", 2, -10.0, "2026-10-05"),
        ("TX006", "C006", 1, 30.0, None),
        ("TX007", "C007", 1, 40.0, "2026-10-06"),
        ("TX007", "C007", 1, 40.0, "2026-10-06"),
    ]

    return spark.createDataFrame(rows, schema)


def test_clean_and_rejected_record_counts(sample_df):
    clean_df, rejected_df = validate_transactions(sample_df)

    assert clean_df.count() == 2
    assert rejected_df.count() == 6


def test_valid_records_have_no_rejection_reason(sample_df):
    clean_df, _ = validate_transactions(sample_df)

    assert "rejection_reason" not in clean_df.columns
    assert clean_df.filter("transaction_id = 'TX001'").count() == 1


def test_missing_transaction_id_is_rejected(sample_df):
    _, rejected_df = validate_transactions(sample_df)

    result = rejected_df.filter(
        "transaction_id IS NULL"
    ).first()

    assert result is not None
    assert "Missing transaction ID" in result["rejection_reason"]


def test_missing_customer_id_is_rejected(sample_df):
    _, rejected_df = validate_transactions(sample_df)

    result = rejected_df.filter(
        "transaction_id = 'TX003'"
    ).first()

    assert result is not None
    assert "Missing customer ID" in result["rejection_reason"]


def test_invalid_quantity_is_rejected(sample_df):
    _, rejected_df = validate_transactions(sample_df)

    result = rejected_df.filter(
        "transaction_id = 'TX004'"
    ).first()

    assert result is not None
    assert "Invalid quantity" in result["rejection_reason"]


def test_negative_price_is_rejected(sample_df):
    _, rejected_df = validate_transactions(sample_df)

    result = rejected_df.filter(
        "transaction_id = 'TX005'"
    ).first()

    assert result is not None
    assert "Invalid unit price" in result["rejection_reason"]


def test_missing_date_is_rejected(sample_df):
    _, rejected_df = validate_transactions(sample_df)

    result = rejected_df.filter(
        "transaction_id = 'TX006'"
    ).first()

    assert result is not None
    assert "Invalid transaction date" in result["rejection_reason"]


def test_duplicate_transaction_is_rejected(sample_df):
    clean_df, rejected_df = validate_transactions(sample_df)

    assert clean_df.filter(
        "transaction_id = 'TX007'"
    ).count() == 1

    duplicate = rejected_df.filter(
        "transaction_id = 'TX007'"
    ).first()

    assert duplicate is not None
    assert "Duplicate transaction ID" in duplicate["rejection_reason"]


def test_record_count_reconciliation(sample_df):
    clean_df, rejected_df = validate_transactions(sample_df)

    assert sample_df.count() == clean_df.count() + rejected_df.count()
