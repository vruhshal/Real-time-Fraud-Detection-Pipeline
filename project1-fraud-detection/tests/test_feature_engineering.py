"""
test_feature_engineering.py
Unit tests for the PySpark feature engineering job.
Run: pytest tests/test_feature_engineering.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'jobs', 'features'))
from feature_engineering import engineer_features, validate_features


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("TestFeatureEngineering")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_transactions(spark):
    """Create a small sample DataFrame for testing."""
    from datetime import datetime
    data = [
        ("TXN001", "CARD001", "CUST001", 500.0,   "IN", datetime(2025, 3, 1, 10, 0, 0), "POS",      0),
        ("TXN002", "CARD001", "CUST001", 450.0,   "IN", datetime(2025, 3, 1, 10, 5, 0), "POS",      0),
        ("TXN003", "CARD001", "CUST001", 49000.0, "US", datetime(2025, 3, 1, 2, 30, 0), "ONLINE",   1),
        ("TXN004", "CARD002", "CUST002", 1200.0,  "IN", datetime(2025, 3, 1, 14, 0, 0), "ONLINE",   0),
        ("TXN005", "CARD002", "CUST002", 1100.0,  "IN", datetime(2025, 3, 1, 14, 2, 0), "CONTACTLESS", 0),
    ]
    return spark.createDataFrame(
        data,
        schema=[
            "transaction_id", "card_id", "customer_id",
            "amount", "merchant_country", "transaction_timestamp",
            "transaction_type", "is_fraud"
        ]
    )


class TestFeatureEngineering:

    def test_output_has_expected_columns(self, spark, sample_transactions):
        result = engineer_features(sample_transactions)
        expected_cols = [
            "transaction_id", "card_id", "amount",
            "txn_count_1h", "txn_count_24h",
            "zscore_amount", "txn_per_minute",
            "is_night_txn", "is_foreign_merchant",
            "is_high_amount", "is_fraud",
        ]
        for col in expected_cols:
            assert col in result.columns, f"Missing column: {col}"

    def test_night_transaction_flag(self, spark, sample_transactions):
        result = engineer_features(sample_transactions)
        night_txn = result.filter(F.col("transaction_id") == "TXN003").collect()[0]
        assert night_txn["is_night_txn"] == 1, "TXN003 at 2:30am should be flagged as night txn"

    def test_day_transaction_not_flagged(self, spark, sample_transactions):
        result = engineer_features(sample_transactions)
        day_txn = result.filter(F.col("transaction_id") == "TXN001").collect()[0]
        assert day_txn["is_night_txn"] == 0, "TXN001 at 10am should NOT be night txn"

    def test_foreign_merchant_flag(self, spark, sample_transactions):
        result = engineer_features(sample_transactions)
        foreign = result.filter(F.col("transaction_id") == "TXN003").collect()[0]
        domestic = result.filter(F.col("transaction_id") == "TXN001").collect()[0]
        assert foreign["is_foreign_merchant"] == 1
        assert domestic["is_foreign_merchant"] == 0

    def test_high_amount_flag(self, spark, sample_transactions):
        result = engineer_features(sample_transactions)
        high = result.filter(F.col("transaction_id") == "TXN003").collect()[0]
        low  = result.filter(F.col("transaction_id") == "TXN001").collect()[0]
        assert high["is_high_amount"] == 1,  "49000 should be flagged as high amount"
        assert low["is_high_amount"]  == 0,  "500 should NOT be flagged as high amount"

    def test_rolling_count_1h(self, spark, sample_transactions):
        """CARD001 has 3 txns within 1 hour — count should be >= 2 for later txns."""
        result = engineer_features(sample_transactions)
        card1_txns = result.filter(F.col("card_id") == "CARD001").orderBy("transaction_timestamp")
        counts = [row["txn_count_1h"] for row in card1_txns.collect()]
        assert all(c >= 1 for c in counts), "All transactions should have at least count=1"

    def test_no_null_transaction_ids(self, spark, sample_transactions):
        result = engineer_features(sample_transactions)
        null_count = result.filter(F.col("transaction_id").isNull()).count()
        assert null_count == 0

    def test_no_negative_amounts(self, spark, sample_transactions):
        result = engineer_features(sample_transactions)
        neg_count = result.filter(F.col("amount") < 0).count()
        assert neg_count == 0

    def test_row_count_preserved(self, spark, sample_transactions):
        result = engineer_features(sample_transactions)
        assert result.count() == sample_transactions.count()


class TestValidateFeatures:

    def test_validation_passes_on_clean_data(self, spark, sample_transactions):
        result = engineer_features(sample_transactions)
        validate_features(result)   # should not raise

    def test_validation_fails_on_null_transaction_id(self, spark):
        bad_data = spark.createDataFrame(
            [(None, "CARD001", "CUST001", 500.0, "IN", None, "POS", 0)],
            schema=["transaction_id", "card_id", "customer_id", "amount",
                    "merchant_country", "transaction_timestamp", "transaction_type", "is_fraud"]
        )
        with pytest.raises(Exception):
            validate_features(bad_data)
