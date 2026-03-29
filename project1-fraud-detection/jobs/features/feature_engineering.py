"""
feature_engineering.py
PySpark job that reads raw transactions and engineers fraud detection features.
Run locally: spark-submit feature_engineering.py --input transactions.json --output features/
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


def create_spark_session():
    return (
        SparkSession.builder.appName("FraudFeatureEngineering")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "50")
        .getOrCreate()
    )


def load_data(spark, input_path: str):
    """Load raw transaction JSON into a Spark DataFrame."""
    df = spark.read.option("multiline", "true").json(input_path)
    df = df.withColumn(
        "transaction_timestamp", F.to_timestamp("transaction_timestamp")
    )
    print(f"Loaded {df.count()} transactions")
    df.printSchema()
    return df


def engineer_features(df):
    """
    Compute fraud detection features per card:
    - Rolling transaction counts (1h, 24h)
    - Rolling amount statistics
    - Velocity (txn per minute)
    - Z-score of transaction amount vs 30-day history
    - Flags: night transaction, foreign merchant, high amount
    """

    # ── Window specs ──────────────────────────────────────────────────
    w_card = Window.partitionBy("card_id").orderBy("transaction_timestamp_unix")

    w_1h = w_card.rangeBetween(-3600, 0)        # 1-hour rolling window
    w_24h = w_card.rangeBetween(-86400, 0)       # 24-hour rolling window
    w_7d = w_card.rangeBetween(-604800, 0)       # 7-day rolling window
    w_30d = w_card.rangeBetween(-2592000, 0)     # 30-day rolling window
    w_10min = w_card.rangeBetween(-600, 0)       # 10-minute rolling window

    df = df.withColumn(
        "transaction_timestamp_unix",
        F.unix_timestamp("transaction_timestamp"),
    )

    # ── Rolling counts ────────────────────────────────────────────────
    df = df.withColumn("txn_count_1h", F.count("transaction_id").over(w_1h))
    df = df.withColumn("txn_count_24h", F.count("transaction_id").over(w_24h))
    df = df.withColumn("txn_count_10min", F.count("transaction_id").over(w_10min))

    # ── Rolling amount stats ──────────────────────────────────────────
    df = df.withColumn("avg_amount_7d", F.avg("amount").over(w_7d))
    df = df.withColumn("avg_amount_30d", F.avg("amount").over(w_30d))
    df = df.withColumn("stddev_amount_30d", F.stddev("amount").over(w_30d))
    df = df.withColumn("max_amount_24h", F.max("amount").over(w_24h))
    df = df.withColumn("sum_amount_24h", F.sum("amount").over(w_24h))

    # ── Z-score: how unusual is this transaction amount? ──────────────
    df = df.withColumn(
        "zscore_amount",
        F.when(
            F.col("stddev_amount_30d") > 0,
            (F.col("amount") - F.col("avg_amount_30d")) / F.col("stddev_amount_30d"),
        ).otherwise(0.0),
    )

    # ── Velocity: transactions per minute (10-min window) ─────────────
    df = df.withColumn(
        "txn_per_minute",
        F.round(F.col("txn_count_10min") / 10.0, 2),
    )

    # ── Binary flags ──────────────────────────────────────────────────
    df = df.withColumn(
        "is_night_txn",
        F.when(F.hour("transaction_timestamp").between(0, 5), 1).otherwise(0),
    )
    df = df.withColumn(
        "is_foreign_merchant",
        F.when(F.col("merchant_country") != "IN", 1).otherwise(0),
    )
    df = df.withColumn(
        "is_high_amount",
        F.when(F.col("amount") > 50000, 1).otherwise(0),
    )
    df = df.withColumn(
        "is_velocity_spike",
        F.when(F.col("txn_per_minute") > 3, 1).otherwise(0),
    )
    df = df.withColumn(
        "is_amount_anomaly",
        F.when(F.col("zscore_amount") > 3.0, 1).otherwise(0),
    )

    # ── Select final feature set ──────────────────────────────────────
    feature_cols = [
        "transaction_id", "card_id", "customer_id",
        "amount", "merchant_country", "transaction_timestamp",
        "transaction_type",
        # rolling counts
        "txn_count_1h", "txn_count_24h", "txn_count_10min",
        # rolling amounts
        "avg_amount_7d", "avg_amount_30d", "max_amount_24h", "sum_amount_24h",
        # derived
        "zscore_amount", "txn_per_minute",
        # flags
        "is_night_txn", "is_foreign_merchant", "is_high_amount",
        "is_velocity_spike", "is_amount_anomaly",
        # label
        "is_fraud",
    ]

    return df.select(feature_cols)


def validate_features(df):
    """Basic data quality checks before writing output."""
    total = df.count()
    nulls = df.filter(F.col("transaction_id").isNull()).count()
    neg_amounts = df.filter(F.col("amount") < 0).count()

    print(f"\n── Feature validation ──────────────────")
    print(f"  Total rows    : {total}")
    print(f"  Null txn_ids  : {nulls}")
    print(f"  Negative amts : {neg_amounts}")
    print(f"  Fraud rate    : {round(df.filter(F.col('is_fraud')==1).count()/total*100,2)}%")

    assert nulls == 0, "Null transaction_ids found!"
    assert neg_amounts == 0, "Negative amounts found!"
    print("  All checks passed.\n")


def write_features(df, output_path: str):
    """Write features as Parquet, partitioned by date."""
    df = df.withColumn("partition_date", F.to_date("transaction_timestamp"))
    (
        df.write.mode("overwrite")
        .partitionBy("partition_date")
        .parquet(output_path)
    )
    print(f"Features written to {output_path}")


def main(input_path: str, output_path: str):
    spark = create_spark_session()
    df_raw = load_data(spark, input_path)
    df_features = engineer_features(df_raw)
    validate_features(df_features)
    write_features(df_features, output_path)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to raw transactions JSON")
    parser.add_argument("--output", required=True, help="Output path for features Parquet")
    args = parser.parse_args()
    main(args.input, args.output)
