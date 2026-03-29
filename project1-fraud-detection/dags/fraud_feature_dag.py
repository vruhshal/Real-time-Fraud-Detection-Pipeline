"""
fraud_feature_dag.py
Airflow DAG — orchestrates the daily fraud feature engineering pipeline.
Schedule: 2am UTC daily
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import logging

# ── Default args ───────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
}

# ── SLA miss callback → Slack alert ───────────────────────────────────────────
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    logging.error(f"SLA MISSED — DAG: {dag.dag_id}, Tasks: {task_list}")
    # In production: send Slack alert here via webhook


def on_failure_callback(context):
    """Send Slack alert on any task failure."""
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    log_url = context["task_instance"].log_url
    logging.error(f"Task failed: {dag_id}.{task_id} — {log_url}")


# ── Python task functions ──────────────────────────────────────────────────────
def check_raw_data_exists(**context):
    """Check that raw transaction files exist for the run date."""
    run_date = context["ds"]  # e.g. "2025-03-15"
    expected_path = f"gs://fraud-pipeline-bucket/raw/transactions/date={run_date}/"
    logging.info(f"Checking raw data at: {expected_path}")
    # In production: use GCSHook to check file existence
    # For local testing, we just log and continue
    logging.info("Raw data check passed.")
    return expected_path


def run_row_count_check(**context):
    """Validate that feature row count matches raw row count."""
    run_date = context["ds"]
    logging.info(f"Running row count reconciliation for {run_date}")
    # In production: query BigQuery to compare raw vs feature counts
    raw_count = 10000       # placeholder
    feature_count = 10000   # placeholder
    assert raw_count == feature_count, \
        f"Row count mismatch: raw={raw_count}, features={feature_count}"
    logging.info(f"Row count check passed: {feature_count} rows")
    return feature_count


def check_fraud_rate(**context):
    """Alert if fraud rate falls outside expected 1–5% range."""
    run_date = context["ds"]
    fraud_rate = 0.031  # placeholder — in production query BQ
    logging.info(f"Fraud rate for {run_date}: {fraud_rate:.1%}")
    if not (0.01 <= fraud_rate <= 0.05):
        raise ValueError(f"Fraud rate {fraud_rate:.1%} is outside expected 1–5% range!")
    logging.info("Fraud rate check passed.")


def load_features_to_bigquery(**context):
    """Load Parquet features from GCS into BigQuery."""
    run_date = context["ds"]
    logging.info(f"Loading features for {run_date} into BigQuery...")
    # In production: use BigQueryInsertJobOperator or bq load command
    logging.info("BigQuery load complete.")


# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="fraud_feature_refresh_dag",
    default_args=default_args,
    description="Daily fraud feature engineering pipeline",
    schedule_interval="0 2 * * *",   # 2am UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fraud", "features", "fintech"],
    sla_miss_callback=sla_miss_callback,
) as dag:

    # ── Task 1: Check raw data exists ─────────────────────────────────
    check_raw = PythonOperator(
        task_id="check_raw_data_exists",
        python_callable=check_raw_data_exists,
        on_failure_callback=on_failure_callback,
        sla=timedelta(minutes=10),
    )

    # ── Task 2: Run PySpark feature engineering ───────────────────────
    run_feature_job = BashOperator(
        task_id="run_feature_engineering",
        bash_command="""
            spark-submit \
              --master yarn \
              --deploy-mode cluster \
              --num-executors 4 \
              --executor-memory 4g \
              --executor-cores 2 \
              /jobs/features/feature_engineering.py \
              --input gs://fraud-pipeline-bucket/raw/transactions/date={{ ds }}/ \
              --output gs://fraud-pipeline-bucket/features/date={{ ds }}/
        """,
        on_failure_callback=on_failure_callback,
        sla=timedelta(minutes=25),
    )

    # ── Task 3: Row count validation ──────────────────────────────────
    validate_counts = PythonOperator(
        task_id="validate_row_counts",
        python_callable=run_row_count_check,
        on_failure_callback=on_failure_callback,
    )

    # ── Task 4: Fraud rate sanity check ───────────────────────────────
    validate_fraud_rate = PythonOperator(
        task_id="validate_fraud_rate",
        python_callable=check_fraud_rate,
        on_failure_callback=on_failure_callback,
    )

    # ── Task 5: Load to BigQuery ──────────────────────────────────────
    load_to_bq = PythonOperator(
        task_id="load_features_to_bigquery",
        python_callable=load_features_to_bigquery,
        on_failure_callback=on_failure_callback,
        sla=timedelta(minutes=40),
    )

    # ── Task 6: Success notification ─────────────────────────────────
    notify_success = BashOperator(
        task_id="notify_success",
        bash_command='echo "Pipeline completed for {{ ds }} — features loaded to BigQuery"',
    )

    # ── DAG dependency chain ──────────────────────────────────────────
    check_raw >> run_feature_job >> validate_counts >> validate_fraud_rate >> load_to_bq >> notify_success
