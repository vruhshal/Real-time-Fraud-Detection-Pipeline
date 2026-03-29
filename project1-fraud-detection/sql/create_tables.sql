-- ─────────────────────────────────────────────────────────────────────────────
-- create_tables.sql
-- BigQuery DDL for the fraud detection pipeline
-- Run: bq query --use_legacy_sql=false < create_tables.sql
-- ─────────────────────────────────────────────────────────────────────────────

-- Raw transactions table (partitioned + clustered for 4x speedup)
CREATE TABLE IF NOT EXISTS `your_project.fraud_detection.raw_transactions`
(
    transaction_id        STRING    NOT NULL,
    card_id               STRING    NOT NULL,
    customer_id           STRING,
    amount                NUMERIC,
    currency              STRING,
    merchant_name         STRING,
    merchant_city         STRING,
    merchant_country      STRING,
    transaction_timestamp TIMESTAMP NOT NULL,
    transaction_type      STRING,
    is_fraud              INT64,
    card_present          BOOL,
    device_id             STRING,
    _loaded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(transaction_timestamp)
CLUSTER BY card_id, merchant_country
OPTIONS (
    description           = 'Raw card transactions — partitioned by date, clustered by card_id',
    partition_expiration_days = 730,
    require_partition_filter  = FALSE
);


-- Engineered features table
CREATE TABLE IF NOT EXISTS `your_project.fraud_detection.transaction_features`
(
    transaction_id      STRING    NOT NULL,
    card_id             STRING    NOT NULL,
    customer_id         STRING,
    amount              NUMERIC,
    merchant_country    STRING,
    transaction_timestamp TIMESTAMP,
    transaction_type    STRING,
    -- rolling counts
    txn_count_1h        INT64,
    txn_count_24h       INT64,
    txn_count_10min     INT64,
    -- rolling amounts
    avg_amount_7d       NUMERIC,
    avg_amount_30d      NUMERIC,
    max_amount_24h      NUMERIC,
    sum_amount_24h      NUMERIC,
    -- derived
    zscore_amount       FLOAT64,
    txn_per_minute      FLOAT64,
    -- flags
    is_night_txn        INT64,
    is_foreign_merchant INT64,
    is_high_amount      INT64,
    is_velocity_spike   INT64,
    is_amount_anomaly   INT64,
    -- label
    is_fraud            INT64,
    _feature_date       DATE,
    _loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY _feature_date
CLUSTER BY card_id
OPTIONS (
    description = 'Engineered fraud features — partitioned by feature date'
);


-- Fraud alerts table (scored results)
CREATE TABLE IF NOT EXISTS `your_project.fraud_detection.fraud_alerts`
(
    alert_id              STRING    NOT NULL,
    transaction_id        STRING    NOT NULL,
    card_id               STRING    NOT NULL,
    amount                NUMERIC,
    merchant_name         STRING,
    merchant_country      STRING,
    transaction_timestamp TIMESTAMP,
    fraud_score           FLOAT64,
    fraud_flags           ARRAY<STRING>,
    alert_severity        STRING,   -- LOW / MEDIUM / HIGH
    is_resolved           BOOL      DEFAULT FALSE,
    resolved_by           STRING,
    resolved_at           TIMESTAMP,
    _created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(transaction_timestamp)
CLUSTER BY alert_severity, card_id;


-- ─────────────────────────────────────────────────────────────────────────────
-- Analytical queries
-- ─────────────────────────────────────────────────────────────────────────────

-- Q1: Daily fraud summary
SELECT
    DATE(transaction_timestamp)   AS txn_date,
    COUNT(*)                      AS total_transactions,
    SUM(is_fraud)                 AS fraud_count,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS fraud_rate_pct,
    ROUND(SUM(IF(is_fraud=1, amount, 0)), 2) AS fraud_amount_inr
FROM `your_project.fraud_detection.raw_transactions`
WHERE DATE(transaction_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 1
ORDER BY 1 DESC;


-- Q2: High-risk cards (multiple fraud flags in 24h)
SELECT
    card_id,
    COUNT(*)                AS suspicious_txns,
    SUM(is_amount_anomaly)  AS amount_anomalies,
    SUM(is_velocity_spike)  AS velocity_spikes,
    SUM(is_foreign_merchant) AS foreign_txns,
    MAX(zscore_amount)      AS max_zscore
FROM `your_project.fraud_detection.transaction_features`
WHERE _feature_date = CURRENT_DATE() - 1
GROUP BY card_id
HAVING suspicious_txns >= 3
ORDER BY suspicious_txns DESC
LIMIT 100;


-- Q3: Fraud by merchant country
SELECT
    merchant_country,
    COUNT(*)              AS total_txns,
    SUM(is_fraud)         AS fraud_count,
    ROUND(SUM(is_fraud)/COUNT(*)*100, 2) AS fraud_rate_pct,
    ROUND(AVG(amount), 2) AS avg_txn_amount
FROM `your_project.fraud_detection.raw_transactions`
GROUP BY merchant_country
ORDER BY fraud_rate_pct DESC;


-- Q4: Alert resolution rate (SLA monitoring)
SELECT
    DATE(_created_at)        AS alert_date,
    alert_severity,
    COUNT(*)                 AS total_alerts,
    COUNTIF(is_resolved)     AS resolved_alerts,
    ROUND(COUNTIF(is_resolved)/COUNT(*)*100, 1) AS resolution_rate_pct
FROM `your_project.fraud_detection.fraud_alerts`
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
