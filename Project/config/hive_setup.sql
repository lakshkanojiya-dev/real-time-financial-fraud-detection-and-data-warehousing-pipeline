-- =====================================================
-- Hive Setup Script for Fraud Detection Project
-- Creates external tables over HDFS data written by
-- Spark streaming and batch jobs.
-- =====================================================

-- Drop tables if they exist (optional, for clean re-run)
DROP TABLE IF EXISTS transactions_stream;
DROP TABLE IF EXISTS daily_fraud_aggregates;

-- -----------------------------------------------------
-- 1. Streaming transactions table
--    Data location: /fraud_project/output/transactions
--    Partitioned by Risk_Action (ALLOW, TRIGGER_OTP, BLOCK_CARD)
-- -----------------------------------------------------
CREATE EXTERNAL TABLE transactions_stream (
    `Time` DOUBLE,
    Amount DOUBLE,
    Class INT,
    eventTime STRING,
    event_ts TIMESTAMP,
    V1 DOUBLE, V2 DOUBLE, V3 DOUBLE, V4 DOUBLE, V5 DOUBLE,
    V6 DOUBLE, V7 DOUBLE, V8 DOUBLE, V9 DOUBLE, V10 DOUBLE,
    V11 DOUBLE, V12 DOUBLE, V13 DOUBLE, V14 DOUBLE, V15 DOUBLE,
    V16 DOUBLE, V17 DOUBLE, V18 DOUBLE, V19 DOUBLE, V20 DOUBLE,
    V21 DOUBLE, V22 DOUBLE, V23 DOUBLE, V24 DOUBLE, V25 DOUBLE,
    V26 DOUBLE, V27 DOUBLE, V28 DOUBLE,
    Fraud_Probability DOUBLE
)
PARTITIONED BY (Risk_Action STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/fraud_project/output/transactions';

-- Repair partitions for streaming table
MSCK REPAIR TABLE transactions_stream;

-- -----------------------------------------------------
-- 2. Batch daily aggregates table
--    Data location: /fraud_project/batch/daily_aggregates
--    Partitioned by transaction_date (DATE)
-- -----------------------------------------------------
CREATE EXTERNAL TABLE daily_fraud_aggregates (
    total_transactions BIGINT,
    total_fraud_transactions BIGINT,
    avg_amount DOUBLE,
    max_amount DOUBLE,
    min_amount DOUBLE,
    total_blocked BIGINT,
    total_trigger_otp BIGINT
)
PARTITIONED BY (transaction_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/fraud_project/batch/daily_aggregates';

-- Repair partitions for batch table
MSCK REPAIR TABLE daily_fraud_aggregates;

-- -----------------------------------------------------
-- 3. (Optional) Create a combined view for easy analysis
-- -----------------------------------------------------
CREATE VIEW IF NOT EXISTS fraud_overview AS
SELECT
    t.event_ts,
    t.Amount,
    t.Class,
    t.Risk_Action,
    a.total_transactions,
    a.total_fraud_transactions,
    a.avg_amount
FROM transactions_stream t
LEFT JOIN daily_fraud_aggregates a
    ON to_date(t.event_ts) = a.transaction_date;
