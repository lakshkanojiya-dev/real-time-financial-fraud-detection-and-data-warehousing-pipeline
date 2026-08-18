from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, avg, max, min, sum, when

# Configuration
TRANSACTIONS_PATH = "hdfs://localhost:9000/fraud_project/output/transactions"
BATCH_OUTPUT_PATH = "hdfs://localhost:9000/fraud_project/batch/daily_aggregates"
HIVE_TABLE_NAME = "daily_fraud_aggregates"

def main():
    spark = SparkSession.builder \
        .appName("BatchFraudAggregation") \
        .config("spark.sql.shuffle.partitions", "4") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[BATCH] Reading transactions from {TRANSACTIONS_PATH}...")
    transactions_df = spark.read.parquet(TRANSACTIONS_PATH)

    transactions_df = transactions_df.filter(col("event_ts").isNotNull())

    daily_agg = transactions_df \
        .withColumn("transaction_date", to_date(col("event_ts"))) \
        .groupBy("transaction_date") \
        .agg(
            count("*").alias("total_transactions"),
            sum(when(col("Class") == 1, 1).otherwise(0)).alias("total_fraud_transactions"),
            avg("Amount").alias("avg_amount"),
            max("Amount").alias("max_amount"),
            min("Amount").alias("min_amount"),
            sum(when(col("Risk_Action") == "BLOCK_CARD", 1).otherwise(0)).alias("total_blocked"),
            sum(when(col("Risk_Action") == "TRIGGER_OTP", 1).otherwise(0)).alias("total_trigger_otp")
        ) \
        .orderBy("transaction_date")

    print("[BATCH] Sample of computed daily aggregates:")
    daily_agg.show(10, truncate=False)

    print(f"[BATCH] Writing results to {BATCH_OUTPUT_PATH}")
    daily_agg.write \
        .mode("overwrite") \
        .partitionBy("transaction_date") \
        .parquet(BATCH_OUTPUT_PATH)

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {HIVE_TABLE_NAME} (
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
        LOCATION '{BATCH_OUTPUT_PATH}'
    """)

    spark.sql(f"MSCK REPAIR TABLE {HIVE_TABLE_NAME}")

    print("[BATCH] Completed successfully.")

    spark.stop()

if __name__ == "__main__":
    main()
