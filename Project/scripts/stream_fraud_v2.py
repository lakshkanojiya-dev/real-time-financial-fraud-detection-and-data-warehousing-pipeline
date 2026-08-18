import uuid
import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, to_timestamp, window, count, avg, lit, udf
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler

# Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "financial-transactions"
MODEL_PATH = "hdfs://localhost:9000/fraud_project/models/spark_rf_model"
HDFS_TRANSACTIONS_PATH = "hdfs://localhost:9000/fraud_project/output/transactions"
HDFS_AGGREGATES_PATH = "hdfs://localhost:9000/fraud_project/output/aggregates"
CHECKPOINT_TRANSACTIONS = "hdfs://localhost:9000/fraud_project/checkpoints/transactions"
CHECKPOINT_AGGREGATES = "hdfs://localhost:9000/fraud_project/checkpoints/aggregates"
HBASE_HOST = "localhost"
HBASE_TABLE = "fraud_alerts"

def write_to_hbase(df, epoch_id):
    """
    foreachBatch function to write high-risk records to HBase.
    """
    if df.isEmpty():
        return
    connection = happybase.Connection(HBASE_HOST)
    table = connection.table(HBASE_TABLE)
    rows = df.collect()
    batch = table.batch()
    for row in rows:
        row_key = f"{row['eventTime']}_{uuid.uuid4()}"
        data = {
            b'cf:Amount': str(row['Amount']).encode(),
            b'cf:Fraud_Probability': str(row['Fraud_Probability']).encode(),
            b'cf:Risk_Action': row['Risk_Action'].encode(),
            b'cf:eventTime': row['eventTime'].encode(),
        }
        batch.put(row_key.encode(), data)
    batch.send()
    connection.close()

def main():
    spark = SparkSession.builder \
        .appName("FraudDetectionStreamingEnhanced") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Define schema for incoming JSON
    feature_fields = [StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)]
    schema = StructType([
        StructField("Time", DoubleType(), True),
        StructField("Amount", DoubleType(), True),
        StructField("Class", IntegerType(), True),
        StructField("eventTime", StringType(), True)
    ] + feature_fields)

    # Load pre-trained ML model
    print(f"[MODEL] Loading model from {MODEL_PATH}...")
    model = PipelineModel.load(MODEL_PATH)

    # Read stream from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON and convert eventTime to timestamp
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_ts", to_timestamp(col("eventTime")))

    # Assemble features for ML model
    feature_cols = [f"V{i}" for i in range(1, 29)] + ["Amount"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    parsed_df = assembler.transform(parsed_df)

    # Apply ML model to get predictions
    predictions = model.transform(parsed_df)

    # Extract fraud probability
    @udf("double")
    def get_fraud_prob(v):
        return float(v[1]) if v is not None else None
    predictions = predictions.withColumn("Fraud_Probability", get_fraud_prob(col("probability")))

    # Risk classification
    processed_df = predictions.withColumn(
        "Risk_Action",
        when(col("Fraud_Probability") >= 0.7, "BLOCK_CARD")
        .when(col("Fraud_Probability") >= 0.4, "TRIGGER_OTP")
        .otherwise("ALLOW")
    )

    # ------------------------------------------------------------
    # Output 1: All transactions to HDFS (for Hive)
    # ------------------------------------------------------------
    output_transactions = processed_df.select(
        "Time", "Amount", "Class", "eventTime", "event_ts",
        *feature_cols, "Fraud_Probability", "Risk_Action"
    )

    transactions_query = output_transactions.writeStream \
        .format("parquet") \
        .partitionBy("Risk_Action") \
        .option("path", HDFS_TRANSACTIONS_PATH) \
        .option("checkpointLocation", CHECKPOINT_TRANSACTIONS) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    # ------------------------------------------------------------
    # Output 2: High-risk alerts to HBase (BLOCK_CARD and TRIGGER_OTP)
    # ------------------------------------------------------------
    high_risk_df = processed_df.filter(
        col("Risk_Action").isin("BLOCK_CARD", "TRIGGER_OTP")
    ).select(
        "eventTime", "Amount", "Fraud_Probability", "Risk_Action"
    )

    hbase_query = high_risk_df.writeStream \
        .foreachBatch(write_to_hbase) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    # ------------------------------------------------------------
    # Output 3: Sliding window aggregates for anomaly detection
    # ------------------------------------------------------------
    windowed_df = processed_df \
        .withWatermark("event_ts", "10 minutes") \
        .groupBy(window("event_ts", "10 minutes", "5 minutes")) \
        .agg(
            count("*").alias("total_txn"),
            avg("Amount").alias("avg_amount"),
            count(when(col("Class") == 1, True)).alias("fraud_count")
        )

    windowed_df = windowed_df.withColumn(
        "is_anomaly",
        when((col("total_txn") > 1000) | (col("avg_amount") > 500), lit(True)).otherwise(lit(False))
    )

    aggregates_query = windowed_df.writeStream \
        .format("parquet") \
        .option("path", HDFS_AGGREGATES_PATH) \
        .option("checkpointLocation", CHECKPOINT_AGGREGATES) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    print("[RUNNING] All streams started. Waiting for termination...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
