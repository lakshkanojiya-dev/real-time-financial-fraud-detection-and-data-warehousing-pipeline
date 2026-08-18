from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, to_timestamp, udf
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler

# Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "financial-transactions"
MODEL_PATH = "hdfs://localhost:9000/fraud_project/models/spark_rf_model"
HDFS_OUTPUT_PATH = "hdfs://localhost:9000/fraud_project/output/transactions"
CHECKPOINT_PATH = "hdfs://localhost:9000/fraud_project/checkpoints/streaming"

def main():
    print("[INIT] Starting Spark Structured Streaming job...")
    spark = SparkSession.builder \
        .appName("FraudDetectionStreaming") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Define the schema for incoming JSON (matches enhanced producer)
    feature_fields = [StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)]
    schema = StructType([
        StructField("Time", DoubleType(), True),
        StructField("Amount", DoubleType(), True),
        StructField("Class", IntegerType(), True),
        StructField("eventTime", StringType(), True)
    ] + feature_fields)

    # Load the pre-trained Spark ML pipeline model from HDFS
    print(f"[MODEL] Loading model from {MODEL_PATH}...")
    model = PipelineModel.load(MODEL_PATH)
    print("[SUCCESS] Model loaded.")

    # Read streaming data from Kafka
    print(f"[STREAM] Connecting to Kafka topic '{KAFKA_TOPIC}'...")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON and extract fields
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema).alias("data")) \
        .select("data.*")

    # Convert eventTime string to timestamp (for future windowing)
    parsed_df = parsed_df.withColumn("event_ts", to_timestamp(col("eventTime")))

    # Assemble features for ML model (required by pipeline)
    feature_cols = [f"V{i}" for i in range(1, 29)] + ["Amount"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    parsed_df = assembler.transform(parsed_df)

    # Apply the ML model to get predictions
    predictions = model.transform(parsed_df)

    # Extract probability of fraud (class 1) from the probability vector
    @udf("double")
    def get_fraud_prob(v):
        return float(v[1]) if v is not None else None

    predictions = predictions.withColumn("Fraud_Probability", get_fraud_prob(col("probability")))

    # Classify risk based on probability thresholds
    processed_df = predictions.withColumn(
        "Risk_Action",
        when(col("Fraud_Probability") >= 0.7, "BLOCK_CARD")
        .when(col("Fraud_Probability") >= 0.4, "TRIGGER_OTP")
        .otherwise("ALLOW")
    )

    # Select final columns for output
    output_df = processed_df.select(
        "Time", "Amount", "Class", "eventTime", "event_ts",
        *feature_cols,
        "Fraud_Probability", "Risk_Action"
    )

    # Write to HDFS as Parquet, partitioned by Risk_Action
    print(f"[OUTPUT] Writing to HDFS at {HDFS_OUTPUT_PATH}")
    query = output_df.writeStream \
        .format("parquet") \
        .partitionBy("Risk_Action") \
        .option("path", HDFS_OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    print("[RUNNING] Stream processing active. Waiting for termination...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
