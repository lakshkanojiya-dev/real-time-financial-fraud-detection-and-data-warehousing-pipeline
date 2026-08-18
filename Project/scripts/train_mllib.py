from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import time

# Paths
DATASET_PATH = "/home/sunbeam/fraud_project/data/creditcard.csv"   # adjust if needed
MODEL_OUTPUT_PATH = "hdfs://localhost:9000/fraud_project/models/spark_rf_model"

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("FraudModelTraining") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("[1/5] Loading dataset...")
    df = spark.read.csv(DATASET_PATH, header=True, inferSchema=True)

    # Drop the 'Time' column (not useful as a feature for classification)
    feature_cols = [f"V{i}" for i in range(1, 29)] + ["Amount"]
    target_col = "Class"

    # Check for missing values (optional, dataset has none)
    # df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

    print("[2/5] Building feature vector and adding class weights...")
    # VectorAssembler to combine features
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # Compute class weights to handle imbalance
    total_count = df.count()
    fraud_count = df.filter(df[target_col] == 1).count()
    non_fraud_count = total_count - fraud_count
    print(f"      Total rows: {total_count}, Fraud: {fraud_count}, Non-fraud: {non_fraud_count}")

    # Weight for fraud = total / fraud_count, weight for non-fraud = total / non_fraud_count
    # This makes the total weight of each class equal.
    weight_fraud = total_count / fraud_count
    weight_non_fraud = total_count / non_fraud_count

    from pyspark.sql.functions import when, lit
    df = df.withColumn("weight",
                       when(df[target_col] == 1, lit(weight_fraud))
                       .otherwise(lit(weight_non_fraud)))

    # Random Forest classifier
    rf = RandomForestClassifier(
        labelCol=target_col,
        featuresCol="features",
        weightCol="weight",
        numTrees=100,
        maxDepth=10,
        seed=42,
        featureSubsetStrategy="auto"
    )

    # Build pipeline
    pipeline = Pipeline(stages=[assembler, rf])

    print("[3/5] Splitting data into train/test...")
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    print("[4/5] Training model...")
    start_time = time.time()
    model = pipeline.fit(train_df)
    print(f"      Training completed in {time.time() - start_time:.2f} seconds")

    # Evaluate on test set
    print("[5/5] Evaluating model...")
    predictions = model.transform(test_df)
    evaluator = BinaryClassificationEvaluator(
        labelCol=target_col,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    auc = evaluator.evaluate(predictions)
    print(f"      Test AUC: {auc:.4f}")

    # Save the pipeline model to HDFS
    model.write().overwrite().save(MODEL_OUTPUT_PATH)
    print(f"[SUCCESS] Model saved to {MODEL_OUTPUT_PATH}")

    spark.stop()

if __name__ == "__main__":
    main()
