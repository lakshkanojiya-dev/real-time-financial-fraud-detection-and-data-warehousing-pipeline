#!/bin/bash
# Runs the batch ETL job using spark-submit
echo "Starting batch ETL job at $(date)"
spark-submit \
    --master local[*] \
    --driver-memory 4g \
    --executor-memory 4g \
    /home/sunbeam/fraud_project_enhanced/scripts/batch_etl.py
if [ $? -eq 0 ]; then
    echo "Batch ETL completed successfully."
else
    echo "Batch ETL failed. Check logs."
    exit 1
fi
