#!/bin/bash
# Basic cluster health checks for HDFS, Kafka, and HBase
echo "=== Cluster Health Check at $(date) ==="

# Check HDFS
echo "Checking HDFS..."
hdfs dfsadmin -report > /tmp/hdfs_report.txt 2>&1
if [ $? -eq 0 ]; then
    echo "HDFS is healthy."
    grep "Live datanodes" /tmp/hdfs_report.txt
else
    echo "HDFS check failed."
fi

# Check Kafka (assumes broker on localhost:9092)
echo "Checking Kafka..."
if nc -z localhost 9092; then
    echo "Kafka broker is reachable."
else
    echo "Kafka broker is NOT reachable."
fi

# Check HBase (check if HMaster is running)
echo "Checking HBase..."
if jps | grep -q HMaster; then
    echo "HBase Master is running."
else
    echo "HBase Master is NOT running."
fi

echo "=== Health check completed ==="
