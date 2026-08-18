#!/bin/bash
# Repairs Hive partitions for streaming and batch tables
echo "Repairing Hive partitions at $(date)"
hive -e "
MSCK REPAIR TABLE transactions_stream;
MSCK REPAIR TABLE daily_fraud_aggregates;
"
if [ $? -eq 0 ]; then
    echo "Partitions repaired successfully."
else
    echo "Partition repair failed."
    exit 1
fi
