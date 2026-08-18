import json
import time
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime

# Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "financial-transactions"
DATASET_PATH = "/home/sunbeam/fraud_project/data/creditcard.csv"  # adjust if needed

def main():
    print("[INIT] Initializing Kafka Producer for Fraud Detection...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"[SUCCESS] Connected to Kafka broker at {KAFKA_BROKER}")
    except Exception as e:
        print(f"[ERROR] Failed to connect: {e}")
        return

    print(f"[DATA] Loading dataset from {DATASET_PATH}...")
    try:
        df = pd.read_csv(DATASET_PATH)
    except FileNotFoundError:
        print(f"[ERROR] Dataset not found at {DATASET_PATH}")
        return

    # Re-order: inject some frauds at the beginning for immediate blocking
    fraud_df = df[df['Class'] == 1]
    normal_df = df[df['Class'] == 0]
    presentation_df = pd.concat([fraud_df.head(10), normal_df]).reset_index(drop=True)

    print(f"[PREPARATION] Total records to stream: {len(presentation_df)}")
    print("[STREAM] Starting live transmission loop...")
    counter = 0

    for index, row in presentation_df.iterrows():
        transaction_dict = row.to_dict()
        transaction_dict['Class'] = int(transaction_dict['Class'])
        # Add event time: current UTC timestamp in ISO 8601 format
        transaction_dict['eventTime'] = datetime.utcnow().isoformat() + 'Z'
        
        try:
            producer.send(TOPIC_NAME, value=transaction_dict)
            counter += 1
            
            if transaction_dict['Class'] == 1:
                print(f"--> [FRAUD INJECTED] Record {counter}: Amount=${transaction_dict['Amount']}")
            else:
                if counter % 500 == 0:
                    print(f"--> [NORMAL] Sent {counter} records")
            
            # Small delay to simulate real-time streaming (adjust as needed)
            time.sleep(0.01)
            
        except Exception as e:
            print(f"[ERROR] Failed at record {counter}: {e}")
            break

    producer.flush()
    print(f"[COMPLETED] Finished streaming {counter} records.")

if __name__ == "__main__":
    main()
