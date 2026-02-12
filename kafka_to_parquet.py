import json
import os
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer

# -----------------------------
# Kafka Consumer Configuration
# -----------------------------
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "datalake-consumer-v1",   # change group id if needed
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["query_logs"])

print("Kafka consumer started and listening to topic: query_logs")

# -----------------------------
# Data Lake Configuration
# -----------------------------
BATCH_SIZE = 1   # write immediately for testing
buffer = []

BASE_PATH = os.path.join(
    os.getcwd(), "datalake", "query_logs"
)

print("Data lake base path:", BASE_PATH)

# -----------------------------
# Consume Loop
# -----------------------------
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            print("No message yet...")
            continue

        if msg.error():
            print("Kafka error:", msg.error())
            continue

        # Decode Kafka message
        raw_value = msg.value().decode("utf-8")
        print("RAW MESSAGE:", raw_value)

        event = json.loads(raw_value)
        buffer.append(event)

        print("Buffer size:", len(buffer))

        # -----------------------------
        # Write to Data Lake (Parquet)
        # -----------------------------
        if len(buffer) >= BATCH_SIZE:
            df = pd.DataFrame(buffer)

            today = datetime.utcnow().strftime("%Y-%m-%d")
            output_path = os.path.join(
                BASE_PATH, f"date={today}"
            )
            os.makedirs(output_path, exist_ok=True)

            file_name = f"logs_{int(datetime.utcnow().timestamp())}.parquet"
            file_path = os.path.join(output_path, file_name)

            table = pa.Table.from_pandas(df)
            pq.write_table(table, file_path)

            print(f"✅ Written {len(buffer)} record(s) to:")
            print(file_path)

            buffer.clear()

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    consumer.close()
