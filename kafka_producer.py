import json

from kafka import KafkaProducer
from datetime import datetime

producer=KafkaProducer(
    bootstrap_servers="localhost:9092",

    value_serializer=lambda v:json.dumps(v).encode("utf-8")
)

def send_event(role,english_query,sql,status):
    event={
        "role":role,
        "english_query":english_query,
        "sql":sql,
        "status":status,
        "timestamp":datetime.utcnow().isoformat()
    }

    producer.send("query_logs",event)
    producer.flush()

