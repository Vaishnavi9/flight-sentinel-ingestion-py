import json
from typing import Dict
from kafka import KafkaProducer

class KafkaDLQ:
    """Sends bad records to a DLQ topic with an error reason."""

    def __init__(self, broker: str, dlq_topic: str):
        self.topic = dlq_topic
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=10,
            retries=3,
        )

    def send(self, bad_payload: Dict, error: str):
        self.producer.send(self.topic, {"error": error, "payload": bad_payload})
        self.producer.flush()
