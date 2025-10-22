import json
from typing import Dict
from kafka import KafkaProducer

class KafkaSink:
    """Sends normalized events to a Kafka topic as JSON."""

    def __init__(self, broker: str, topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=10,
            retries=3,
        )

    def send(self, payload: Dict):
        """Send one message and flush (simple & reliable for a PoC)."""
        self.producer.send(self.topic, payload)
        self.producer.flush()
