from typing import Dict
from ingestion_py.sinks.kafka_sink import KafkaSink

class StatusSink:
    def __init__(self, broker: str, topic: str = "flights.status"):
        self._sink = KafkaSink(broker, topic)

    def send(self, status: Dict):
        self._sink.send(status)
