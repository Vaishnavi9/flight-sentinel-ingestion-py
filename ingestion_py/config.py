import os
from pydantic import BaseModel

class Settings(BaseModel):
    """Holds configuration values, usually from environment variables."""
    kafka_broker: str = os.getenv("KAFKA_BROKER", "localhost:9092")
    topic_raw: str = os.getenv("TOPIC_RAW", "flights.raw")
    topic_dlq: str = os.getenv("TOPIC_DLQ", "flights.dlq")
    sleep_secs: float = float(os.getenv("SLEEP_SECS", "0.5"))
    metrics_port: int = int(os.getenv("METRICS_PORT", "9108"))
