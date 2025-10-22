import argparse, time
from pathlib import Path
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion_py.config import Settings
from ingestion_py.sources.csv_source import read_csv
from ingestion_py.transforms.normalizer import normalize
from ingestion_py.sinks.kafka_sink import KafkaSink
from ingestion_py.dlq.kafka_dlq import KafkaDLQ
from ingestion_py.metrics import start_metrics, records_total, errors_total, last_sent_ts

load_dotenv()  # optional .env support

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=8))
def publish_record(sink: KafkaSink, payload: dict):
    """Retry sending a record if Kafka is briefly unavailable."""
    sink.send(payload)

def run(csv_path: str, settings: Settings):
    # Start metrics endpoint (e.g., http://localhost:9108/metrics)
    start_metrics(settings.metrics_port)

    sink = KafkaSink(settings.kafka_broker, settings.topic_raw)
    dlq = KafkaDLQ(settings.kafka_broker, settings.topic_dlq)

    for row in read_csv(csv_path):
        try:
            payload = normalize(row)
            publish_record(sink, payload)
            records_total.inc()
            last_sent_ts.set_to_current_time()
            print("->", payload)
            time.sleep(settings.sleep_secs)  # throttle to look like "live" data
        except Exception as e:
            errors_total.inc()
            dlq.send(row, str(e))
            print("DLQ:", row, "ERR:", e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument("--topic", required=False, help="Override topic (else env TOPIC_RAW)")
    args = parser.parse_args()

    csv_path = str(Path(args.csv).expanduser().resolve())
    settings = Settings()
    if args.topic:
        settings.topic_raw = args.topic

    run(csv_path, settings)
