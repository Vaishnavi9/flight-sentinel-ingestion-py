from prometheus_client import Counter, Gauge, start_http_server

# Counters and gauges visible at /metrics
records_total = Counter("fs_ingestion_records_total", "Total records ingested")
errors_total = Counter("fs_ingestion_errors_total", "Total ingestion errors")
last_sent_ts = Gauge("fs_ingestion_last_sent_timestamp", "Unix timestamp of last successful send")

def start_metrics(port: int):
    """Starts a tiny HTTP server exposing Prometheus metrics."""
    start_http_server(port)
