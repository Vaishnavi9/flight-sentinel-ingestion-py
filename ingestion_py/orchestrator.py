from typing import Optional, Tuple
from ingestion_py.sinks.kafka_sink import KafkaSink
from ingestion_py.sinks.status_sink import StatusSink
from ingestion_py.sources.opensky_live import fetch_live_states
from ingestion_py.sources.opensky_arrivals import fetch_arrivals_yesterday

class Orchestrator:
    def __init__(
        self,
        broker: str,
        bbox: Optional[Tuple[float, float, float, float]],
        airport_icao: str,
        live_interval_sec: int = 300,
        arrival_interval_sec: int = 900,
    ):
        self.states_sink = KafkaSink(broker, "external.opensky.states")
        self.arrivals_sink = KafkaSink(broker, "flights.raw")
        self.status_sink = StatusSink(broker, "flights.status")

        self.bbox = bbox
        self.airport = airport_icao
        self.live_interval = live_interval_sec
        self.arrival_interval = arrival_interval_sec

    def run(self):
        # run both generators interleaved in a simple round-robin
        live_gen = fetch_live_states(self.bbox, self.live_interval)
        arr_gen  = fetch_arrivals_yesterday(self.airport, self.arrival_interval)

        while True:
            # Pull one event from each generator, publish accordingly
            for _ in range(2):
                evt = next(live_gen)
                if evt.get("event_type") == "provider_status":
                    self.status_sink.send(evt)
                else:
                    self.states_sink.send(evt)

            evt = next(arr_gen)
            if evt.get("event_type") == "provider_status":
                self.status_sink.send(evt)
            else:
                self.arrivals_sink.send(evt)
