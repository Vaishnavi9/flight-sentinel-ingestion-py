# ingestion_py/main.py

from ingestion_py.config import Settings
from ingestion_py.orchestrator import Orchestrator

def run_autonomous():
    """
    Runs OpenSky ingestion in autonomous mode:
    - Polls /states/all every 5 min (live telemetry within NL/DE bbox).
    - Polls /flights/arrival for yesterday’s arrivals every 15 min.
    - Publishes:
        external.opensky.states -> raw live aircraft states
        flights.raw             -> normalized flight status (arrivals)
        flights.status          -> ingestion health/status events
    """
    settings = Settings()

    # bounding box roughly NL/DE region (lamin, lamax, lomin, lomax)
    bbox = (50.0, 54.0, 3.0, 10.5)

    # Schiphol airport (Amsterdam)
    airport = "EHAM"

    orch = Orchestrator(
        broker=settings.kafka_broker,
        bbox=bbox,
        airport_icao=airport,
        live_interval_sec=30,   # every 5 min
        arrival_interval_sec=60 # every 15 min
    )
    orch.run()


if __name__ == "__main__":
    run_autonomous()
