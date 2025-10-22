# ingestion_py/sources/opensky_airport_source.py
import time, requests
from typing import Iterator, Dict
from datetime import datetime, timezone

OS_API_BASE = "https://opensky-network.org/api"

def fetch_airport_arrivals_yesterday(airport: str, interval: int = 900) -> Iterator[Dict]:
    """
    Every `interval` seconds, fetch arrivals for the *previous day's* same window.
    Example: at 10:30Z today, query [09:45Z yesterday, 10:00Z yesterday].
    """
    while True:
        now = int(datetime.now(timezone.utc).timestamp())
        end = now - 24*3600              # shift to yesterday
        begin = end - interval

        try:
            r = requests.get(
                f"{OS_API_BASE}/flights/arrival",
                params={"airport": airport, "begin": begin, "end": end},
                timeout=15,
            )
            if r.status_code == 404:
                print(f"[opensky] no arrivals for {airport} in window {begin}-{end}")
                time.sleep(interval)
                continue
            r.raise_for_status()

            for f in r.json() or []:
                yield {
                    "event_type": "flight_status",
                    "provider": "opensky",
                    "ingested_at": datetime.utcnow().isoformat() + "Z",
                    "flight_id": f.get("icao24"),
                    "carrier": None,
                    "origin": f.get("estDepartureAirport"),
                    "destination": airport,
                    "scheduled_dep": None,
                    "actual_dep": (
                        datetime.fromtimestamp(f.get("firstSeen"), tz=timezone.utc).isoformat()
                        if f.get("firstSeen") else None
                    ),
                    "scheduled_arr": None,
                    "actual_arr": (
                        datetime.fromtimestamp(f.get("lastSeen"), tz=timezone.utc).isoformat()
                        if f.get("lastSeen") else None
                    ),
                    "delay_minutes_dep": None,
                    "delay_minutes_arr": None,
                    "provenance": {
                        "source_record_id": f.get("icao24"),
                        "source_timestamp": f.get("lastSeen"),
                    },
                }
            print(f"[opensky] arrivals fetched for {airport}: {len(r.json() or [])}")

        except requests.HTTPError as e:
            print(f"[opensky] HTTP error: {e} url={r.url}")
        except Exception as e:
            print(f"[opensky] error fetching arrivals: {e}")

        time.sleep(interval)
