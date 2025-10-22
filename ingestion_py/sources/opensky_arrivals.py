import time, requests
from typing import Iterator, Dict
from datetime import datetime, timezone

OS_API_BASE = "https://opensky-network.org/api"

def fetch_arrivals_yesterday(airport: str, interval: int = 900) -> Iterator[Dict]:
    """
    Every `interval` seconds, fetch arrivals for *yesterday's* same window.
    Avoids 404 that happens when trying to query "today".
    Yields canonical-ish flight_status events (scheduled_* are null; OpenSky has actuals only).
    """
    while True:
        now = int(datetime.now(timezone.utc).timestamp())
        end = now - 24*3600
        begin = end - interval

        try:
            r = requests.get(
                f"{OS_API_BASE}/flights/arrival",
                params={"airport": airport, "begin": begin, "end": end},
                timeout=15,
            )
            if r.status_code == 404:
                yield {
                    "event_type": "provider_status",
                    "provider": "opensky",
                    "ingested_at": datetime.utcnow().isoformat() + "Z",
                    "status": "empty_window",
                    "message": f"no arrivals for {airport} in {begin}-{end}"
                }
                time.sleep(interval)
                continue

            r.raise_for_status()
            flights = r.json() or []

            for f in flights:
                yield {
                    "event_type": "flight_status",
                    "provider": "opensky",
                    "ingested_at": datetime.utcnow().isoformat() + "Z",
                    "flight_id": f.get("icao24"),  # OpenSky uses icao24; no airline code
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
                    "delay_minutes_dep": None,    # we don't know schedule here
                    "delay_minutes_arr": None,
                    "provenance": {
                        "source_record_id": f.get("icao24"),
                        "source_timestamp": f.get("lastSeen"),
                    },
                }

            yield {
                "event_type": "provider_status",
                "provider": "opensky",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
                "status": "ok",
                "message": f"arrivals fetched: {len(flights)} for {airport}"
            }

        except requests.HTTPError as e:
            yield {
                "event_type": "provider_status",
                "provider": "opensky",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
                "status": "http_error",
                "message": f"/flights/arrival error: {e}"
            }
        except Exception as e:
            yield {
                "event_type": "provider_status",
                "provider": "opensky",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
                "status": "exception",
                "message": f"/flights/arrival exception: {e}"
            }

        time.sleep(interval)
