import time, requests
from typing import Iterator, Dict, Optional
from datetime import datetime, timezone

OS_URL = "https://opensky-network.org/api/states/all"

def fetch_live_states(bbox: Optional[tuple]=None, interval: int = 300) -> Iterator[Dict]:
    """
    Poll /states/all every `interval` seconds.
    bbox=(lamin, lamax, lomin, lomax) to limit area (recommended for quota).
    Yields normalized dicts per aircraft state.
    """
    params = {}
    if bbox:
        lamin, lamax, lomin, lomax = bbox
        params = {"lamin": lamin, "lamax": lamax, "lomin": lomin, "lomax": lomax}

    while True:
        try:
            r = requests.get(OS_URL, params=params, timeout=15)
            r.raise_for_status()
            j = r.json() or {}
            srv_time = j.get("time")
            for s in j.get("states", []) or []:
                # s indices per OpenSky docs
                yield {
                    "event_type": "opensky_state",
                    "provider": "opensky",
                    "ingested_at": datetime.utcnow().isoformat() + "Z",
                    "srv_time": srv_time,
                    "icao24": s[0],
                    "callsign": (s[1] or "").strip() if s[1] else None,
                    "origin_country": s[2],
                    "time_position": s[3],
                    "last_contact": s[4],
                    "longitude": s[5],
                    "latitude": s[6],
                    "baro_altitude_m": s[7],
                    "on_ground": s[8],
                    "velocity_mps": s[9],
                    "true_track_deg": s[10],
                    "vertical_rate_mps": s[11],
                    "geo_altitude_m": s[13],
                    "squawk": s[14],
                    "spi": s[15],
                    "position_source": s[16],
                    "category": s[17] if len(s) > 17 else None
                }
        except requests.HTTPError as e:
            yield {
                "event_type": "provider_status",
                "provider": "opensky",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
                "status": "http_error",
                "message": f"/states/all error: {e}"
            }
        except Exception as e:
            yield {
                "event_type": "provider_status",
                "provider": "opensky",
                "ingested_at": datetime.utcnow().isoformat() + "Z",
                "status": "exception",
                "message": f"/states/all exception: {e}"
            }

        time.sleep(interval)
