from datetime import datetime, timezone
from typing import Dict

def to_iso8601(z: str) -> str:
    """Ensures timestamps are ISO-8601 with UTC offset (e.g., 2025-01-05T10:10:00+00:00)."""
    s = z.strip()
    if s.endswith("Z"):
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    else:
        dt = datetime.fromisoformat(s)
    return dt.astimezone(timezone.utc).isoformat()

def normalize(row: Dict) -> Dict:
    """Uppercases codes and normalizes timestamps."""
    return {
        "flight_id": row["flight_id"],
        "carrier": row["carrier"].upper(),
        "origin": row["origin"].upper(),
        "destination": row["destination"].upper(),
        "scheduled_dep": to_iso8601(row["scheduled_dep"]),
        "actual_dep": to_iso8601(row["actual_dep"]),
    }
