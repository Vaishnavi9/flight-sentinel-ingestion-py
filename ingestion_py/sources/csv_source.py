import csv
from typing import Dict, Iterator
from pathlib import Path

def read_csv(path: str) -> Iterator[Dict]:
    """Yields one dictionary per CSV row."""
    p = Path(path)
    with p.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Strip spaces from strings for cleanliness
            yield {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
