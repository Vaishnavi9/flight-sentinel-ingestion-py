from ingestion_py.transforms.normalizer import normalize

def test_normalizer_valid_row():
    row = {
        "flight_id": "LH123",
        "carrier": "lh",
        "origin": "fra",
        "destination": "ams",
        "scheduled_dep": "2025-01-05T10:10:00Z",
        "actual_dep": "2025-01-05T10:42:00Z",
    }
    out = normalize(row)
    assert out["carrier"] == "LH"
    assert out["origin"] == "FRA"
    assert out["destination"] == "AMS"
    assert out["scheduled_dep"].endswith("+00:00")
    assert out["actual_dep"].endswith("+00:00")
