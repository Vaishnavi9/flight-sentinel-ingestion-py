from ingestion_py.sources.csv_source import read_csv

def test_csv_reader(tmp_path):
    csv_file = tmp_path / "flights.csv"
    csv_file.write_text(
        "flight_id,carrier,origin,destination,scheduled_dep,actual_dep\n"
        "LH123,LH,FRA,AMS,2025-01-05T10:10:00Z,2025-01-05T10:42:00Z\n"
    )
    rows = list(read_csv(str(csv_file)))
    assert len(rows) == 1
    assert rows[0]["flight_id"] == "LH123"
