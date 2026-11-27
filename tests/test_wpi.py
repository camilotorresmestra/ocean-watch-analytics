import csv
import os

from oceanwatch import refs

ROOT = os.path.join(os.path.dirname(__file__), "..")


def test_world_port_index_csv_matches_schema():
    with open(os.path.join(ROOT, refs.WPI_CSV_RELATIVE), encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert list(rows[0]) == list(refs.WPI_COLUMN_COMMENTS)
    assert len(rows) == 3630
    assert len({r["port_index"] for r in rows}) == len(rows)
    assert all(-90 <= float(r["LAT"]) <= 90 and -180 <= float(r["LON"]) <= 180 for r in rows)
    assert any(r["port_name"] == "SEATTLE" for r in rows)
