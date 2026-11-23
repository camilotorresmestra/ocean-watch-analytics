import uuid
from datetime import datetime, timedelta

from pyspark.sql import functions as F

from oceanwatch import config, gold
from helpers import cell

SILVER_DDL = (
    "MMSI string, BaseDateTime timestamp, day date, LAT double, LON double, SOG double, "
    "VesselName string, VesselType int, Length double, TransceiverClass string, h3_cell bigint"
)


def positions(spark, rows):
    out = []
    for mmsi, ts, lat, lon, sog, vtype in rows:
        out.append((mmsi, ts, ts.date(), lat, lon, sog, f"SHIP {mmsi}", vtype, 100.0, "A",
                    cell((lat, lon), config.H3_RES_TRAFFIC)))
    return spark.createDataFrame(out, SILVER_DDL)


T0 = datetime(2023, 6, 8, 10, 0, 0)


def test_track_discards_jumps(spark):
    # 0,1 grados de latitud (~11,1 km) por hora = 6 nudos; luego un salto de 10 grados en 1 minuto.
    rows = [
        ("1", T0, 30.0, -80.0, 6.0, 70),
        ("1", T0 + timedelta(hours=1), 30.1, -80.0, 6.0, 70),
        ("1", T0 + timedelta(hours=1, minutes=1), 40.1, -80.0, 6.0, 70),
        ("1", T0 + timedelta(hours=2), 30.2, -80.0, 6.0, 70),
    ]
    out = gold.vessel_daily_track(positions(spark, rows)).collect()[0]
    assert out["positions"] == 4
    assert out["segments"] == 3
    assert out["segments_discarded"] == 2  # ida y vuelta del salto
    assert abs(out["distance_km"] - 11.12) < 0.05
    assert out["active_hours"] == 2.0
    assert out["end_lat"] == 30.2
    assert [p["lat"] for p in out["track"]] == [30.0, 30.1, 30.2]  # 10:00, 11:00 (11:01 cae en el mismo intervalo), 12:00


def test_port_presence_nearest_port(spark):
    ports = spark.createDataFrame(
        [(1, "SAN DIEGO", "US", 32.7157, -117.1739), (2, "NATIONAL CITY", "US", 32.6600, -117.1200), (3, "LEJOS", "US", 33.5, -118.0)],
        "port_index int, port_name string, country_code string, LAT double, LON double",
    )
    rows = [
        ("1", T0, 32.71, -117.18, 0.1, 70),
        ("1", T0 + timedelta(minutes=30), 32.712, -117.178, 0.2, 70),
        ("1", T0 + timedelta(hours=3), 31.0, -117.5, 10.0, 70),  # fuera de todo radio
        ("2", T0, 32.661, -117.121, 3.0, 52),
    ]
    out = {r["MMSI"]: r for r in gold.port_presence(positions(spark, rows), ports).collect()}
    assert set(out) == {"1", "2"}
    assert out["1"]["port_name"] == "SAN DIEGO" and out["1"]["positions"] == 2
    assert out["1"]["minutes_in_port"] == 30.0 and out["1"]["stopped_positions"] == 2
    assert out["2"]["port_name"] == "NATIONAL CITY" and out["2"]["stopped_positions"] == 0


def test_traffic_cell_hourly(spark):
    rows = [
        ("1", T0, 32.70, -117.18, 5.0, 70),
        ("1", T0 + timedelta(minutes=5), 32.70, -117.18, 7.0, 70),
        ("2", T0 + timedelta(minutes=10), 32.70, -117.18, 9.0, 70),
        ("2", T0 + timedelta(hours=1), 32.70, -117.18, 9.0, 70),
    ]
    out = gold.traffic_cell_hourly(positions(spark, rows)).orderBy("hour").collect()
    assert [(r["hour"], r["positions"], r["vessels"], r["avg_sog"]) for r in out] == [(10, 3, 2, 7.0), (11, 1, 1, 9.0)]


def test_overwrite_day_is_idempotent(spark):
    table = f"default.gold_{uuid.uuid4().hex[:8]}"
    df = positions(spark, [("1", T0, 32.7, -117.18, 5.0, 70), ("1", T0 + timedelta(days=1), 32.7, -117.18, 5.0, 70)])
    gold.overwrite_day(gold.traffic_cell_hourly(df), table, "2023-06-08")
    gold.overwrite_day(gold.traffic_cell_hourly(df), table, "2023-06-09")
    gold.overwrite_day(gold.traffic_cell_hourly(df), table, "2023-06-08")  # reintento
    counts = {str(r["day"]): r["count"] for r in spark.table(table).groupBy("day").count().collect()}
    assert counts == {"2023-06-08": 1, "2023-06-09": 1}
