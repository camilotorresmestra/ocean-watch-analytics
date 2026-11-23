from datetime import datetime

import h3

from oceanwatch.schema import AIS_SCHEMA

KANSAS = (38.5, -98.0)  # tierra adentro
ST_LOUIS_RIVER = (38.627, -90.18)  # río Misisipi, tierra en Natural Earth pero navegable
SAN_DIEGO_BAY = (32.70, -117.18)
PACIFIC = (32.5, -118.0)


def cell(latlon, res):
    return h3.str_to_int(h3.latlng_to_cell(latlon[0], latlon[1], res))


def row(mmsi="367000001", ts="2023-06-08 10:00:00", lat=PACIFIC[0], lon=PACIFIC[1], sog=10.0,
        vessel_type=70, heading=90.0, imo="IMO9074729", corrupt=None, name="TEST SHIP",
        source_file="/Volumes/x/landing/AIS_2023_06_08.csv", ingested="2023-06-09 00:00:00", cog=45.0):
    return {
        "MMSI": mmsi,
        "BaseDateTime": datetime.fromisoformat(ts) if ts else None,
        "LAT": lat, "LON": lon, "SOG": sog, "COG": cog, "Heading": heading,
        "VesselName": name, "IMO": imo, "CallSign": "WXYZ", "VesselType": vessel_type,
        "Status": 0, "Length": 100.0, "Width": 20.0, "Draft": 5.0, "Cargo": None,
        "TransceiverClass": "A", "_corrupt_record": corrupt,
        "source_file": source_file,
        "source_modified_at": datetime(2023, 6, 9),
        "bronze_ingested_at": datetime.fromisoformat(ingested),
    }


BRONZE_DDL = ", ".join(
    [f"`{f.name}` {f.dataType.simpleString()}" for f in AIS_SCHEMA.fields]
    + ["source_file string", "source_modified_at timestamp", "bronze_ingested_at timestamp"]
)


def bronze_df(spark, rows):
    cols = [f.name for f in AIS_SCHEMA.fields] + ["source_file", "source_modified_at", "bronze_ingested_at"]
    return spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], BRONZE_DDL)
