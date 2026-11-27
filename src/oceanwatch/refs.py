"""Tablas de referencia del pipeline: máscara de tierra, celdas navegables y puertos."""

from __future__ import annotations

import json
import urllib.request

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import config, geo


def _load_geojson(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "ocean-watch-analytics/2.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.load(resp)


def _polygons(geometry: dict):
    if geometry["type"] == "Polygon":
        yield geometry
    elif geometry["type"] == "MultiPolygon":
        for coords in geometry["coordinates"]:
            yield {"type": "Polygon", "coordinates": coords}


def _polyfill(features: list[dict], res: int) -> tuple[set[str], list[tuple]]:
    """Celdas cuyo centro cae dentro de los polígonos.

    H3 no puede rellenar algunos polígonos que abrazan un polo (Antártida); se omiten y se
    devuelven sus cajas envolventes para dejarlo registrado.
    """
    import h3  # solo se necesita en la tarea de preparación

    cells: set[str] = set()
    skipped: list[tuple] = []
    for feature in features:
        for poly in _polygons(feature["geometry"]):
            try:
                cells.update(h3.geo_to_cells(poly, res))
            except h3.H3BaseException:
                lons = [p[0] for p in poly["coordinates"][0]]
                lats = [p[1] for p in poly["coordinates"][0]]
                skipped.append((min(lats), max(lats), min(lons), max(lons)))
    return cells, skipped


def interior_land_cells(land: dict, lakes: dict, res: int, rings: int) -> list[int]:
    """Celdas H3 cuyo centro y el de todo su anillo `rings` caen en tierra firme.

    `land` y `lakes` son FeatureCollections GeoJSON (Natural Earth). Los lagos se restan
    para que los Grandes Lagos cuenten como agua.
    """
    import h3

    land_cells, skipped_land = _polyfill(land["features"], res)
    lake_cells, skipped_lakes = _polyfill(lakes["features"], res)
    for bbox in skipped_land + skipped_lakes:
        print(f"Polígono omitido (lat {bbox[0]:.1f} a {bbox[1]:.1f}, lon {bbox[2]:.1f} a {bbox[3]:.1f})")
    land_cells -= lake_cells
    interior = [c for c in land_cells if all(n in land_cells for n in h3.grid_disk(c, rings))]
    return sorted(h3.str_to_int(c) for c in interior)


def build_land_cells(spark, res: int = config.H3_RES_LAND, rings: int = config.LAND_BUFFER_RINGS) -> DataFrame:
    land = _load_geojson(config.NATURAL_EARTH_LAND_URL)
    lakes = _load_geojson(config.NATURAL_EARTH_LAKES_URL)
    cells = interior_land_cells(land, lakes, res, rings)
    return spark.createDataFrame([(c,) for c in cells], "cell BIGINT")


def build_navigable_cells(
    reference_positions: DataFrame,
    min_vessels: int = config.NAVIGABLE_MIN_VESSELS,
    dilation: int = config.NAVIGABLE_DILATION,
) -> DataFrame:
    """Celdas recorridas por al menos `min_vessels` buques distintos con MMSI válido,
    dilatadas `dilation` anillos.

    El umbral evita que una sola transmisión errónea convierta una celda de tierra en
    navegable; la dilatación cubre los tramos de un río o canal sin tráfico observado.
    """
    observed = (
        reference_positions.where(F.col("MMSI").rlike(config.MMSI_REGEX) & geo.coords_in_range())
        .withColumn("cell", geo.h3_cell(config.H3_RES_NAVIGABLE))
        .groupBy("cell")
        .agg(F.countDistinct("MMSI").alias("vessels"))
        .where(F.col("vessels") >= min_vessels)
    )
    return (
        observed.select(F.explode(geo.h3_kring("cell", dilation)).alias("cell"), "vessels")
        .groupBy("cell")
        .agg(F.max("vessels").alias("max_vessels_nearby"))
    )


def build_ports(world_port_index: DataFrame) -> DataFrame:
    return world_port_index.select("port_index", "port_name", "country_code", "LAT", "LON").where(geo.coords_in_range())


# Catálogo de tipos de buque AIS (ITU-R M.1371, documentado por NOAA Marine Cadastre), el mismo
# de la Entrega 1 (sección 3b). La consola lo usa para nombrar la categoría de cada buque.
VESSEL_TYPE_RANGES = [
    (0, 0, "Not available"),
    (1, 19, "Reserved"),
    (20, 29, "Wing in ground"),
    (30, 30, "Fishing"),
    (31, 32, "Towing"),
    (33, 33, "Dredging or underwater ops"),
    (34, 34, "Diving ops"),
    (35, 35, "Military ops"),
    (36, 36, "Sailing"),
    (37, 37, "Pleasure craft"),
    (38, 39, "Reserved"),
    (40, 49, "High speed craft"),
    (50, 50, "Pilot vessel"),
    (51, 51, "Search and rescue"),
    (52, 52, "Tug"),
    (53, 53, "Port tender"),
    (54, 54, "Anti-pollution equipment"),
    (55, 55, "Law enforcement"),
    (56, 57, "Spare, local vessel"),
    (58, 58, "Medical transport"),
    (59, 59, "Noncombatant ship"),
    (60, 69, "Passenger"),
    (70, 79, "Cargo"),
    (80, 89, "Tanker"),
    (90, 99, "Other type"),
]


def vessel_type_rows() -> list[tuple[int, str]]:
    """Una fila (VesselType, vessel_category) por código de 0 a 99."""
    return [(code, name) for low, high, name in VESSEL_TYPE_RANGES for code in range(low, high + 1)]



# World Port Index (NGA). Copia versionada en data/reference/world_port_index.csv: es la misma
# tabla de la Entrega 1 (sección 1.8, WPI.mdb convertido a grados decimales). Va en el repo porque
# el serverless del workspace institucional no alcanza Google Drive, de donde se descargaba.
WPI_CSV_RELATIVE = "data/reference/world_port_index.csv"
WPI_SCHEMA = "port_index LONG, port_name STRING, country_code STRING, LAT DOUBLE, LON DOUBLE"
WPI_COLUMN_COMMENTS = {
    "port_index": "Numero del puerto en el World Port Index",
    "port_name": "Nombre principal del puerto",
    "country_code": "Codigo de pais del World Port Index",
    "LAT": "Latitud del punto de referencia del puerto, grados decimales",
    "LON": "Longitud del punto de referencia del puerto, grados decimales",
}
