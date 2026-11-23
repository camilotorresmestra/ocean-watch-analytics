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
