"""Funciones espaciales en columnas de Spark.

Las celdas H3 se calculan con las funciones nativas de Databricks (`h3_longlatash3`,
`h3_toparent`, `h3_kring`). En la Entrega 1 se usó una UDF de pandas con la librería
`h3`, y el plan mostró que el paso `ArrowEvalPython` era el más costoso de la consulta;
las funciones nativas corren en la JVM. Los identificadores son los mismos (H3 estándar,
como BIGINT).
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

from . import config


def coords_in_range(lat: str = "LAT", lon: str = "LON") -> Column:
    return (
        F.col(lat).isNotNull()
        & F.col(lon).isNotNull()
        & F.col(lat).between(-90, 90)
        & F.col(lon).between(-180, 180)
    )


def h3_cell(res: int, lat: str = "LAT", lon: str = "LON") -> Column:
    """Celda H3 (BIGINT) de la posición; null si las coordenadas no son válidas."""
    return F.when(coords_in_range(lat, lon), F.expr(f"h3_longlatash3({lon}, {lat}, {res})"))


def h3_parent(cell: str, res: int) -> Column:
    return F.expr(f"h3_toparent({cell}, {res})")


def h3_kring(cell: str, k: int) -> Column:
    return F.expr(f"h3_kring({cell}, {k})")


def haversine_km(lat1, lon1, lat2, lon2) -> Column:
    """Distancia de gran círculo en km entre dos pares de columnas (o nombres)."""
    lat1, lon1, lat2, lon2 = (F.col(c) if isinstance(c, str) else c for c in (lat1, lon1, lat2, lon2))
    a = F.pow(F.sin(F.radians(lat2 - lat1) / 2), 2) + F.cos(F.radians(lat1)) * F.cos(
        F.radians(lat2)
    ) * F.pow(F.sin(F.radians(lon2 - lon1) / 2), 2)
    return 2 * config.EARTH_RADIUS_KM * F.asin(F.sqrt(a))


def sog_limit(vessel_type: str = "VesselType") -> Column:
    """Velocidad máxima plausible en nudos según el tipo de buque."""
    return F.when(
        F.col(vessel_type).isin(config.FAST_VESSEL_TYPES), F.lit(config.SOG_MAX_FAST_TYPES)
    ).otherwise(F.lit(config.SOG_MAX))
