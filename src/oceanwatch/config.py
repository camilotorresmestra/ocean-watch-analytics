"""Nombres de objetos en Unity Catalog, rutas y constantes del pipeline.

Un solo lugar para estos valores evita que los notebooks se desalineen
(en la Entrega 1 hubo tablas escritas en workspace.default por un nombre sin calificar).
"""

# Catálogo y esquemas, uno por capa. Los permisos por rol (requisito 5) se dan por esquema.
CATALOG = "ocean_watch"
SCHEMA_RAW = "raw"  # Entrega 1: datos de la fuente y tablas de referencia
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
SCHEMA_ML = "ml"


def table(schema: str, name: str) -> str:
    """Nombre totalmente calificado catalogo.esquema.tabla."""
    return f"{CATALOG}.{schema}.{name}"


# Volume de archivos del feed diario.
VOLUME = "ais_feed"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/{VOLUME}"
LANDING_DIR = f"{VOLUME_ROOT}/landing"  # CSV descomprimidos que lee Auto Loader
CHECKPOINT_DIR = f"{VOLUME_ROOT}/_checkpoints"  # un subdirectorio por stream
SCHEMA_DIR = f"{VOLUME_ROOT}/_schemas"

# Fuente NOAA Marine Cadastre: un zip por día.
BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{year}/AIS_{year}_{month:02d}_{day:02d}.zip"

# Tablas de referencia de la Entrega 1.
WORLD_PORT_INDEX = table(SCHEMA_RAW, "world_port_index")
VESSEL_TYPE_CATALOG = table(SCHEMA_RAW, "vessel_type_catalog")

# Umbrales de calidad, justificados en la sección 2 de la Entrega 1.
SOG_MAX = 35.0  # nudos; percentil 99 = 22 nudos
SOG_NO_DISPONIBLE = 102.3  # valor reservado por el estándar AIS
HEADING_NO_DISPONIBLE = 511.0
COG_NO_DISPONIBLE = 360.0
MMSI_REGEX = r"^\d{9}$"

SESSION_TIMEZONE = "UTC"
