"""Nombres de objetos en Unity Catalog, rutas y constantes del pipeline.

Un solo lugar para estos valores evita que los notebooks se desalineen
(en la Entrega 1 hubo tablas escritas en workspace.default por un nombre sin calificar).
"""

# ---------------------------------------------------------------------------
# Catálogo y esquemas, uno por capa. Los permisos por rol (requisito 5) se dan por esquema.
# ---------------------------------------------------------------------------
CATALOG = "ocean_watch"
SCHEMA_RAW = "raw"  # Entrega 1: datos de la fuente y tablas de referencia
SCHEMA_REF = "ref"  # tablas de referencia del pipeline (máscara de tierra, celdas navegables)
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
SCHEMA_ML = "ml"


def table(schema: str, name: str) -> str:
    """Nombre totalmente calificado catalogo.esquema.tabla."""
    return f"{CATALOG}.{schema}.{name}"


# Tablas por capa.
BRONZE_AIS = table(SCHEMA_BRONZE, "ais")
SILVER_AIS = table(SCHEMA_SILVER, "ais")
SILVER_QUARANTINE = table(SCHEMA_SILVER, "ais_quarantine")
SILVER_QUALITY_METRICS = table(SCHEMA_SILVER, "quality_metrics")
GOLD_VESSEL_DAILY_TRACK = table(SCHEMA_GOLD, "vessel_daily_track")
GOLD_PORT_PRESENCE = table(SCHEMA_GOLD, "port_presence")
GOLD_TRAFFIC_CELL_HOURLY = table(SCHEMA_GOLD, "traffic_cell_hourly")
GOLD_SPEED_ANOMALIES = table(SCHEMA_GOLD, "vessel_speed_anomalies")

# Tablas de referencia.
WORLD_PORT_INDEX = table(SCHEMA_RAW, "world_port_index")
VESSEL_TYPE_CATALOG = table(SCHEMA_RAW, "vessel_type_catalog")
REF_LAND_CELLS = table(SCHEMA_REF, "land_cells")
REF_NAVIGABLE_CELLS = table(SCHEMA_REF, "navigable_cells")
REF_PORTS = table(SCHEMA_REF, "ports")

# Modelo registrado en Unity Catalog.
ML_MODEL_NAME = f"{CATALOG}.{SCHEMA_ML}.speed_anomaly_detector"
ML_MODEL_ALIAS = "champion"
ML_EXPERIMENT_NAME = "/Shared/ocean_watch/speed_anomaly_detector"

# ---------------------------------------------------------------------------
# Volume del feed diario. landing/ es lo único que lee Auto Loader.
# ---------------------------------------------------------------------------
VOLUME = "ais_feed"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/{VOLUME}"
LANDING_DIR = f"{VOLUME_ROOT}/landing"
MANIFEST_DIR = f"{VOLUME_ROOT}/_manifest"
CHECKPOINT_DIR = f"{VOLUME_ROOT}/_checkpoints"  # un subdirectorio por stream
SCHEMA_DIR = f"{VOLUME_ROOT}/_schemas"

CHECKPOINT_BRONZE = f"{CHECKPOINT_DIR}/bronze_ais"
CHECKPOINT_SILVER = f"{CHECKPOINT_DIR}/silver_ais"

# ---------------------------------------------------------------------------
# Fuente NOAA Marine Cadastre: un zip por día con un CSV adentro.
# ---------------------------------------------------------------------------
NOAA_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{year}/AIS_{year}_{month:02d}_{day:02d}.zip"
# Primer día del feed de la Entrega 2: el día siguiente al corpus de la Entrega 1
# (2023-06-01 a 2023-06-07), que sirve de referencia para las celdas navegables.
FEED_START_DATE = "2023-06-08"

# Máscara de tierra: polígonos de Natural Earth (dominio público).
NATURAL_EARTH_LAND_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/"
    "v5.1.2/geojson/ne_10m_land.geojson"
)
NATURAL_EARTH_LAKES_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/"
    "v5.1.2/geojson/ne_10m_lakes.geojson"
)

# ---------------------------------------------------------------------------
# Roles (grupos de la cuenta) para los permisos del requisito 5.
# ---------------------------------------------------------------------------
GROUP_ENGINEERS = "oceanwatch_ingenieros"  # escriben en todas las capas
GROUP_ANALYSTS = "oceanwatch_analistas"  # leen oro (tablero)
GROUP_SCIENTISTS = "oceanwatch_cientificos"  # leen plata y oro, escriben en ml

# ---------------------------------------------------------------------------
# Umbrales de calidad, justificados en la sección 2 de la Entrega 1.
# ---------------------------------------------------------------------------
SOG_MAX = 35.0  # nudos; percentil 99 = 22 nudos
# Tipos que superan 35 nudos en operación normal (Entrega 1, sección 2, R3):
# 40-49 embarcación rápida, 51 búsqueda y rescate, 55 cumplimiento de la ley.
SOG_MAX_FAST_TYPES = 60.0
FAST_VESSEL_TYPES = list(range(40, 50)) + [51, 55]
SOG_NO_DISPONIBLE = 102.3  # valor reservado por el estándar AIS
HEADING_NO_DISPONIBLE = 511.0
COG_NO_DISPONIBLE = 360.0
IMO_NO_DISPONIBLE = "IMO0000000"
MMSI_REGEX = r"^\d{9}$"
IMO_REGEX = r"^IMO\d{7}$"

# ---------------------------------------------------------------------------
# Grilla espacial (H3).
# ---------------------------------------------------------------------------
H3_RES_TRAFFIC = 8  # ~0,7 km², igual que la pregunta 3d de la Entrega 1
H3_RES_LAND = 5  # máscara de tierra (~250 km²): se calcula para todo el mundo en segundos
H3_RES_NAVIGABLE = 7  # celdas navegables observadas (~5 km²)
H3_RES_PORT = 5  # celdas candidatas para el cruce con puertos (~250 km²)
# Una celda de resolución 5 cuenta como tierra adentro solo si su centro y el de todo su
# anillo de este radio caen en tierra: deja fuera la costa, los muelles y los estuarios.
# Medido sobre un día real (2023-06-08, 8,9 M filas): con 1 anillo quedaba en tierra adentro
# el 18,1% de las posiciones; con 2, el 8,5% (ríos, canales y bahías interiores).
LAND_BUFFER_RINGS = 2
# Una celda de resolución 7 es navegable si la recorrieron al menos estos buques distintos
# en la semana de referencia de la Entrega 1; luego se dilata NAVIGABLE_DILATION anillos
# para cubrir los tramos de un río sin tráfico observado. Medido sobre el mismo día: con
# 5 buques y sin dilatar se rechazaba el 0,76% de las filas (483 buques reales en ríos y
# canales); con 2 buques y 1 anillo, el 0,09% (116 buques, la mitad con 3 posiciones o menos).
NAVIGABLE_MIN_VESSELS = 2
NAVIGABLE_DILATION = 1

# Presencia en puerto.
PORT_RADIUS_KM = 8.0
STOPPED_SOG = 0.5  # nudos; por debajo el buque se considera detenido

# Trayectorias: una posición por buque cada tantos minutos en el arreglo de la ruta.
TRACK_SAMPLE_MINUTES = 10

KNOT_KMH = 1.852
EARTH_RADIUS_KM = 6371.0

SESSION_TIMEZONE = "UTC"
