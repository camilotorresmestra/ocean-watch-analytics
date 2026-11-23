"""Compuertas de calidad de la capa plata.

Cada regla es una expectativa: una condición que la fila debe cumplir para pasar a plata.
Las filas que fallan al menos una regla van a cuarentena con la lista de reglas fallidas
como causa; el stream no se detiene.

Las reglas retoman el diagnóstico R1 a R9 de la Entrega 1 (sección 2). Los valores de
"no disponible" del estándar AIS (Heading 511, SOG 102,3, IMO0000000) NO son compuertas:
el Anexo A de la Entrega 1 mostró que descartarlos elimina casi toda la flota Clase B.
Esos valores se normalizan a null en `standardize` y se marcan en `quality_flags`.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from . import config, geo
from .schema import CORRUPT_RECORD_COLUMN

# Fecha del nombre del archivo de origen (AIS_2023_06_08.csv -> 2023-06-08).
def file_day() -> Column:
    return F.to_date(
        F.regexp_extract(F.col("source_file"), r"AIS_(\d{4})_(\d{2})_(\d{2})", 0), "'AIS_'yyyy_MM_dd"
    )


@dataclass(frozen=True)
class Rule:
    name: str
    category: str
    description: str
    entrega1: str  # regla equivalente del diagnóstico de la Entrega 1

    def passes(self) -> Column:
        return _CONDITIONS[self.name]()


RULES = [
    Rule("lectura_valida", "integridad", "Todos los campos se convirtieron al tipo declarado", "R6"),
    Rule("mmsi_formato", "identificador", "MMSI de exactamente 9 dígitos", "R1"),
    Rule("timestamp_presente", "integridad", "BaseDateTime no nulo", "-"),
    Rule("fecha_del_archivo", "integridad", "BaseDateTime cae en el día del archivo de origen", "R7"),
    Rule("coordenadas_en_rango", "posicion", "LAT en [-90, 90] y LON en [-180, 180], no nulas", "R2"),
    Rule("coordenadas_no_cero", "posicion", "La posición no es (0, 0), valor por defecto de transpondedores mal configurados", "-"),
    Rule("posicion_en_agua", "posicion", "La posición no cae tierra adentro (máscara Natural Earth con margen, salvo celdas navegables observadas)", "nueva"),
    Rule("velocidad_plausible", "velocidad", "SOG entre 0 y el máximo del tipo de buque (35 nudos; 60 para tipos 40-49, 51 y 55), o no disponible", "R3"),
]

DUPLICATE_RULE = Rule(
    "duplicado_mmsi_timestamp", "duplicado", "Ya existe una posición con el mismo (MMSI, BaseDateTime)", "R8, R9"
)

RULES_BY_NAME = {r.name: r for r in RULES + [DUPLICATE_RULE]}

_CONDITIONS = {
    "lectura_valida": lambda: F.col(CORRUPT_RECORD_COLUMN).isNull(),
    "mmsi_formato": lambda: F.coalesce(F.col("MMSI").rlike(config.MMSI_REGEX), F.lit(False)),
    "timestamp_presente": lambda: F.col("BaseDateTime").isNotNull(),
    "fecha_del_archivo": lambda: F.coalesce(F.to_date("BaseDateTime") == file_day(), F.lit(False)),
    "coordenadas_en_rango": lambda: geo.coords_in_range(),
    "coordenadas_no_cero": lambda: ~((F.col("LAT") == 0) & (F.col("LON") == 0)) | F.col("LAT").isNull(),
    "posicion_en_agua": lambda: ~F.coalesce(F.col("_on_land"), F.lit(False)),
    "velocidad_plausible": lambda: (
        F.col("SOG").isNull()
        | (F.col("SOG") == config.SOG_NO_DISPONIBLE)
        | F.col("SOG").between(F.lit(0.0), geo.sog_limit())
    ),
}


def annotate_land(df: DataFrame, land_cells: DataFrame, navigable_cells: DataFrame) -> DataFrame:
    """Agrega `_on_land`: la posición cae en una celda de tierra adentro y no navegable.

    land_cells: columna `cell` (BIGINT, resolución H3_RES_LAND).
    navigable_cells: columna `cell` (BIGINT, resolución H3_RES_NAVIGABLE).
    """
    land = land_cells.select(F.col("cell").alias("_land_cell"), F.lit(True).alias("_is_land"))
    nav = navigable_cells.select(F.col("cell").alias("_nav_cell"), F.lit(True).alias("_is_navigable"))
    return (
        df.withColumn("_land_cell", geo.h3_cell(config.H3_RES_LAND))
        .withColumn("_nav_cell", geo.h3_cell(config.H3_RES_NAVIGABLE))
        .join(F.broadcast(land), "_land_cell", "left")
        .join(F.broadcast(nav), "_nav_cell", "left")
        .withColumn(
            "_on_land",
            F.coalesce(F.col("_is_land"), F.lit(False)) & ~F.coalesce(F.col("_is_navigable"), F.lit(False)),
        )
        .drop("_land_cell", "_nav_cell", "_is_land", "_is_navigable")
    )


def evaluate(df: DataFrame) -> DataFrame:
    """Agrega `failed_rules` (array con los nombres de las reglas que la fila no cumple).

    Requiere la columna `_on_land` de `annotate_land`.
    """
    failed = F.array(*[F.when(~r.passes(), F.lit(r.name)) for r in RULES])
    return df.withColumn("failed_rules", F.filter(failed, lambda x: x.isNotNull()))


def standardize(df: DataFrame) -> DataFrame:
    """Normaliza los valores de no disponible a null y los deja marcados en `quality_flags`."""
    imo_irregular = F.col("IMO").isNotNull() & ~F.col("IMO").rlike(config.IMO_REGEX)
    imo_missing = F.col("IMO").isNull() | (F.col("IMO") == config.IMO_NO_DISPONIBLE)
    flags = F.array(
        F.when(F.col("Heading") == config.HEADING_NO_DISPONIBLE, F.lit("heading_no_disponible")),
        F.when(F.col("SOG") == config.SOG_NO_DISPONIBLE, F.lit("sog_no_disponible")),
        F.when(F.col("COG") >= config.COG_NO_DISPONIBLE, F.lit("cog_no_disponible")),
        F.when(imo_missing, F.lit("imo_no_disponible")),
        F.when(imo_irregular & ~imo_missing, F.lit("imo_irregular")),
    )
    return (
        df.withColumn("quality_flags", F.filter(flags, lambda x: x.isNotNull()))
        .withColumn("Heading", F.when(F.col("Heading") != config.HEADING_NO_DISPONIBLE, F.col("Heading")))
        .withColumn("SOG", F.when(F.col("SOG") != config.SOG_NO_DISPONIBLE, F.col("SOG")))
        .withColumn("COG", F.when(F.col("COG") < config.COG_NO_DISPONIBLE, F.col("COG")))
        .withColumn("IMO", F.when(~imo_missing & ~imo_irregular, F.col("IMO")))
        .withColumn("VesselName", F.when(F.trim("VesselName") != "", F.trim("VesselName")))
        .withColumn("CallSign", F.when(F.trim("CallSign") != "", F.trim("CallSign")))
    )
