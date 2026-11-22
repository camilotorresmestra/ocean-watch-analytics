"""Esquema de los CSV de AIS de NOAA Marine Cadastre.

Las decisiones de tipos están justificadas en la sección 1 del README
(por ejemplo, Heading es DOUBLE porque NOAA lo publica como 511.0).
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Orden exacto del encabezado del CSV. Spark no lo valida con un esquema explícito,
# así que se compara a mano antes de leer.
EXPECTED_COLUMNS = [
    "MMSI", "BaseDateTime", "LAT", "LON", "SOG", "COG", "Heading",
    "VesselName", "IMO", "CallSign", "VesselType", "Status",
    "Length", "Width", "Draft", "Cargo", "TransceiverClass",
]

CORRUPT_RECORD_COLUMN = "_corrupt_record"

AIS_SCHEMA = StructType([
    StructField("MMSI",             StringType(),    nullable=True),
    StructField("BaseDateTime",     TimestampType(), nullable=True),
    StructField("LAT",              DoubleType(),    nullable=True),
    StructField("LON",              DoubleType(),    nullable=True),
    StructField("SOG",              DoubleType(),    nullable=True),
    StructField("COG",              DoubleType(),    nullable=True),
    StructField("Heading",          DoubleType(),    nullable=True),
    StructField("VesselName",       StringType(),    nullable=True),
    StructField("IMO",              StringType(),    nullable=True),
    StructField("CallSign",         StringType(),    nullable=True),
    StructField("VesselType",       IntegerType(),   nullable=True),
    StructField("Status",           IntegerType(),   nullable=True),
    StructField("Length",           DoubleType(),    nullable=True),
    StructField("Width",            DoubleType(),    nullable=True),
    StructField("Draft",            DoubleType(),    nullable=True),
    StructField("Cargo",            IntegerType(),   nullable=True),
    StructField("TransceiverClass", StringType(),    nullable=True),
    StructField(CORRUPT_RECORD_COLUMN, StringType(), nullable=True),
])

assert [f.name for f in AIS_SCHEMA.fields[:-1]] == EXPECTED_COLUMNS

# Opciones de lectura CSV, comunes a spark.read y a Auto Loader.
CSV_OPTIONS = {
    "header": "true",
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": CORRUPT_RECORD_COLUMN,
    "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss",
}
