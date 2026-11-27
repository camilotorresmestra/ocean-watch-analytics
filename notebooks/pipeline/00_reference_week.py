# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Semana de referencia (Entrega 1)
# MAGIC
# MAGIC Primera tarea del job `ocean_watch_setup`. Carga en `raw.ais` los días 2023-06-01 a 2023-06-07, el corpus de la Entrega 1. El setup los usa para las **celdas navegables**: sin ellas, la regla `posicion_en_agua` manda a cuarentena los buques de ríos y puertos interiores que Natural Earth marca como tierra.
# MAGIC
# MAGIC Reutiliza `landing.deliver_day` (reintentos, integridad y manifiesto por día), pero en un Volume propio de `raw`: estos archivos no pasan por bronce ni por el job diario. Es idempotente: un día con manifiesto no se vuelve a descargar y la tabla se reescribe completa.

# COMMAND ----------

# MAGIC %pip install -q zstandard

# COMMAND ----------

import os
import sys
from datetime import timedelta

sys.path.append(os.path.abspath("../../src"))

from pyspark.sql import functions as F

from oceanwatch import config, landing
from oceanwatch.schema import AIS_SCHEMA, CSV_OPTIONS

spark.conf.set("spark.sql.session.timeZone", config.SESSION_TIMEZONE)

# Esta tarea corre antes de 00_setup: crea su propio esquema para funcionar en un catálogo vacío.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{config.SCHEMA_RAW} "
          "COMMENT 'Entrega 1: datos AIS tal como se publican y tablas de referencia (World Port Index, tipos de buque)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {config.CATALOG}.{config.SCHEMA_RAW}.ais_raw "
          "COMMENT 'CSV diarios de la semana de referencia de la Entrega 1 y sus manifiestos'")

# COMMAND ----------

day = landing.parse_day(config.REFERENCE_WEEK_START)
end = landing.parse_day(config.REFERENCE_WEEK_END)
while day <= end:
    result = landing.deliver_day(day, config.RAW_CSV_DIR, config.RAW_MANIFEST_DIR, config.NOAA_URL,
                                 tmp_dir="/tmp/ais_reference")
    print(day, result["status"], result.get("csv_bytes"))
    day += timedelta(days=1)

# COMMAND ----------

reader = spark.read.schema(AIS_SCHEMA)
for key, value in CSV_OPTIONS.items():
    reader = reader.option(key, value)

raw = (
    reader.csv(f"{config.RAW_CSV_DIR}/*.csv")
    .withColumn("source_file", F.col("_metadata.file_name"))
    .withColumn("day", F.to_date("BaseDateTime"))
)
(raw.write.mode("overwrite").option("overwriteSchema", "true").partitionBy("day")
    .saveAsTable(config.RAW_AIS))
spark.sql(f"COMMENT ON TABLE {config.RAW_AIS} IS "
          "'Entrega 1: AIS de NOAA del 2023-06-01 al 2023-06-07, tal como se publica. Insumo de ref.navigable_cells'")

# COMMAND ----------

summary = spark.table(config.RAW_AIS).groupBy("source_file").agg(
    F.count("*").alias("rows"), F.count(F.col("_corrupt_record")).alias("corrupt")
).orderBy("source_file")
display(summary)
n_files = summary.count()
assert n_files == (end - landing.parse_day(config.REFERENCE_WEEK_START)).days + 1, f"Se esperaban 7 archivos, hay {n_files}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catálogo de tipos de buque
# MAGIC
# MAGIC La otra tabla de referencia de la Entrega 1 (sección 3b). La consola la usa para mostrar la categoría de cada buque en los paneles de buques en puerto y anomalías.

# COMMAND ----------

from oceanwatch import refs

vessel_types = spark.createDataFrame(refs.vessel_type_rows(), "VesselType INT, vessel_category STRING")
vessel_types.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.VESSEL_TYPE_CATALOG)
spark.sql(f"COMMENT ON TABLE {config.VESSEL_TYPE_CATALOG} IS 'Catalogo de tipos de buque AIS (ITU-R M.1371, documentado por NOAA Marine Cadastre). Una fila por codigo de VesselType'")
spark.sql(f"ALTER TABLE {config.VESSEL_TYPE_CATALOG} ALTER COLUMN VesselType COMMENT 'Codigo AIS de tipo de buque, 0 a 99'")
spark.sql(f"ALTER TABLE {config.VESSEL_TYPE_CATALOG} ALTER COLUMN vessel_category COMMENT 'Categoria del codigo segun el catalogo AIS'")
assert spark.table(config.VESSEL_TYPE_CATALOG).count() == 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## World Port Index
# MAGIC
# MAGIC Puertos del mundo (NGA), la misma tabla de la Entrega 1 (sección 1.8), versionada en `data/reference/world_port_index.csv`: el serverless del workspace institucional no alcanza Google Drive, de donde se descargaba el `WPI.mdb`. El setup construye con ella `ref.ports` y la presencia en puertos de oro.

# COMMAND ----------

import pandas as pd

wpi_path = os.path.abspath(os.path.join("../..", refs.WPI_CSV_RELATIVE))
# round_trip: el lector por defecto de pandas cambia el último dígito de algunos decimales.
wpi = pd.read_csv(wpi_path, keep_default_na=False, float_precision="round_trip",
                  dtype={"port_name": str, "country_code": str})
(spark.createDataFrame(wpi, refs.WPI_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(config.WORLD_PORT_INDEX))
spark.sql(f"COMMENT ON TABLE {config.WORLD_PORT_INDEX} IS 'World Port Index (NGA): puertos del mundo con su punto de referencia. Copia de la Entrega 1 versionada en el repo. Insumo de ref.ports'")
for column, comment in refs.WPI_COLUMN_COMMENTS.items():
    spark.sql(f"ALTER TABLE {config.WORLD_PORT_INDEX} ALTER COLUMN `{column}` COMMENT '{comment}'")
print(f"{config.WORLD_PORT_INDEX}: {len(wpi):,} puertos desde {refs.WPI_CSV_RELATIVE}")

assert spark.table(config.WORLD_PORT_INDEX).count() == len(wpi)
