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
