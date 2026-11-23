# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronce: Auto Loader
# MAGIC
# MAGIC Carga a `ocean_watch.bronze.ais` los CSV nuevos de `landing/` con Structured Streaming (`cloudFiles`).
# MAGIC
# MAGIC - **Checkpoint** en el Volume: guarda qué archivos ya se procesaron. Si la tarea se reinicia, no vuelve a cargar un archivo ya confirmado, y un archivo a medio cargar se repite completo (escritura Delta transaccional).
# MAGIC - **`trigger(availableNow=True)`**: procesa lo pendiente y termina, para que la tarea del job tenga fin.
# MAGIC - **Inmutable:** solo `append`. Bronce guarda la fila tal como llegó, con el esquema explícito de la Entrega 1, más columnas de auditoría. Ninguna limpieza ni deduplicación ocurre aquí; eso es plata.
# MAGIC - **`pathGlobFilter = *.csv`**: ignora los archivos temporales de la tarea de llegada.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../../src"))

from pyspark.sql import functions as F

from oceanwatch import config
from oceanwatch.schema import AIS_SCHEMA, CSV_OPTIONS

spark.conf.set("spark.sql.session.timeZone", config.SESSION_TIMEZONE)

# COMMAND ----------

reader = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{config.SCHEMA_DIR}/bronze_ais")
    .option("pathGlobFilter", "*.csv")
)
for key, value in CSV_OPTIONS.items():
    reader = reader.option(key, value)

bronze = (
    reader.schema(AIS_SCHEMA)
    .load(config.LANDING_DIR)
    .withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("source_modified_at", F.col("_metadata.file_modification_time"))
    .withColumn("bronze_ingested_at", F.current_timestamp())
)

query = (
    bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", config.CHECKPOINT_BRONZE)
    .trigger(availableNow=True)
    .toTable(config.BRONZE_AIS)
)
query.awaitTermination()

# COMMAND ----------

spark.sql(f"COMMENT ON TABLE {config.BRONZE_AIS} IS 'Posiciones AIS del feed diario tal como llegan de NOAA (CSV en landing/), cargadas con Auto Loader. Solo append; sin limpieza'")
spark.sql(f"ALTER TABLE {config.BRONZE_AIS} SET TBLPROPERTIES ('layer' = 'bronze', 'source' = 'NOAA Marine Cadastre AIS')")

# Evidencia: filas por archivo y lo que procesó esta corrida.
progress = query.lastProgress
print("Filas en esta corrida:", progress["numInputRows"] if progress else 0)
display(
    spark.table(config.BRONZE_AIS)
    .groupBy("source_file")
    .agg(F.count("*").alias("filas"), F.max("bronze_ingested_at").alias("cargado"))
    .orderBy("source_file")
)
