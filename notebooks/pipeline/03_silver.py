# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Plata: compuertas de calidad
# MAGIC
# MAGIC Lee `ocean_watch.bronze.ais` en streaming y, por micro-lote (`foreachBatch`):
# MAGIC
# MAGIC 1. Evalúa las compuertas (`src/oceanwatch/rules.py`): `lectura_valida`, `mmsi_formato`, `timestamp_presente`, `fecha_del_archivo`, `coordenadas_en_rango`, `coordenadas_no_cero`, `posicion_en_agua` y `velocidad_plausible`.
# MAGIC 2. Marca como duplicadas las copias de una misma clave (`MMSI`, `BaseDateTime`) dentro del lote.
# MAGIC 3. Envía a `silver.ais_quarantine` toda fila que falle al menos una compuerta, con `failed_rules` como causa. El stream no se detiene.
# MAGIC 4. Normaliza los valores de no disponible (Heading 511, SOG 102,3, COG 360, IMO0000000) a null, marcados en `quality_flags`. No son compuertas: el Anexo A de la Entrega 1 mostró que descartarlos elimina casi toda la flota Clase B.
# MAGIC 5. Inserta en `silver.ais` con `MERGE` solo las claves nuevas: deduplica contra lo ya cargado y hace que repetir un lote no duplique filas.
# MAGIC 6. Registra en `silver.quality_metrics` las filas totales, aprobadas, en cuarentena y fallidas por regla.
# MAGIC
# MAGIC **Tolerancia a fallos.** Si el lote falla, Structured Streaming lo repite con el mismo `batch_id` al reiniciar desde el checkpoint. Cuarentena y métricas usan escrituras Delta idempotentes (`txnAppId`, `txnVersion`) y plata usa `MERGE`, así que el resultado es el mismo que sin fallo. El parámetro `simular_fallo` lo demuestra: el primer lote falla después de escribir la cuarentena; el reintento de la tarea (configurado en el job) lo completa.

# COMMAND ----------

dbutils.widgets.dropdown("simular_fallo", "no", ["no", "si"], "Simular fallo en el primer lote")
dbutils.widgets.text("run_id", "", "Id de la corrida del job ({{job.run_id}})")

# COMMAND ----------

import os
import sys
import uuid

sys.path.append(os.path.abspath("../../src"))

from pyspark.sql import functions as F

from oceanwatch import config, silver

spark.conf.set("spark.sql.session.timeZone", config.SESSION_TIMEZONE)

# COMMAND ----------

marker = None
if dbutils.widgets.get("simular_fallo") == "si":
    # Un marcador por corrida del job: el primer intento falla, el reintento lo encuentra y sigue.
    run_id = dbutils.widgets.get("run_id") or uuid.uuid4().hex
    marker = f"{config.VOLUME_ROOT}/_fault_injection/silver_{run_id}"
    print(f"Fallo simulado activado; marcador {marker}")

process = silver.make_batch_processor(
    spark,
    land_cells=spark.table(config.REF_LAND_CELLS),
    navigable_cells=spark.table(config.REF_NAVIGABLE_CELLS),
    fail_once_marker=marker,
)

query = (
    spark.readStream.table(config.BRONZE_AIS)
    .writeStream.foreachBatch(process)
    .option("checkpointLocation", config.CHECKPOINT_SILVER)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidencia

# COMMAND ----------

# Métricas de los lotes de esta corrida (o los últimos si no hubo datos nuevos).
display(
    spark.table(config.SILVER_QUALITY_METRICS)
    .where(F.col("batch_id") == spark.table(config.SILVER_QUALITY_METRICS).agg(F.max("batch_id")).first()[0])
    .orderBy("day", "category", "metric")
)

# COMMAND ----------

# Causas de cuarentena acumuladas.
display(
    spark.table(config.SILVER_QUARANTINE)
    .select(F.explode("failed_rules").alias("regla"))
    .groupBy("regla")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# Compuerta: ninguna fila de plata incumple las reglas que se pueden verificar sobre la fila.
from oceanwatch import geo

violations = (
    spark.table(config.SILVER_AIS)
    .where(
        ~F.col("MMSI").rlike(config.MMSI_REGEX)
        | ~geo.coords_in_range()
        | ((F.col("LAT") == 0) & (F.col("LON") == 0))
        | (F.col("SOG") < 0)
        | (F.col("SOG") > geo.sog_limit())
    )
    .count()
)
duplicates = spark.table(config.SILVER_AIS).groupBy("MMSI", "BaseDateTime").count().where("count > 1").count()
print(f"Filas de plata que incumplen una compuerta: {violations}")
print(f"Claves (MMSI, BaseDateTime) repetidas en plata: {duplicates}")
assert violations == 0 and duplicates == 0
