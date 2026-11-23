# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Oro: modelo del negocio
# MAGIC
# MAGIC Construye para el día `fecha` las tres tablas del negocio, solo a partir de `ocean_watch.silver.ais`:
# MAGIC
# MAGIC | Tabla | Grano | Uso en la consola |
# MAGIC |---|---|---|
# MAGIC | `gold.vessel_daily_track` | buque × día | trayectorias, insumo del modelo de anomalías |
# MAGIC | `gold.port_presence` | buque × puerto × día | ranking de puertos, buques en puerto |
# MAGIC | `gold.traffic_cell_hourly` | celda H3 res 8 × hora | mapa de intensidad de tráfico |
# MAGIC
# MAGIC Cada tabla se reescribe solo para el día (`replaceWhere`): un reintento o un backfill deja el mismo resultado. El contrato de cada tabla (`docs/contrato_oro.md`) se aplica como comentarios en Unity Catalog.

# COMMAND ----------

dbutils.widgets.text("fecha", "", "Fecha (AAAA-MM-DD)")

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../../src"))

from pyspark.sql import functions as F

from oceanwatch import config, contract, gold

spark.conf.set("spark.sql.session.timeZone", config.SESSION_TIMEZONE)

day = dbutils.widgets.get("fecha").strip()
assert day, "El parámetro fecha es obligatorio"

silver_day = spark.table(config.SILVER_AIS).where(F.col("day") == F.lit(day).cast("date"))
silver_rows = silver_day.count()
print(f"{day}: {silver_rows:,} posiciones en plata")
assert silver_rows > 0, f"Plata no tiene posiciones para {day}: ¿llegó el archivo y corrió la tarea de plata?"

# COMMAND ----------

outputs = {
    config.GOLD_VESSEL_DAILY_TRACK: gold.vessel_daily_track(silver_day),
    config.GOLD_PORT_PRESENCE: gold.port_presence(silver_day, spark.table(config.REF_PORTS)),
    config.GOLD_TRAFFIC_CELL_HOURLY: gold.traffic_cell_hourly(silver_day),
}
for table, df in outputs.items():
    gold.overwrite_day(df, table, day)
    contract.apply_comments(spark, table)
    print(f"{table}: {spark.table(table).where(F.col('day') == day).count():,} filas para {day}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidencia: conciliación con plata

# COMMAND ----------

track_positions = spark.table(config.GOLD_VESSEL_DAILY_TRACK).where(F.col("day") == day).agg(F.sum("positions")).first()[0]
cell_positions = spark.table(config.GOLD_TRAFFIC_CELL_HOURLY).where(F.col("day") == day).agg(F.sum("positions")).first()[0]
print(f"Plata: {silver_rows:,} | trayectorias: {track_positions:,} | matriz de tráfico: {cell_positions:,}")
assert track_positions == silver_rows == cell_positions

display(
    spark.table(config.GOLD_PORT_PRESENCE)
    .where(F.col("day") == day)
    .groupBy("port_name")
    .agg(F.countDistinct("MMSI").alias("buques"), F.sum("stopped_positions").alias("posiciones_detenido"))
    .orderBy(F.desc("buques"))
    .limit(10)
)
