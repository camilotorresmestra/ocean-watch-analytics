# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Llegada del archivo del día
# MAGIC
# MAGIC Primera tarea del job diario. Descarga el zip de NOAA del día indicado, verifica su integridad y deja el CSV en `landing/`, donde lo recoge Auto Loader.
# MAGIC
# MAGIC **Parámetro `fecha`:**
# MAGIC - `auto` (ejecución programada): el día siguiente al último entregado, a partir de `FEED_START_DATE`. Cada corrida del horario simula la llegada de un día nuevo.
# MAGIC - `AAAA-MM-DD` (backfill o reproceso): ese día. Si ya se entregó, no se vuelve a descargar.
# MAGIC
# MAGIC La fecha resuelta se publica como valor de tarea (`fecha`) para las tareas siguientes.
# MAGIC
# MAGIC Garantías (las de la Entrega 1, sección 1.2): reintentos con backoff de 10, 20, 40 y 80 s; bytes contra `Content-Length`; CRC-32 del zip; tamaño del CSV extraído; encabezado contra las 17 columnas esperadas. El CSV se escribe con un nombre temporal que empieza por `_` y se renombra al final, así Auto Loader nunca lee un archivo a medio escribir.

# COMMAND ----------

dbutils.widgets.text("fecha", "auto", "Fecha (AAAA-MM-DD o auto)")

# COMMAND ----------

import json
import os
import sys

sys.path.append(os.path.abspath("../../src"))

from oceanwatch import config, landing

fecha_param = dbutils.widgets.get("fecha").strip()
day = landing.resolve_day(fecha_param, config.MANIFEST_DIR, config.FEED_START_DATE, config.FEED_END_DATE)
print(f"Parámetro: {fecha_param} -> día a entregar: {day}")

# Feed automático terminado: la tarea `hay_dia` del job salta bronce, plata, oro y ML.
dbutils.jobs.taskValues.set(key="hay_dia", value="si" if day else "no")
if day is None:
    dbutils.jobs.taskValues.set(key="fecha", value="")
    dbutils.jobs.taskValues.set(key="landing_status", value="feed_terminado")
    dbutils.notebook.exit(f"Feed terminado: todos los días hasta {config.FEED_END_DATE} ya se entregaron")

# COMMAND ----------

result = landing.deliver_day(day, config.LANDING_DIR, config.MANIFEST_DIR, config.NOAA_URL)
print(json.dumps(result, indent=2))

dbutils.jobs.taskValues.set(key="fecha", value=day.isoformat())
dbutils.jobs.taskValues.set(key="landing_status", value=result["status"])
