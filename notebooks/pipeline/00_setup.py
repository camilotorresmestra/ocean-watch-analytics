# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Preparación del lakehouse
# MAGIC
# MAGIC Se ejecuta una vez (job `ocean_watch_setup`), no en el job diario. Crea:
# MAGIC
# MAGIC 1. El catálogo y un esquema por capa (`bronze`, `silver`, `gold`, `ml`, `ref`), con comentarios.
# MAGIC 2. El Volume del feed diario (`landing/`, manifiestos y checkpoints).
# MAGIC 3. Las tablas de referencia: máscara de tierra, celdas navegables y puertos.
# MAGIC 4. Las tablas de plata (vacías) con su esquema declarado.
# MAGIC 5. Los permisos por rol (requisito 5).
# MAGIC
# MAGIC Es idempotente: todo usa `IF NOT EXISTS` o reescribe tablas de referencia completas.

# COMMAND ----------

# MAGIC %pip install -q h3==4.1.2

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../../src"))

from oceanwatch import config, refs, silver

spark.conf.set("spark.sql.session.timeZone", config.SESSION_TIMEZONE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catálogo, esquemas y Volume

# COMMAND ----------

SCHEMAS = {
    config.SCHEMA_RAW: "Entrega 1: datos AIS tal como se publican y tablas de referencia (World Port Index, tipos de buque)",
    config.SCHEMA_REF: "Tablas de referencia del pipeline: máscara de tierra, celdas navegables y puertos",
    config.SCHEMA_BRONZE: "Capa bronce: CSV del feed diario cargados por Auto Loader, sin modificar (inmutable)",
    config.SCHEMA_SILVER: "Capa plata: posiciones que pasan las compuertas de calidad, cuarentena y métricas de calidad",
    config.SCHEMA_GOLD: "Capa oro: modelo del negocio (trayectorias, presencia en puertos, tráfico por celda, anomalías)",
    config.SCHEMA_ML: "Modelos registrados en Unity Catalog",
}

# Unity Catalog exige CREATE CATALOG sobre el metastore aun con IF NOT EXISTS, y ese permiso
# solo lo tiene el administrador. Si el catálogo ya existe, no se intenta crear: así cualquier
# integrante con permisos sobre el catálogo puede volver a correr el setup.
if spark.sql(f"SHOW CATALOGS LIKE '{config.CATALOG}'").count() == 0:
    spark.sql(f"CREATE CATALOG {config.CATALOG} COMMENT 'OceanWatch Analytics: lakehouse del tráfico marítimo (MINE 4213, 2026-20)'")
for schema, comment in SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG}.{schema} COMMENT '{comment}'")

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {config.CATALOG}.{config.SCHEMA_BRONZE}.{config.VOLUME} "
    "COMMENT 'Feed diario: landing/ (CSV que lee Auto Loader), _manifest/ (integridad por día) y _checkpoints/'"
)
for d in (config.LANDING_DIR, config.MANIFEST_DIR, config.CHECKPOINT_DIR, config.SCHEMA_DIR):
    os.makedirs(d, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tablas de referencia
# MAGIC
# MAGIC **Máscara de tierra.** Polígonos de tierra de Natural Earth 10m (dominio público), menos los lagos, rellenados con celdas H3 de resolución 5. Una celda cuenta como *tierra adentro* solo si ella y todo su anillo 2 son tierra: así la costa, los muelles y los estuarios nunca quedan marcados. Toma alrededor de un minuto.
# MAGIC
# MAGIC **Celdas navegables.** Celdas H3 de resolución 7 que recorrieron al menos 2 buques distintos en la semana de la Entrega 1 (`ocean_watch.raw.ais`), dilatadas 1 anillo. Cubren los ríos y canales tierra adentro que Natural Earth marca como tierra (por ejemplo el Misisipi en St. Louis o el Columbia en Portland).
# MAGIC
# MAGIC La regla `posicion_en_agua` de plata rechaza una posición solo si cae en tierra adentro **y** no en una celda navegable.
# MAGIC
# MAGIC Calibración sobre un día real (2023-06-08, 8,9 millones de filas, con ese mismo día como referencia navegable): la primera versión (1 anillo de margen, 5 buques, sin dilatar) rechazaba el 0,76% de las filas, casi todas de remolcadores y barcazas reales en ríos y canales. La versión final rechaza el 0,09% (116 buques): una mezcla de errores claros (posiciones que saltan más de un grado, puntos en Misuri lejos de ríos navegables) y buques reales en vías pequeñas, como el delta del Sacramento. Con la semana completa como referencia se espera menos.

# COMMAND ----------

land = refs.build_land_cells(spark)
land.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.REF_LAND_CELLS)
spark.sql(f"COMMENT ON TABLE {config.REF_LAND_CELLS} IS 'Celdas H3 res {config.H3_RES_LAND} de tierra adentro (Natural Earth 10m land menos lakes, con margen de {config.LAND_BUFFER_RINGS} anillo)'")
print(f"{config.REF_LAND_CELLS}: {spark.table(config.REF_LAND_CELLS).count():,} celdas")

# COMMAND ----------

if spark.catalog.tableExists(f"{config.CATALOG}.{config.SCHEMA_RAW}.ais"):
    reference = spark.table(f"{config.CATALOG}.{config.SCHEMA_RAW}.ais")
else:
    print("No existe ocean_watch.raw.ais (Entrega 1): las celdas navegables quedan vacías")
    reference = spark.createDataFrame([], "MMSI STRING, LAT DOUBLE, LON DOUBLE")

nav = refs.build_navigable_cells(reference)
nav.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.REF_NAVIGABLE_CELLS)
spark.sql(f"COMMENT ON TABLE {config.REF_NAVIGABLE_CELLS} IS 'Celdas H3 res {config.H3_RES_NAVIGABLE} recorridas por al menos {config.NAVIGABLE_MIN_VESSELS} buques distintos en la semana de la Entrega 1, dilatadas {config.NAVIGABLE_DILATION} anillo'")
print(f"{config.REF_NAVIGABLE_CELLS}: {spark.table(config.REF_NAVIGABLE_CELLS).count():,} celdas")

# COMMAND ----------

ports = refs.build_ports(spark.table(config.WORLD_PORT_INDEX))
ports.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(config.REF_PORTS)
spark.sql(f"COMMENT ON TABLE {config.REF_PORTS} IS 'Puertos del World Port Index con coordenadas válidas, insumo de gold.port_presence'")
print(f"{config.REF_PORTS}: {spark.table(config.REF_PORTS).count():,} puertos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tablas de plata
# MAGIC
# MAGIC Se crean con esquema explícito para que el `MERGE` de plata y los `append` idempotentes de cuarentena y métricas tengan un destino fijo.

# COMMAND ----------

spark.sql(silver.silver_ddl(config.SILVER_AIS))
spark.sql(silver.quarantine_ddl(config.SILVER_QUARANTINE))
spark.sql(silver.metrics_ddl(config.SILVER_QUALITY_METRICS))

spark.sql(f"COMMENT ON TABLE {config.SILVER_AIS} IS 'Posiciones AIS que pasan todas las compuertas de calidad, sin duplicados por (MMSI, BaseDateTime). Valores de no disponible normalizados a null y marcados en quality_flags'")
spark.sql(f"COMMENT ON TABLE {config.SILVER_QUARANTINE} IS 'Filas de bronce que fallan al menos una compuerta; failed_rules lista las causas (ver src/oceanwatch/rules.py)'")
spark.sql(f"COMMENT ON TABLE {config.SILVER_QUALITY_METRICS} IS 'Métricas de calidad por micro-lote y día: filas totales, aprobadas, en cuarentena y fallidas por regla'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Permisos por rol
# MAGIC
# MAGIC | Grupo | Permisos |
# MAGIC |---|---|
# MAGIC | `oceanwatch_ingenieros` | Todo en el catálogo: operan el pipeline |
# MAGIC | `oceanwatch_cientificos` | Lectura de plata, oro y referencias; crear modelos en `ml` |
# MAGIC | `oceanwatch_analistas` | Lectura de oro (tablero) |
# MAGIC
# MAGIC Los grupos deben existir en la cuenta. Si alguno no existe, la celda lo reporta y sigue.

# COMMAND ----------

C = config.CATALOG
GRANTS = [
    (config.GROUP_ENGINEERS, f"GRANT ALL PRIVILEGES ON CATALOG {C} TO `{{g}}`"),
    (config.GROUP_SCIENTISTS, f"GRANT USE CATALOG ON CATALOG {C} TO `{{g}}`"),
    (config.GROUP_SCIENTISTS, f"GRANT USE SCHEMA, SELECT ON SCHEMA {C}.{config.SCHEMA_SILVER} TO `{{g}}`"),
    (config.GROUP_SCIENTISTS, f"GRANT USE SCHEMA, SELECT ON SCHEMA {C}.{config.SCHEMA_GOLD} TO `{{g}}`"),
    (config.GROUP_SCIENTISTS, f"GRANT USE SCHEMA, SELECT ON SCHEMA {C}.{config.SCHEMA_REF} TO `{{g}}`"),
    (config.GROUP_SCIENTISTS, f"GRANT USE SCHEMA, CREATE MODEL, CREATE TABLE ON SCHEMA {C}.{config.SCHEMA_ML} TO `{{g}}`"),
    (config.GROUP_ANALYSTS, f"GRANT USE CATALOG ON CATALOG {C} TO `{{g}}`"),
    (config.GROUP_ANALYSTS, f"GRANT USE SCHEMA, SELECT ON SCHEMA {C}.{config.SCHEMA_GOLD} TO `{{g}}`"),
]

for group, stmt in GRANTS:
    sql = stmt.format(g=group)
    try:
        spark.sql(sql)
        print(f"OK  {sql}")
    except Exception as exc:
        print(f"NO  {sql}\n    {str(exc).splitlines()[0]}")

# COMMAND ----------

# Evidencia del requisito 5.
for schema in (config.SCHEMA_BRONZE, config.SCHEMA_SILVER, config.SCHEMA_GOLD, config.SCHEMA_ML):
    display(spark.sql(f"SHOW GRANTS ON SCHEMA {config.CATALOG}.{schema}"))
