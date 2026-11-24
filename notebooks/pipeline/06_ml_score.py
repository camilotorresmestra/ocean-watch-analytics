# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Puntuación diaria de anomalías de velocidad
# MAGIC
# MAGIC Última tarea del job diario. Carga el modelo `ocean_watch.ml.speed_anomaly_detector@champion` desde Unity Catalog, calcula las variables del día con la misma función del entrenamiento y escribe `gold.vessel_speed_anomalies` para el día (`replaceWhere`). Alimenta el panel de anomalías de la consola.

# COMMAND ----------

# "dia" y no "fecha": un parámetro del job se pasa a todas las tareas y reemplaza al de la
# tarea con el mismo nombre, así que fecha=auto pisaría el día que resolvió landing.
dbutils.widgets.text("dia", "", "Día (AAAA-MM-DD)")

# COMMAND ----------

# MAGIC %pip install -q scikit-learn==1.5.2

# COMMAND ----------

import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.abspath("../../src"))

import mlflow
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F

from oceanwatch import config, contract, gold, ml

mlflow.set_registry_uri("databricks-uc")
spark.conf.set("spark.sql.session.timeZone", config.SESSION_TIMEZONE)

day = dbutils.widgets.get("dia").strip()
assert day, "El parámetro dia es obligatorio"

# COMMAND ----------

model_uri = f"models:/{config.ML_MODEL_NAME}@{config.ML_MODEL_ALIAS}"
try:
    version = MlflowClient().get_model_version_by_alias(config.ML_MODEL_NAME, config.ML_MODEL_ALIAS).version
except Exception as exc:
    # Los primeros días del feed corren antes del primer entrenamiento: no es un fallo del pipeline.
    print(f"No hay modelo {model_uri} todavía ({str(exc).splitlines()[0]}). Correr el job ocean_watch_train.")
    dbutils.notebook.exit("sin_modelo")
model = mlflow.sklearn.load_model(model_uri)
print(f"Modelo {model_uri} (versión {version})")

features = ml.build_features(
    spark.table(config.GOLD_VESSEL_DAILY_TRACK).where(F.col("day") == F.lit(day).cast("date"))
).toPandas()
assert len(features) > 0, f"Oro no tiene trayectorias para {day}"

X = features[ml.FEATURES]
features["anomaly_score"] = -model.score_samples(X)
features["is_anomaly"] = model.predict(X) == -1
features["rule_flag"] = ml.rule_flag(features)
features["model_name"] = config.ML_MODEL_NAME
features["model_version"] = str(version)
features["scored_at"] = datetime.now(timezone.utc).replace(tzinfo=None)

columns = list(contract.BY_TABLE[config.GOLD_SPEED_ANOMALIES].columns)
scored = spark.createDataFrame(features[columns])
gold.overwrite_day(scored, config.GOLD_SPEED_ANOMALIES, day)
contract.apply_comments(spark, config.GOLD_SPEED_ANOMALIES)

print(f"{day}: {int(features['is_anomaly'].sum())} anomalías de {len(features)} buque-días")
display(spark.table(config.GOLD_SPEED_ANOMALIES).where(F.col("day") == day).orderBy(F.desc("anomaly_score")).limit(20))
