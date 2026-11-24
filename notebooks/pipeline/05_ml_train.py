# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Entrenamiento del detector de anomalías de velocidad
# MAGIC
# MAGIC No corre en el job diario: se ejecuta cuando oro tiene varios días (job `ocean_watch_setup`, tarea `train`, o a mano).
# MAGIC
# MAGIC 1. **Variables** desde `gold.vessel_daily_track` con `oceanwatch.ml.build_features` (la misma función que usa la puntuación diaria).
# MAGIC 2. **Experimento** MLflow con una corrida por valor de `contamination`; cada corrida registra parámetros, las mismas métricas y el modelo con su firma.
# MAGIC 3. **Registro** en Unity Catalog (`ocean_watch.ml.speed_anomaly_detector`) de la mejor corrida, con el alias `champion`. La tarea diaria carga siempre `@champion`, así que cambiar de modelo no exige tocar el job.
# MAGIC
# MAGIC Sin etiquetas, la comparación entre corridas usa una señal independiente: las reglas físicas (`ml.rule_flag`). Se elige la corrida con mejor F1 contra esa señal. El objetivo es la integración reproducible, no la precisión.

# COMMAND ----------

# MAGIC %pip install -q scikit-learn==1.5.2

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../../src"))

import mlflow
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient

from oceanwatch import config, ml

from databricks.sdk import WorkspaceClient

mlflow.set_registry_uri("databricks-uc")
# MLflow no crea la carpeta padre del experimento: si no existe, set_experiment falla con NOT_FOUND.
WorkspaceClient().workspace.mkdirs(config.ML_EXPERIMENT_NAME.rsplit("/", 1)[0])
mlflow.set_experiment(config.ML_EXPERIMENT_NAME)

# COMMAND ----------

features = ml.build_features(spark.table(config.GOLD_VESSEL_DAILY_TRACK)).toPandas()
days = sorted(features["day"].astype(str).unique())
print(f"{len(features):,} buque-días de {len(days)} días: {days[0]} a {days[-1]}")

# COMMAND ----------

CONTAMINATIONS = [0.005, 0.01, 0.02]
runs = []
for contamination in CONTAMINATIONS:
    with mlflow.start_run(run_name=f"iforest_c{contamination}") as run:
        model = ml.make_model(contamination).fit(features[ml.FEATURES])
        metrics = ml.evaluate(model, features)
        p, r = metrics["rule_precision"], metrics["rule_recall"]
        metrics["rule_f1"] = 2 * p * r / (p + r) if p + r else 0.0

        mlflow.log_params({
            "contamination": contamination,
            "n_estimators": 200,
            "features": ",".join(ml.FEATURES),
            "min_positions": ml.MIN_POSITIONS,
            "train_days": f"{days[0]}..{days[-1]}",
        })
        mlflow.log_metrics(metrics)
        X = features[ml.FEATURES]
        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            signature=infer_signature(X, model.predict(X)),
            input_example=X.head(5),
        )
        runs.append((metrics["rule_f1"], run.info.run_id, contamination, metrics))
        print(f"contamination={contamination}: {metrics}")

# COMMAND ----------

best_f1, best_run, best_c, best_metrics = max(runs)
print(f"Mejor corrida: {best_run} (contamination={best_c}, F1 contra reglas={best_f1:.3f})")

version = mlflow.register_model(f"runs:/{best_run}/model", config.ML_MODEL_NAME)
client = MlflowClient()
client.set_registered_model_alias(config.ML_MODEL_NAME, config.ML_MODEL_ALIAS, version.version)
client.update_registered_model(
    config.ML_MODEL_NAME,
    description="Isolation Forest sobre variables diarias de velocidad por buque (gold.vessel_daily_track). Alias champion = modelo usado por el job diario.",
)
print(f"{config.ML_MODEL_NAME} versión {version.version} -> @{config.ML_MODEL_ALIAS}")
