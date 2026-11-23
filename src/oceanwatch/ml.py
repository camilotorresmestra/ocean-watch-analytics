"""Detección de anomalías de velocidad por buque y día.

Unidad: una fila de `gold.vessel_daily_track` (buque, día). El modelo es un Isolation
Forest sobre variables de velocidad y desplazamiento: no necesita etiquetas y marca los
días de un buque que se alejan del comportamiento del resto de la flota.

El objetivo del requisito 6 es la integración reproducible (variables desde el lakehouse,
experimento en MLflow, modelo registrado en Unity Catalog y puntuación dentro del job),
no la precisión. Las mismas funciones de variables se usan para entrenar y para puntuar.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FEATURES = [
    "avg_sog",
    "p95_sog",
    "max_sog",
    "std_sog",
    "distance_km",
    "active_hours",
    "stopped_ratio",
    "max_implied_knots",
    "discarded_ratio",
    "positions_per_hour",
    "km_per_active_hour",
]

KEY = ["day", "MMSI"]
CONTEXT = ["vessel_name", "vessel_type"]

# Un día con pocas posiciones no permite calcular velocidades confiables.
MIN_POSITIONS = 10


def build_features(track: DataFrame) -> DataFrame:
    hours = F.greatest(F.col("active_hours"), F.lit(1 / 60))
    return (
        track.where(F.col("positions") >= MIN_POSITIONS)
        .withColumn("discarded_ratio", F.col("segments_discarded") / F.greatest(F.col("segments"), F.lit(1)))
        .withColumn("positions_per_hour", F.col("positions") / hours)
        .withColumn("km_per_active_hour", F.col("distance_km") / hours)
        .select(*KEY, *CONTEXT, *[F.col(c).cast("double").alias(c) for c in FEATURES])
        .fillna(0.0, subset=FEATURES)
    )


def rule_flag(features_pdf, sog_max: float = 35.0):
    """Señal de referencia, independiente del modelo, para comparar corridas.

    Un día es sospechoso por reglas si la velocidad reportada o la implícita supera el
    umbral físico, o si más del 5% de los segmentos se descartaron por saltos.
    """
    return (
        (features_pdf["max_sog"] > sog_max)
        | (features_pdf["max_implied_knots"] > sog_max)
        | (features_pdf["discarded_ratio"] > 0.05)
    )


def make_model(contamination: float, n_estimators: int = 200, seed: int = 42):
    from sklearn.ensemble import IsolationForest
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import RobustScaler

    return Pipeline(
        [
            ("scale", RobustScaler()),
            ("iforest", IsolationForest(n_estimators=n_estimators, contamination=contamination, random_state=seed)),
        ]
    )


def evaluate(model, features_pdf) -> dict:
    """Métricas comparables entre corridas (sin etiquetas)."""
    X = features_pdf[FEATURES]
    is_anomaly = model.predict(X) == -1
    scores = -model.score_samples(X)  # mayor = más anómalo
    flagged = rule_flag(features_pdf)
    n_anom = int(is_anomaly.sum())
    return {
        "rows": int(len(X)),
        "anomalies": n_anom,
        "anomaly_rate": float(is_anomaly.mean()),
        # De lo que el modelo marca, cuánto coincide con la señal de reglas.
        "rule_precision": float((is_anomaly & flagged).sum() / n_anom) if n_anom else 0.0,
        # De lo que las reglas marcan, cuánto encuentra el modelo.
        "rule_recall": float((is_anomaly & flagged).sum() / flagged.sum()) if flagged.sum() else 0.0,
        "score_p50": float(sorted(scores)[len(scores) // 2]),
    }
