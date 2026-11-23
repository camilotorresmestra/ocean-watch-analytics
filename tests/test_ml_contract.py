import os
import uuid

import numpy as np
import pandas as pd

from oceanwatch import contract, gold, ml
from test_gold import T0, positions
from datetime import timedelta


def _track(spark):
    rows = []
    for v in range(30):
        for i in range(20):
            rows.append((str(100000000 + v), T0 + timedelta(minutes=10 * i), 30.0 + 0.01 * i, -80.0, 5.0 + (v % 3), 70))
    return gold.vessel_daily_track(positions(spark, rows))


def test_features_shape(spark):
    f = ml.build_features(_track(spark)).toPandas()
    assert len(f) == 30
    assert list(f.columns) == ml.KEY + ml.CONTEXT + ml.FEATURES
    assert not f[ml.FEATURES].isna().any().any()


def test_train_log_and_load(tmp_path):
    import mlflow

    rng = np.random.default_rng(0)
    df = pd.DataFrame(rng.normal(10, 2, size=(500, len(ml.FEATURES))), columns=ml.FEATURES)
    df["discarded_ratio"] = 0.0
    df["max_implied_knots"] = df["max_sog"]
    df.loc[:4, ["max_sog", "max_implied_knots"]] = 80.0  # cinco días claramente anómalos
    model = ml.make_model(0.01).fit(df[ml.FEATURES])
    metrics = ml.evaluate(model, df)
    assert metrics["anomalies"] == 5 and metrics["rule_recall"] == 1.0

    mlflow.set_tracking_uri(f"file://{tmp_path}/mlruns")
    with mlflow.start_run():
        sig = mlflow.models.infer_signature(df[ml.FEATURES], model.predict(df[ml.FEATURES]))
        info = mlflow.sklearn.log_model(model, "model", signature=sig)
    loaded = mlflow.sklearn.load_model(info.model_uri)
    assert (loaded.predict(df[ml.FEATURES]) == model.predict(df[ml.FEATURES])).all()


def test_contract_matches_gold_schemas(spark):
    rows = [("1", T0, 32.71, -117.18, 0.1, 70), ("1", T0 + timedelta(minutes=30), 32.712, -117.178, 0.2, 70)]
    df = positions(spark, rows)
    ports = spark.createDataFrame([(1, "SAN DIEGO", "US", 32.7157, -117.1739)],
                                  "port_index int, port_name string, country_code string, LAT double, LON double")
    built = {
        "vessel_daily_track": gold.vessel_daily_track(df),
        "port_presence": gold.port_presence(df, ports),
        "traffic_cell_hourly": gold.traffic_cell_hourly(df),
    }
    for c in contract.GOLD:
        name = c.table.split(".")[-1]
        if name in built:
            assert built[name].columns == list(c.columns), name


def test_contract_doc_is_current():
    path = os.path.join(os.path.dirname(__file__), "..", "docs", "contrato_oro.md")
    assert open(path, encoding="utf-8").read() == contract.render_markdown()
