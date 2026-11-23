import uuid

import pytest
from pyspark.sql import functions as F

from oceanwatch import config, rules, silver
from helpers import KANSAS, PACIFIC, SAN_DIEGO_BAY, ST_LOUIS_RIVER, bronze_df, cell, row


@pytest.fixture()
def refs(spark):
    land = spark.createDataFrame(
        [(cell(KANSAS, config.H3_RES_LAND),), (cell(ST_LOUIS_RIVER, config.H3_RES_LAND),)], "cell BIGINT"
    )
    nav = spark.createDataFrame([(cell(ST_LOUIS_RIVER, config.H3_RES_NAVIGABLE),)], "cell BIGINT")
    return land, nav


CASES = [
    ("ok", row(), []),
    ("mmsi corto", row(mmsi="9110192"), ["mmsi_formato"]),
    ("mmsi nulo", row(mmsi=None), ["mmsi_formato"]),
    ("lat fuera de rango", row(lat=95.0), ["coordenadas_en_rango"]),
    ("lon nula", row(lon=None), ["coordenadas_en_rango"]),
    ("null island", row(lat=0.0, lon=0.0), ["coordenadas_no_cero"]),
    ("tierra adentro", row(lat=KANSAS[0], lon=KANSAS[1]), ["posicion_en_agua"]),
    ("rio navegable", row(lat=ST_LOUIS_RIVER[0], lon=ST_LOUIS_RIVER[1]), []),
    ("bahia", row(lat=SAN_DIEGO_BAY[0], lon=SAN_DIEGO_BAY[1]), []),
    ("sog 50 carga", row(sog=50.0, vessel_type=70), ["velocidad_plausible"]),
    ("sog 50 rapida", row(sog=50.0, vessel_type=40), []),
    ("sog 61 rapida", row(sog=61.0, vessel_type=55), ["velocidad_plausible"]),
    ("sog negativa", row(sog=-1.0), ["velocidad_plausible"]),
    ("sog no disponible", row(sog=102.3), []),
    ("sog nula", row(sog=None), []),
    ("corrupta", row(corrupt="a,b,c"), ["lectura_valida"]),
    ("otro dia", row(ts="2023-06-09 00:00:01"), ["fecha_del_archivo"]),
    ("sin timestamp", row(ts=None), ["timestamp_presente", "fecha_del_archivo"]),
    ("varias", row(mmsi="1", sog=99.0), ["mmsi_formato", "velocidad_plausible"]),
]


@pytest.mark.parametrize("name,r,expected", CASES, ids=[c[0] for c in CASES])
def test_rules(spark, refs, name, r, expected):
    land, nav = refs
    out = rules.evaluate(rules.annotate_land(bronze_df(spark, [r]), land, nav)).collect()
    assert len(out) == 1
    assert sorted(out[0]["failed_rules"]) == sorted(expected)


def test_standardize_no_disponible(spark):
    df = bronze_df(spark, [row(heading=511.0, sog=102.3, cog=360.0, imo="IMO0000000", name="  ")])
    out = rules.standardize(df).collect()[0]
    assert out["Heading"] is None and out["SOG"] is None and out["COG"] is None and out["IMO"] is None
    assert out["VesselName"] is None
    assert sorted(out["quality_flags"]) == ["cog_no_disponible", "heading_no_disponible", "imo_no_disponible", "sog_no_disponible"]


def test_standardize_imo_irregular(spark):
    out = rules.standardize(bronze_df(spark, [row(imo="IMO123")])).collect()[0]
    assert out["IMO"] is None and out["quality_flags"] == ["imo_irregular"]


def _tables(spark):
    suffix = uuid.uuid4().hex[:8]
    names = {k: f"default.{k}_{suffix}" for k in ("silver", "quarantine", "metrics")}
    spark.sql(silver.silver_ddl(names["silver"]))
    spark.sql(silver.quarantine_ddl(names["quarantine"]))
    spark.sql(silver.metrics_ddl(names["metrics"]))
    return names


def _metric(spark, table, batch_id, metric):
    rows = spark.table(table).where((F.col("batch_id") == batch_id) & (F.col("metric") == metric)).collect()
    return sum(r["rows"] for r in rows)


def test_batch_processor_end_to_end(spark, refs):
    land, nav = refs
    t = _tables(spark)
    process = silver.make_batch_processor(spark, land, nav, t["silver"], t["quarantine"], t["metrics"], app_id=t["silver"])

    batch0 = [
        row(),  # ok
        row(),  # duplicado exacto (R8)
        row(sog=11.0, ingested="2023-06-09 00:00:05"),  # misma clave, contenido distinto (R9)
        row(mmsi="367000002", lat=KANSAS[0], lon=KANSAS[1]),  # tierra
        row(mmsi="367000003", ts="2023-06-08 11:00:00", heading=511.0),  # ok con no disponible
    ]
    process(bronze_df(spark, batch0), 0)

    s = spark.table(t["silver"])
    assert s.count() == 2
    kept = s.where("MMSI = '367000001'").collect()[0]
    assert kept["SOG"] == 10.0  # se conserva la primera en llegar
    assert kept["h3_cell"] == cell(PACIFIC, config.H3_RES_TRAFFIC)
    assert s.where("MMSI = '367000003'").collect()[0]["quality_flags"] == ["heading_no_disponible"]

    q = spark.table(t["quarantine"])
    assert q.count() == 3
    causes = sorted(tuple(sorted(r["failed_rules"])) for r in q.collect())
    assert causes == [("duplicado_mmsi_timestamp",), ("duplicado_mmsi_timestamp",), ("posicion_en_agua",)]

    assert _metric(spark, t["metrics"], 0, "aprobadas") == 2
    assert _metric(spark, t["metrics"], 0, "cuarentena") == 3
    assert _metric(spark, t["metrics"], 0, "insertadas_en_plata") == 2
    assert _metric(spark, t["metrics"], 0, "duplicado_contra_plata") == 0

    # Reintento del mismo lote (fallo y reinicio desde el checkpoint): nada se duplica.
    process(bronze_df(spark, batch0), 0)
    assert spark.table(t["silver"]).count() == 2
    assert spark.table(t["quarantine"]).count() == 3
    assert spark.table(t["metrics"]).where("batch_id = 0").count() == spark.table(t["metrics"]).where("batch_id = 0").select("day", "metric").distinct().count()

    # Un lote posterior trae otra vez una clave ya cargada (archivo reenviado).
    process(bronze_df(spark, [row(), row(mmsi="367000004")]), 1)
    assert spark.table(t["silver"]).count() == 3
    assert _metric(spark, t["metrics"], 1, "duplicado_contra_plata") == 1
    assert _metric(spark, t["metrics"], 1, "insertadas_en_plata") == 1


def test_streaming_from_bronze_with_restart(spark, refs, tmp_path):
    """Bronce Delta -> foreachBatch -> plata, con reinicio desde el checkpoint."""
    land, nav = refs
    t = _tables(spark)
    bronze_table = f"default.bronze_{uuid.uuid4().hex[:8]}"
    bronze_df(spark, [row(mmsi="367000010"), row(mmsi="1")]).write.format("delta").saveAsTable(bronze_table)
    process = silver.make_batch_processor(spark, land, nav, t["silver"], t["quarantine"], t["metrics"], app_id=t["silver"])
    checkpoint = str(tmp_path / "ckpt")

    def run():
        (
            spark.readStream.table(bronze_table)
            .writeStream.foreachBatch(process)
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
            .awaitTermination()
        )

    run()
    assert spark.table(t["silver"]).count() == 1
    assert spark.table(t["quarantine"]).count() == 1

    run()  # sin datos nuevos: el checkpoint evita reprocesar
    assert spark.table(t["silver"]).count() == 1
    assert spark.table(t["quarantine"]).count() == 1

    bronze_df(spark, [row(mmsi="367000011")]).write.format("delta").mode("append").saveAsTable(bronze_table)
    run()
    assert spark.table(t["silver"]).count() == 2


def test_simulated_failure_then_restart(spark, refs, tmp_path):
    """El lote falla tras escribir la cuarentena; al reiniciar desde el checkpoint se repite
    con el mismo batch_id y el resultado es el mismo que sin fallo."""
    land, nav = refs
    t = _tables(spark)
    bronze_table = f"default.bronze_{uuid.uuid4().hex[:8]}"
    bronze_df(spark, [row(mmsi="367000020"), row(mmsi="367000021"), row(mmsi="1")]).write.format("delta").saveAsTable(bronze_table)
    marker = str(tmp_path / "fallo" / "marker")
    process = silver.make_batch_processor(
        spark, land, nav, t["silver"], t["quarantine"], t["metrics"], app_id=t["silver"], fail_once_marker=marker
    )
    checkpoint = str(tmp_path / "ckpt")

    def run():
        (
            spark.readStream.table(bronze_table).writeStream.foreachBatch(process)
            .option("checkpointLocation", checkpoint).trigger(availableNow=True).start().awaitTermination()
        )

    with pytest.raises(Exception, match="Fallo simulado"):
        run()
    assert spark.table(t["quarantine"]).count() == 1  # se alcanzó a escribir
    assert spark.table(t["silver"]).count() == 0  # el MERGE no corrió

    run()  # reinicio
    assert spark.table(t["silver"]).count() == 2
    assert spark.table(t["quarantine"]).count() == 1  # el append repetido se ignoró
    assert _metric(spark, t["metrics"], 0, "insertadas_en_plata") == 2
