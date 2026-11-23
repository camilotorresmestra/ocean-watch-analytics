"""Bronce -> plata: compuertas, cuarentena, deduplicación y métricas por micro-lote.

Se usa `foreachBatch` porque cada micro-lote tiene tres salidas (plata, cuarentena y
métricas) y porque la deduplicación contra lo ya cargado se resuelve con MERGE.

Tolerancia a fallos: si un micro-lote falla a mitad de camino, Structured Streaming lo
vuelve a ejecutar con el mismo `batch_id` al reiniciar desde el checkpoint.
- Plata: MERGE que solo inserta claves (MMSI, BaseDateTime) nuevas, así que repetir el
  lote no duplica filas.
- Cuarentena y métricas: escrituras Delta idempotentes (`txnAppId` + `txnVersion`): Delta
  ignora un segundo append con el mismo `batch_id`.

Límite conocido: si el fallo ocurre después del MERGE y antes de escribir las métricas, al
reintentar el MERGE ya no inserta nada y la métrica `duplicado_contra_plata` del lote
cuenta como duplicadas las filas que sí se insertaron en el primer intento.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import config, geo, rules

KEY = ["MMSI", "BaseDateTime"]

SILVER_COLUMNS = [
    "MMSI", "BaseDateTime", "day", "LAT", "LON", "SOG", "COG", "Heading",
    "VesselName", "IMO", "CallSign", "VesselType", "Status", "Length", "Width",
    "Draft", "Cargo", "TransceiverClass", "h3_cell", "quality_flags",
    "source_file", "bronze_ingested_at", "silver_processed_at",
]

QUARANTINE_EXTRA = ["failed_rules", "batch_id", "quarantined_at"]

METRICS_SCHEMA = (
    "batch_id BIGINT, day DATE, metric STRING, category STRING, rows BIGINT, "
    "total_rows BIGINT, pct DOUBLE, processed_at TIMESTAMP"
)


def silver_ddl(table: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        MMSI STRING NOT NULL, BaseDateTime TIMESTAMP NOT NULL, day DATE NOT NULL,
        LAT DOUBLE NOT NULL, LON DOUBLE NOT NULL, SOG DOUBLE, COG DOUBLE, Heading DOUBLE,
        VesselName STRING, IMO STRING, CallSign STRING, VesselType INT, Status INT,
        Length DOUBLE, Width DOUBLE, Draft DOUBLE, Cargo INT, TransceiverClass STRING,
        h3_cell BIGINT, quality_flags ARRAY<STRING>,
        source_file STRING, bronze_ingested_at TIMESTAMP, silver_processed_at TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (day)
    """


def quarantine_ddl(table: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        MMSI STRING, BaseDateTime TIMESTAMP, LAT DOUBLE, LON DOUBLE, SOG DOUBLE, COG DOUBLE,
        Heading DOUBLE, VesselName STRING, IMO STRING, CallSign STRING, VesselType INT,
        Status INT, Length DOUBLE, Width DOUBLE, Draft DOUBLE, Cargo INT,
        TransceiverClass STRING, _corrupt_record STRING, source_file STRING,
        source_modified_at TIMESTAMP, bronze_ingested_at TIMESTAMP, failed_rules ARRAY<STRING>, batch_id BIGINT,
        quarantined_at TIMESTAMP
    )
    USING DELTA
    """


def metrics_ddl(table: str) -> str:
    return f"CREATE TABLE IF NOT EXISTS {table} ({METRICS_SCHEMA}) USING DELTA"


def _idempotent_append(df: DataFrame, table: str, batch_id: int, app_id: str) -> None:
    (
        df.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", batch_id)
        .saveAsTable(table)
    )


def classify(batch: DataFrame, land_cells: DataFrame, navigable_cells: DataFrame) -> DataFrame:
    """Evalúa las reglas y marca duplicados dentro del lote.

    Devuelve el lote con `failed_rules` (incluye `duplicado_mmsi_timestamp` para todas las
    copias de una clave salvo la primera que pasa las demás reglas).
    """
    evaluated = rules.evaluate(rules.annotate_land(batch, land_cells, navigable_cells))
    # Entre las copias de una clave, se conserva la que pasa las reglas, y entre esas la
    # primera en llegar; el desempate final por hash hace el resultado determinista.
    w = Window.partitionBy(*KEY).orderBy(
        F.size("failed_rules"), "bronze_ingested_at", "source_file", F.xxhash64(*batch.columns)
    )
    dup = F.lit(rules.DUPLICATE_RULE.name)
    return (
        evaluated.withColumn("_rn", F.row_number().over(w))
        .withColumn(
            "failed_rules",
            F.when(
                (F.col("_rn") > 1) & F.col("MMSI").isNotNull() & F.col("BaseDateTime").isNotNull(),
                F.array_union("failed_rules", F.array(dup)),
            ).otherwise(F.col("failed_rules")),
        )
        .drop("_rn", "_on_land")
    )


def batch_metrics(classified: DataFrame, batch_id: int, processed_at: datetime) -> DataFrame:
    """Una fila por (día, métrica): filas totales, aprobadas y fallidas por regla."""
    day = F.coalesce(rules.file_day(), F.to_date("BaseDateTime"))
    all_rules = rules.RULES + [rules.DUPLICATE_RULE]
    aggs = [F.count(F.lit(1)).alias("total_rows")]
    aggs.append(F.sum((F.size("failed_rules") == 0).cast("long")).alias("aprobadas"))
    aggs.append(F.sum((F.size("failed_rules") > 0).cast("long")).alias("cuarentena"))
    aggs += [
        F.sum(F.array_contains("failed_rules", r.name).cast("long")).alias(r.name) for r in all_rules
    ]
    wide = classified.groupBy(day.alias("day")).agg(*aggs)

    metric_cat = [("aprobadas", "resumen"), ("cuarentena", "resumen")] + [(r.name, r.category) for r in all_rules]
    stack = "stack({n}, {pairs}) AS (metric, category, rows)".format(
        n=len(metric_cat),
        pairs=", ".join(f"'{m}', '{c}', `{m}`" for m, c in metric_cat),
    )
    return (
        wide.selectExpr("day", "total_rows", stack)
        .withColumn("batch_id", F.lit(batch_id).cast("bigint"))
        .withColumn("pct", F.round(100 * F.col("rows") / F.col("total_rows"), 4))
        .withColumn("processed_at", F.lit(processed_at).cast("timestamp"))
        .select("batch_id", "day", "metric", "category", "rows", "total_rows", "pct", "processed_at")
    )


def to_silver_rows(passed: DataFrame, processed_at: datetime) -> DataFrame:
    return (
        rules.standardize(passed)
        .withColumn("day", F.to_date("BaseDateTime"))
        .withColumn("h3_cell", geo.h3_cell(config.H3_RES_TRAFFIC))
        .withColumn("silver_processed_at", F.lit(processed_at).cast("timestamp"))
        .select(*SILVER_COLUMNS)
    )


def merge_into_silver(spark: SparkSession, rows: DataFrame, table: str) -> dict:
    """Inserta solo las claves nuevas. Devuelve las métricas de la operación MERGE."""
    target = DeltaTable.forName(spark, table)
    (
        target.alias("t")
        .merge(
            rows.alias("s"),
            # `day` primero: permite descartar particiones del destino.
            "t.day = s.day AND t.MMSI = s.MMSI AND t.BaseDateTime = s.BaseDateTime",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    last = target.history(1).select("operation", "operationMetrics").collect()[0]
    return dict(last["operationMetrics"]) if last["operation"] == "MERGE" else {}


def make_batch_processor(
    spark: SparkSession,
    land_cells: DataFrame,
    navigable_cells: DataFrame,
    silver_table: str = config.SILVER_AIS,
    quarantine_table: str = config.SILVER_QUARANTINE,
    metrics_table: str = config.SILVER_QUALITY_METRICS,
    app_id: str = "ocean_watch_silver_ais",
    use_persist: bool = False,
    fail_once_marker: str | None = None,
):
    """Función para `writeStream.foreachBatch`.

    fail_once_marker: ruta de un archivo marcador para la demostración de tolerancia a
    fallos. Si se indica y el archivo no existe, el primer lote falla a propósito después
    de escribir la cuarentena y antes del MERGE (crea el marcador para no volver a fallar).
    """

    def process(batch: DataFrame, batch_id: int) -> None:
        processed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        classified = classify(batch, land_cells, navigable_cells)
        # El cómputo serverless no admite persist; en un clúster clásico ahorra recalcular
        # el lote para cada una de las tres salidas.
        if use_persist:
            classified = classified.persist()

        metrics = batch_metrics(classified, batch_id, processed_at)

        quarantine = (
            classified.where(F.size("failed_rules") > 0)
            .withColumn("batch_id", F.lit(batch_id).cast("bigint"))
            .withColumn("quarantined_at", F.lit(processed_at).cast("timestamp"))
        )
        _idempotent_append(quarantine, quarantine_table, batch_id, f"{app_id}_quarantine")

        if fail_once_marker and not os.path.exists(fail_once_marker):
            os.makedirs(os.path.dirname(fail_once_marker) or ".", exist_ok=True)
            open(fail_once_marker, "w").close()
            raise RuntimeError(f"Fallo simulado en el lote {batch_id} (demostración de tolerancia a fallos)")

        passed = classified.where(F.size("failed_rules") == 0)
        merge = merge_into_silver(spark, to_silver_rows(passed, processed_at), silver_table)

        # Duplicados contra lo que ya estaba en plata (llegadas repetidas en otro lote).
        inserted = int(merge.get("numTargetRowsInserted", 0))
        source = int(merge.get("numSourceRows", 0))
        extra = spark.createDataFrame(
            [(batch_id, "insertadas_en_plata", "resumen", inserted), (batch_id, "duplicado_contra_plata", "duplicado", source - inserted)],
            "batch_id BIGINT, metric STRING, category STRING, rows BIGINT",
        )
        totals = metrics.select("batch_id", "day", "total_rows", "processed_at").distinct()
        # La métrica de MERGE es del lote completo; se reporta en el primer día del lote.
        first_day = totals.orderBy("day").limit(1)
        extra = (
            extra.join(first_day, "batch_id")
            .withColumn("pct", F.round(100 * F.col("rows") / F.col("total_rows"), 4))
            .select("batch_id", "day", "metric", "category", "rows", "total_rows", "pct", "processed_at")
        )
        _idempotent_append(metrics.unionByName(extra), metrics_table, batch_id, f"{app_id}_metrics")

        if use_persist:
            classified.unpersist()

    return process
