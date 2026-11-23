"""Plata -> oro: el modelo del negocio, un día a la vez.

Oro solo lee de plata, así que ningún registro que falló una compuerta llega aquí.
Cada tabla se reescribe para el día procesado con `replaceWhere`, de modo que repetir
el día (reintento o backfill) deja el mismo resultado.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import config, geo

# ---------------------------------------------------------------------------
# Trayectorias diarias por buque
# ---------------------------------------------------------------------------


def segments(positions: DataFrame) -> DataFrame:
    """Segmentos entre posiciones consecutivas del mismo buque, con velocidad implícita.

    Un segmento es válido si su velocidad implícita no supera el máximo del tipo de buque.
    En la Entrega 1 (pregunta 3c) el 0,09% de los segmentos sumaba el 82,9% de la distancia:
    eran saltos de coordenadas, no desplazamientos reales.
    """
    w = Window.partitionBy("MMSI").orderBy("BaseDateTime")
    return (
        positions.withColumn("prev_lat", F.lag("LAT").over(w))
        .withColumn("prev_lon", F.lag("LON").over(w))
        .withColumn("prev_ts", F.lag("BaseDateTime").over(w))
        .withColumn("seg_hours", (F.unix_timestamp("BaseDateTime") - F.unix_timestamp("prev_ts")) / 3600.0)
        .withColumn("seg_km", F.when(F.col("prev_ts").isNotNull(), geo.haversine_km("prev_lat", "prev_lon", "LAT", "LON")))
        .withColumn("seg_knots", F.col("seg_km") / F.col("seg_hours") / config.KNOT_KMH)
        .withColumn(
            "seg_valid",
            F.col("prev_ts").isNotNull() & (F.col("seg_hours") > 0) & (F.col("seg_knots") <= geo.sog_limit()),
        )
    )


def vessel_daily_track(silver_day: DataFrame) -> DataFrame:
    segs = segments(silver_day)
    bucket = F.date_trunc("hour", "BaseDateTime") + F.expr(
        f"make_interval(0, 0, 0, 0, 0, floor(minute(BaseDateTime) / {config.TRACK_SAMPLE_MINUTES}) * {config.TRACK_SAMPLE_MINUTES}, 0)"
    )
    # Ruta simplificada: la primera posición de cada intervalo de TRACK_SAMPLE_MINUTES.
    wb = Window.partitionBy("MMSI", "_bucket").orderBy("BaseDateTime")
    sampled = segs.withColumn("_bucket", bucket).withColumn("_first_in_bucket", F.row_number().over(wb) == 1)
    point = F.struct(F.col("BaseDateTime").alias("ts"), F.col("LAT").alias("lat"), F.col("LON").alias("lon"))

    return sampled.groupBy("day", "MMSI").agg(
        F.max_by("VesselName", F.when(F.col("VesselName").isNotNull(), F.col("BaseDateTime"))).alias("vessel_name"),
        F.max_by("VesselType", F.when(F.col("VesselType").isNotNull(), F.col("BaseDateTime"))).alias("vessel_type"),
        F.max("Length").alias("length_m"),
        F.max("TransceiverClass").alias("transceiver_class"),
        F.count(F.lit(1)).alias("positions"),
        F.min("BaseDateTime").alias("first_seen"),
        F.max("BaseDateTime").alias("last_seen"),
        F.round((F.unix_timestamp(F.max("BaseDateTime")) - F.unix_timestamp(F.min("BaseDateTime"))) / 3600.0, 3).alias("active_hours"),
        F.round(F.sum(F.when(F.col("seg_valid"), F.col("seg_km")).otherwise(0.0)), 3).alias("distance_km"),
        F.sum(F.col("prev_ts").isNotNull().cast("int")).alias("segments"),
        F.sum((F.col("prev_ts").isNotNull() & ~F.col("seg_valid")).cast("int")).alias("segments_discarded"),
        F.round(F.max(F.when(F.col("seg_hours") > 0, F.col("seg_knots"))), 3).alias("max_implied_knots"),
        F.round(F.avg("SOG"), 3).alias("avg_sog"),
        F.round(F.expr("percentile_approx(SOG, 0.95)"), 3).alias("p95_sog"),
        F.round(F.max("SOG"), 3).alias("max_sog"),
        F.round(F.stddev("SOG"), 3).alias("std_sog"),
        F.round(F.avg((F.col("SOG") < config.STOPPED_SOG).cast("double")), 4).alias("stopped_ratio"),
        F.min_by("LAT", "BaseDateTime").alias("start_lat"),
        F.min_by("LON", "BaseDateTime").alias("start_lon"),
        F.max_by("LAT", "BaseDateTime").alias("end_lat"),
        F.max_by("LON", "BaseDateTime").alias("end_lon"),
        F.array_sort(F.collect_list(F.when(F.col("_first_in_bucket"), point))).alias("track"),
    )


# ---------------------------------------------------------------------------
# Presencia en puertos
# ---------------------------------------------------------------------------


def port_candidates(ports: DataFrame) -> DataFrame:
    """Cada puerto repetido en las celdas de su anillo, para un cruce por igualdad.

    ports: port_index, port_name, country_code, LAT, LON.
    Una celda de resolución 5 mide unos 8,5 km de lado; el anillo 1 cubre más que el
    radio de PORT_RADIUS_KM alrededor del punto del puerto.
    """
    return (
        ports.withColumn("_port_cell", geo.h3_cell(config.H3_RES_PORT))
        .withColumn("cell", F.explode(geo.h3_kring("_port_cell", 1)))
        .select(
            "cell",
            "port_index",
            "port_name",
            "country_code",
            F.col("LAT").alias("port_lat"),
            F.col("LON").alias("port_lon"),
        )
    )


def port_presence(silver_day: DataFrame, ports: DataFrame) -> DataFrame:
    """Una fila por (día, buque, puerto) con las posiciones a menos de PORT_RADIUS_KM.

    Si varios puertos quedan dentro del radio, la posición se asigna al más cercano.
    Llegada y salida son la primera y la última posición en el radio durante el día; un
    buque que sale y vuelve el mismo día queda en una sola visita.
    """
    near = (
        silver_day.withColumn("cell", geo.h3_cell(config.H3_RES_PORT))
        .join(F.broadcast(port_candidates(ports)), "cell")
        .withColumn("distance_km", geo.haversine_km("LAT", "LON", "port_lat", "port_lon"))
        .where(F.col("distance_km") <= config.PORT_RADIUS_KM)
    )
    w = Window.partitionBy("MMSI", "BaseDateTime").orderBy("distance_km", "port_index")
    nearest = near.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
    return nearest.groupBy("day", "MMSI", "port_index", "port_name", "country_code").agg(
        F.max_by("VesselName", F.when(F.col("VesselName").isNotNull(), F.col("BaseDateTime"))).alias("vessel_name"),
        F.max_by("VesselType", F.when(F.col("VesselType").isNotNull(), F.col("BaseDateTime"))).alias("vessel_type"),
        F.min("BaseDateTime").alias("arrival_ts"),
        F.max("BaseDateTime").alias("departure_ts"),
        F.round((F.unix_timestamp(F.max("BaseDateTime")) - F.unix_timestamp(F.min("BaseDateTime"))) / 60.0, 1).alias("minutes_in_port"),
        F.count(F.lit(1)).alias("positions"),
        F.sum((F.col("SOG") < config.STOPPED_SOG).cast("int")).alias("stopped_positions"),
        F.round(F.min("distance_km"), 3).alias("min_distance_km"),
    )


# ---------------------------------------------------------------------------
# Matriz de tráfico por celda y hora
# ---------------------------------------------------------------------------


def traffic_cell_hourly(silver_day: DataFrame) -> DataFrame:
    return (
        silver_day.withColumn("hour", F.hour("BaseDateTime"))
        .groupBy("day", "hour", "h3_cell")
        .agg(
            F.count(F.lit(1)).alias("positions"),
            F.countDistinct("MMSI").alias("vessels"),
            F.round(F.avg("SOG"), 3).alias("avg_sog"),
            F.round(F.avg("LAT"), 6).alias("center_lat"),
            F.round(F.avg("LON"), 6).alias("center_lon"),
        )
    )


def overwrite_day(df: DataFrame, table: str, day: str) -> None:
    """Reemplaza solo la partición del día: reintentos y backfills son idempotentes."""
    spark = df.sparkSession
    if not spark.catalog.tableExists(table):
        df.limit(0).write.format("delta").partitionBy("day").saveAsTable(table)
    (
        df.where(F.col("day") == F.lit(day).cast("date"))
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"day = DATE'{day}'")
        .saveAsTable(table)
    )
