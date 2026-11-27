"""Contrato de datos de la capa oro (requisito 5).

Es la fuente única: `apply_comments` lo escribe en Unity Catalog y `render_markdown`
genera `docs/contrato_oro.md`. Un test verifica que el documento, el contrato y el
esquema real de las tablas coincidan.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from . import config


@dataclass(frozen=True)
class TableContract:
    table: str
    description: str
    grain: str
    key: list[str]
    columns: dict[str, str]
    guarantees: list[str] = field(default_factory=list)


COMMON_GUARANTEES = [
    "Solo se construye a partir de `proyecto.equipo3_silver.ais`: ningún registro en cuarentena llega a esta tabla.",
    "Se reescribe por día con `replaceWhere`: reprocesar un día deja el mismo resultado.",
    "Frescura: el job diario actualiza el día procesado; el día se registra en la columna `day`.",
]

GOLD = [
    TableContract(
        table=config.GOLD_VESSEL_DAILY_TRACK,
        description="Trayectoria diaria por buque: actividad, distancia recorrida, velocidades y ruta simplificada.",
        grain="Un buque (MMSI) en un día UTC.",
        key=["day", "MMSI"],
        columns={
            "day": "Día UTC de las posiciones",
            "MMSI": "Identificador del buque (9 dígitos)",
            "vessel_name": "Último nombre no nulo reportado en el día",
            "vessel_type": "Último código AIS de tipo de buque no nulo reportado en el día",
            "length_m": "Eslora máxima reportada, metros",
            "transceiver_class": "Clase del transpondedor AIS (A o B)",
            "positions": "Posiciones del buque en plata para el día",
            "first_seen": "Primera posición del día (UTC)",
            "last_seen": "Última posición del día (UTC)",
            "active_hours": "Horas entre la primera y la última posición",
            "distance_km": "Distancia recorrida en km, solo con segmentos de velocidad implícita plausible",
            "segments": "Segmentos entre posiciones consecutivas",
            "segments_discarded": "Segmentos descartados por velocidad implícita sobre el máximo del tipo de buque",
            "max_implied_knots": "Máxima velocidad implícita entre posiciones consecutivas, nudos (incluye segmentos descartados)",
            "avg_sog": "Velocidad sobre el fondo promedio, nudos (sin valores de no disponible)",
            "p95_sog": "Percentil 95 de la velocidad sobre el fondo, nudos",
            "max_sog": "Velocidad sobre el fondo máxima, nudos",
            "std_sog": "Desviación estándar de la velocidad sobre el fondo, nudos",
            "stopped_ratio": "Fracción de posiciones con SOG por debajo de 0,5 nudos",
            "start_lat": "Latitud de la primera posición",
            "start_lon": "Longitud de la primera posición",
            "end_lat": "Latitud de la última posición",
            "end_lon": "Longitud de la última posición",
            "track": "Ruta simplificada: primera posición de cada intervalo de 10 minutos, ordenada por tiempo",
        },
        guarantees=["`distance_km` excluye saltos de coordenadas (segmentos sobre 35 nudos, o 60 en tipos rápidos)."],
    ),
    TableContract(
        table=config.GOLD_PORT_PRESENCE,
        description="Presencia de buques en puertos del World Port Index.",
        grain="Un buque (MMSI) en un puerto en un día UTC.",
        key=["day", "MMSI", "port_index"],
        columns={
            "day": "Día UTC de las posiciones",
            "MMSI": "Identificador del buque (9 dígitos)",
            "port_index": "Número del puerto en el World Port Index",
            "port_name": "Nombre del puerto en el World Port Index",
            "country_code": "Código de país del World Port Index",
            "vessel_name": "Último nombre no nulo reportado en el día",
            "vessel_type": "Último código AIS de tipo de buque no nulo reportado en el día",
            "arrival_ts": "Primera posición del día dentro del radio del puerto (UTC)",
            "departure_ts": "Última posición del día dentro del radio del puerto (UTC)",
            "minutes_in_port": "Minutos entre la llegada y la salida",
            "positions": "Posiciones dentro del radio del puerto",
            "stopped_positions": "Posiciones dentro del radio con SOG por debajo de 0,5 nudos",
            "min_distance_km": "Distancia mínima al punto de referencia del puerto, km",
        },
        guarantees=[
            "Una posición cuenta en el puerto si está a 8 km o menos de su punto de referencia; si hay varios, se asigna al más cercano.",
            "Si un buque sale y vuelve el mismo día, queda una sola fila con la primera llegada y la última salida.",
        ],
    ),
    TableContract(
        table=config.GOLD_TRAFFIC_CELL_HOURLY,
        description="Matriz de tráfico por celda espacial H3 (resolución 8) y hora.",
        grain="Una celda H3 de resolución 8 en una hora de un día UTC.",
        key=["day", "hour", "h3_cell"],
        columns={
            "day": "Día UTC",
            "hour": "Hora UTC, 0 a 23",
            "h3_cell": "Celda H3 de resolución 8 (BIGINT), unos 0,7 km²",
            "positions": "Posiciones en la celda durante la hora",
            "vessels": "Buques distintos en la celda durante la hora",
            "avg_sog": "Velocidad sobre el fondo promedio, nudos",
            "center_lat": "Latitud promedio de las posiciones de la celda en la hora",
            "center_lon": "Longitud promedio de las posiciones de la celda en la hora",
        },
    ),
    TableContract(
        table=config.GOLD_SPEED_ANOMALIES,
        description="Puntuación de anomalía de velocidad por buque y día, producida por el modelo registrado.",
        grain="Un buque (MMSI) en un día UTC con al menos 10 posiciones.",
        key=["day", "MMSI"],
        columns={
            "day": "Día UTC",
            "MMSI": "Identificador del buque (9 dígitos)",
            "vessel_name": "Último nombre no nulo reportado en el día",
            "vessel_type": "Último código AIS de tipo de buque no nulo reportado en el día",
            "avg_sog": "Velocidad sobre el fondo promedio, nudos",
            "max_sog": "Velocidad sobre el fondo máxima, nudos",
            "max_implied_knots": "Máxima velocidad implícita entre posiciones consecutivas, nudos",
            "distance_km": "Distancia recorrida en el día, km",
            "anomaly_score": "Puntuación del Isolation Forest; mayor es más anómalo",
            "is_anomaly": "Verdadero si el modelo marca el día del buque como anómalo",
            "rule_flag": "Verdadero si las reglas físicas marcan el día (referencia independiente del modelo)",
            "model_name": "Modelo registrado en Unity Catalog",
            "model_version": "Versión del modelo que produjo la puntuación",
            "scored_at": "Momento de la puntuación (UTC)",
        },
    ),
]

BY_TABLE = {c.table: c for c in GOLD}


def _sql_str(s: str) -> str:
    return s.replace("'", "\\'")


def apply_comments(spark, table: str) -> None:
    c = BY_TABLE[table]
    spark.sql(f"COMMENT ON TABLE {table} IS '{_sql_str(c.description + ' Grano: ' + c.grain)}'")
    for col, comment in c.columns.items():
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN `{col}` COMMENT '{_sql_str(comment)}'")
    spark.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES ("
        f"'layer' = 'gold', 'contract' = 'docs/contrato_oro.md', 'key' = '{', '.join(c.key)}')"
    )


def render_markdown() -> str:
    lines = [
        "# Contrato de datos de la capa oro",
        "",
        "<!-- Generado desde src/oceanwatch/contract.py con `python -m oceanwatch.contract`. No editar a mano. -->",
        "",
        "Consumidores: la consola de OceanWatch (tablero) y el modelo de anomalías de velocidad.",
        "",
        "## Garantías comunes",
        "",
        *[f"- {g}" for g in COMMON_GUARANTEES],
        "",
        "## Acceso",
        "",
        f"- `{config.GROUP_ANALYSTS}`: lectura de `{config.CATALOG}.{config.SCHEMA_GOLD}`.",
        f"- `{config.GROUP_SCIENTISTS}`: lectura de plata y oro, escritura en `{config.CATALOG}.{config.SCHEMA_ML}`.",
        f"- `{config.GROUP_ENGINEERS}`: escritura en todas las capas.",
        "",
    ]
    for c in GOLD:
        lines += [
            f"## `{c.table}`",
            "",
            c.description,
            "",
            f"- **Grano:** {c.grain}",
            f"- **Llave:** {', '.join(f'`{k}`' for k in c.key)}",
            *[f"- {g}" for g in c.guarantees],
            "",
            "| Columna | Descripción |",
            "|---|---|",
            *[f"| `{col}` | {desc} |" for col, desc in c.columns.items()],
            "",
        ]
    return "\n".join(lines)


if __name__ == "__main__":
    print(render_markdown(), end="")
