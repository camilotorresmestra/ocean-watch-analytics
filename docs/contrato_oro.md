# Contrato de datos de la capa oro

<!-- Generado desde src/oceanwatch/contract.py con `python -m oceanwatch.contract`. No editar a mano. -->

Consumidores: la consola de OceanWatch (tablero) y el modelo de anomalías de velocidad.

## Garantías comunes

- Solo se construye a partir de `proyecto.equipo3_silver.ais`: ningún registro en cuarentena llega a esta tabla.
- Se reescribe por día con `replaceWhere`: reprocesar un día deja el mismo resultado.
- Frescura: el job diario actualiza el día procesado; el día se registra en la columna `day`.

## Acceso

- `oceanwatch_analistas`: lectura de `proyecto.equipo3_gold`.
- `oceanwatch_cientificos`: lectura de plata y oro, escritura en `proyecto.equipo3_ml`.
- `oceanwatch_ingenieros`: escritura en todas las capas.

## `proyecto.equipo3_gold.vessel_daily_track`

Trayectoria diaria por buque: actividad, distancia recorrida, velocidades y ruta simplificada.

- **Grano:** Un buque (MMSI) en un día UTC.
- **Llave:** `day`, `MMSI`
- `distance_km` excluye saltos de coordenadas (segmentos sobre 35 nudos, o 60 en tipos rápidos).

| Columna | Descripción |
|---|---|
| `day` | Día UTC de las posiciones |
| `MMSI` | Identificador del buque (9 dígitos) |
| `vessel_name` | Último nombre no nulo reportado en el día |
| `vessel_type` | Último código AIS de tipo de buque no nulo reportado en el día |
| `length_m` | Eslora máxima reportada, metros |
| `transceiver_class` | Clase del transpondedor AIS (A o B) |
| `positions` | Posiciones del buque en plata para el día |
| `first_seen` | Primera posición del día (UTC) |
| `last_seen` | Última posición del día (UTC) |
| `active_hours` | Horas entre la primera y la última posición |
| `distance_km` | Distancia recorrida en km, solo con segmentos de velocidad implícita plausible |
| `segments` | Segmentos entre posiciones consecutivas |
| `segments_discarded` | Segmentos descartados por velocidad implícita sobre el máximo del tipo de buque |
| `max_implied_knots` | Máxima velocidad implícita entre posiciones consecutivas, nudos (incluye segmentos descartados) |
| `avg_sog` | Velocidad sobre el fondo promedio, nudos (sin valores de no disponible) |
| `p95_sog` | Percentil 95 de la velocidad sobre el fondo, nudos |
| `max_sog` | Velocidad sobre el fondo máxima, nudos |
| `std_sog` | Desviación estándar de la velocidad sobre el fondo, nudos |
| `stopped_ratio` | Fracción de posiciones con SOG por debajo de 0,5 nudos |
| `start_lat` | Latitud de la primera posición |
| `start_lon` | Longitud de la primera posición |
| `end_lat` | Latitud de la última posición |
| `end_lon` | Longitud de la última posición |
| `track` | Ruta simplificada: primera posición de cada intervalo de 10 minutos, ordenada por tiempo |

## `proyecto.equipo3_gold.port_presence`

Presencia de buques en puertos del World Port Index.

- **Grano:** Un buque (MMSI) en un puerto en un día UTC.
- **Llave:** `day`, `MMSI`, `port_index`
- Una posición cuenta en el puerto si está a 8 km o menos de su punto de referencia; si hay varios, se asigna al más cercano.
- Si un buque sale y vuelve el mismo día, queda una sola fila con la primera llegada y la última salida.

| Columna | Descripción |
|---|---|
| `day` | Día UTC de las posiciones |
| `MMSI` | Identificador del buque (9 dígitos) |
| `port_index` | Número del puerto en el World Port Index |
| `port_name` | Nombre del puerto en el World Port Index |
| `country_code` | Código de país del World Port Index |
| `vessel_name` | Último nombre no nulo reportado en el día |
| `vessel_type` | Último código AIS de tipo de buque no nulo reportado en el día |
| `arrival_ts` | Primera posición del día dentro del radio del puerto (UTC) |
| `departure_ts` | Última posición del día dentro del radio del puerto (UTC) |
| `minutes_in_port` | Minutos entre la llegada y la salida |
| `positions` | Posiciones dentro del radio del puerto |
| `stopped_positions` | Posiciones dentro del radio con SOG por debajo de 0,5 nudos |
| `min_distance_km` | Distancia mínima al punto de referencia del puerto, km |

## `proyecto.equipo3_gold.traffic_cell_hourly`

Matriz de tráfico por celda espacial H3 (resolución 8) y hora.

- **Grano:** Una celda H3 de resolución 8 en una hora de un día UTC.
- **Llave:** `day`, `hour`, `h3_cell`

| Columna | Descripción |
|---|---|
| `day` | Día UTC |
| `hour` | Hora UTC, 0 a 23 |
| `h3_cell` | Celda H3 de resolución 8 (BIGINT), unos 0,7 km² |
| `positions` | Posiciones en la celda durante la hora |
| `vessels` | Buques distintos en la celda durante la hora |
| `avg_sog` | Velocidad sobre el fondo promedio, nudos |
| `center_lat` | Latitud promedio de las posiciones de la celda en la hora |
| `center_lon` | Longitud promedio de las posiciones de la celda en la hora |

## `proyecto.equipo3_gold.vessel_speed_anomalies`

Puntuación de anomalía de velocidad por buque y día, producida por el modelo registrado.

- **Grano:** Un buque (MMSI) en un día UTC con al menos 10 posiciones.
- **Llave:** `day`, `MMSI`

| Columna | Descripción |
|---|---|
| `day` | Día UTC |
| `MMSI` | Identificador del buque (9 dígitos) |
| `vessel_name` | Último nombre no nulo reportado en el día |
| `vessel_type` | Último código AIS de tipo de buque no nulo reportado en el día |
| `avg_sog` | Velocidad sobre el fondo promedio, nudos |
| `max_sog` | Velocidad sobre el fondo máxima, nudos |
| `max_implied_knots` | Máxima velocidad implícita entre posiciones consecutivas, nudos |
| `distance_km` | Distancia recorrida en el día, km |
| `anomaly_score` | Puntuación del Isolation Forest; mayor es más anómalo |
| `is_anomaly` | Verdadero si el modelo marca el día del buque como anómalo |
| `rule_flag` | Verdadero si las reglas físicas marcan el día (referencia independiente del modelo) |
| `model_name` | Modelo registrado en Unity Catalog |
| `model_version` | Versión del modelo que produjo la puntuación |
| `scored_at` | Momento de la puntuación (UTC) |
