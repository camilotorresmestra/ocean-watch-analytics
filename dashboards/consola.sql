-- Consola de OceanWatch (requisito 4): datasets del tablero de Databricks (AI/BI Dashboards).
-- Cada bloque es un dataset; todos leen la capa oro salvo el indicador de calidad, que lee
-- las métricas de plata. El parámetro :fecha es un filtro de fecha del tablero.

-- ---------------------------------------------------------------------------
-- 1. Mapa de intensidad de tráfico por celda (visualización: Point map)
--    Latitud = center_lat, longitud = center_lon, tamaño/color = positions.
-- ---------------------------------------------------------------------------
SELECT
  h3_cell,
  ROUND(AVG(center_lat), 5)        AS center_lat,
  ROUND(AVG(center_lon), 5)        AS center_lon,
  SUM(positions)                   AS positions,
  MAX(vessels)                     AS max_vessels_in_hour,
  ROUND(AVG(avg_sog), 2)           AS avg_sog
FROM ocean_watch.gold.traffic_cell_hourly
WHERE day = :fecha
GROUP BY h3_cell
ORDER BY positions DESC
LIMIT 5000;

-- ---------------------------------------------------------------------------
-- 2. Tráfico por hora del día (visualización: Line)
-- ---------------------------------------------------------------------------
SELECT hour, SUM(positions) AS positions, SUM(vessels) AS vessel_cells
FROM ocean_watch.gold.traffic_cell_hourly
WHERE day = :fecha
GROUP BY hour
ORDER BY hour;

-- ---------------------------------------------------------------------------
-- 3. Ranking de puertos (visualización: Bar)
-- ---------------------------------------------------------------------------
SELECT
  port_name,
  country_code,
  COUNT(DISTINCT MMSI)                                   AS vessels,
  COUNT(DISTINCT CASE WHEN stopped_positions > 0 THEN MMSI END) AS vessels_stopped,
  ROUND(SUM(minutes_in_port) / 60, 1)                    AS vessel_hours_in_port
FROM ocean_watch.gold.port_presence
WHERE day = :fecha
GROUP BY port_name, country_code
ORDER BY vessels DESC
LIMIT 20;

-- ---------------------------------------------------------------------------
-- 4. Buques en puerto (visualización: Table)
-- ---------------------------------------------------------------------------
SELECT
  p.port_name,
  p.MMSI,
  p.vessel_name,
  COALESCE(t.vessel_category, 'Sin tipo')  AS vessel_category,
  p.arrival_ts,
  p.departure_ts,
  p.minutes_in_port,
  p.stopped_positions
FROM ocean_watch.gold.port_presence AS p
LEFT JOIN ocean_watch.raw.vessel_type_catalog AS t ON t.VesselType = p.vessel_type
WHERE p.day = :fecha
ORDER BY p.port_name, p.arrival_ts;

-- ---------------------------------------------------------------------------
-- 5. Anomalías del día (visualización: Table + contador)
-- ---------------------------------------------------------------------------
SELECT
  a.MMSI,
  a.vessel_name,
  COALESCE(t.vessel_category, 'Sin tipo')  AS vessel_category,
  ROUND(a.anomaly_score, 4)                AS anomaly_score,
  a.rule_flag,
  a.avg_sog,
  a.max_sog,
  a.max_implied_knots,
  a.distance_km,
  a.model_version
FROM ocean_watch.gold.vessel_speed_anomalies AS a
LEFT JOIN ocean_watch.raw.vessel_type_catalog AS t ON t.VesselType = a.vessel_type
WHERE a.day = :fecha AND a.is_anomaly
ORDER BY a.anomaly_score DESC;

-- ---------------------------------------------------------------------------
-- 6. Calidad del día (visualización: Counter y Bar por regla)
-- ---------------------------------------------------------------------------
SELECT metric, category, SUM(rows) AS rows, ROUND(100 * SUM(rows) / SUM(total_rows), 3) AS pct
FROM ocean_watch.silver.quality_metrics
WHERE day = :fecha
GROUP BY metric, category
ORDER BY category, rows DESC;
