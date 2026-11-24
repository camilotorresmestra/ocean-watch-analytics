# Pipeline de la Entrega 2

Cada notebook es una tarea de un job definido en [`databricks.yml`](../../databricks.yml). La lógica vive en [`src/oceanwatch`](../../src/oceanwatch) y tiene tests locales; los notebooks solo leen parámetros, llaman a esas funciones y muestran evidencia.

```
ocean_watch_setup (una vez)       ocean_watch_daily (programado, fecha=auto)          ocean_watch_train (a demanda)
  setup                             landing -> bronze -> silver -> gold -> ml_score      train
```

| Notebook | Job / tarea | Qué hace | Requisito |
|---|---|---|---|
| `00_setup.py` | setup / setup | Catálogo, esquemas por capa, Volume, referencias (máscara de tierra, celdas navegables, puertos), tablas de plata y permisos por rol | 2, 5 |
| `01_landing.py` | daily / landing | Descarga y verifica el zip del día; deja el CSV en `landing/`. Publica la fecha resuelta como valor de tarea | 1, 3 |
| `02_bronze.py` | daily / bronze | Auto Loader a `bronze.ais` con checkpoint; solo append | 1 |
| `03_silver.py` | daily / silver | Compuertas, cuarentena con causa, deduplicación por (`MMSI`, `BaseDateTime`) con `MERGE`, métricas de calidad | 1, 2 |
| `04_gold.py` | daily / gold | `vessel_daily_track`, `port_presence`, `traffic_cell_hourly` para la fecha; contrato como comentarios | 2, 5 |
| `05_ml_train.py` | train / train | Experimento MLflow (3 corridas), registro en Unity Catalog y alias `champion` | 6 |
| `06_ml_score.py` | daily / ml_score | Puntúa la fecha con `@champion` y escribe `gold.vessel_speed_anomalies` | 6 |

Los notebooks usan el formato fuente de Databricks (`.py` con `# COMMAND ----------`): en Databricks se abren como notebooks y en git se revisan como código.

## Puesta en marcha

Requiere la [CLI de Databricks](https://docs.databricks.com/dev-tools/cli/) autenticada contra el workspace institucional y que la Entrega 1 haya dejado `ocean_watch.raw.ais` y `ocean_watch.raw.world_port_index`.

```bash
databricks bundle validate
databricks bundle deploy -t dev --var alert_email=<correo del equipo>
databricks bundle run ocean_watch_setup -t dev
databricks bundle run ocean_watch_daily -t dev                          # entrega 2023-06-08
databricks bundle run ocean_watch_daily -t dev                          # 2023-06-09, y así
databricks bundle run ocean_watch_train -t dev                          # con varios días en oro
```

`dev` despliega en `/Workspace/Shared/ocean_watch_analytics/<usuario>/dev` con jobs `[dev <usuario>] …`, para probar cambios antes del PR. `prod` (`/Workspace/Shared/ocean_watch_analytics/prod`) no se despliega a mano: lo hace la GitHub Action [`deploy.yml`](../../.github/workflows/deploy.yml) en cada merge a `main`, después de los tests y de `bundle validate`. Usa los secretos del repo `DATABRICKS_HOST` y `DATABRICKS_TOKEN`.

Para la ejecución programada: `databricks bundle deploy -t dev --var alert_email=... --var schedule_status=UNPAUSED`.

## Demostraciones para la sustentación

| Qué | Cómo | Evidencia |
|---|---|---|
| Checkpoint de bronce | Correr `daily` dos veces con la misma fecha | La segunda corrida de `bronze` procesa 0 filas |
| Tolerancia a fallos de plata | `bundle run ocean_watch_daily --params simular_fallo=si` | `silver` falla en el intento 1 y termina en el reintento; conteos de plata y cuarentena iguales a una corrida sin fallo |
| Dependencias | La misma corrida | `gold` y `ml_score` no arrancan hasta que `silver` termina |
| Backfill | `--params fecha=2023-06-09` sobre un día ya cargado | `landing` lo omite y `gold` reescribe el día con los mismos conteos |
| Duplicados entre lotes | Copiar a `landing/` un CSV ya cargado con otro nombre | `quality_metrics.duplicado_contra_plata` = filas del archivo; plata no crece |
| Programación | Despausar el horario | Historial de corridas disparadas por el horario |
| Permisos y linaje | `00_setup` y Catalog Explorer | `SHOW GRANTS` por esquema; pestaña Lineage de `gold.port_presence` |

## Pruebas locales

```bash
pip install -r requirements-dev.txt        # requiere Java 17
pytest tests
```

Las funciones H3 nativas de Databricks se emulan con la librería `h3` (mismos identificadores). Auto Loader, el registro de modelos en Unity Catalog, los permisos y el job solo se pueden probar en Databricks.
