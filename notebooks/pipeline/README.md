# Pipeline de la Entrega 2

Cada notebook es una tarea de un job definido en [`databricks.yml`](../../databricks.yml). La lógica vive en [`src/oceanwatch`](../../src/oceanwatch) y tiene tests locales; los notebooks solo leen parámetros, llaman a esas funciones y muestran evidencia.

```
ocean_watch_setup (una vez)       ocean_watch_daily (programado, fecha=auto)                    ocean_watch_train (a demanda)
  reference_week -> setup           landing -> hay_dia -> bronze -> silver -> gold -> ml_score     train
```

| Notebook | Job / tarea | Qué hace | Requisito |
|---|---|---|---|
| `00_reference_week.py` | setup / reference_week | Esquema `equipo3_raw` y sus tablas: la semana de la Entrega 1 en `raw.ais` (insumo de las celdas navegables), el World Port Index desde `data/reference/` y el catálogo de tipos de buque | 2 |
| `00_setup.py` | setup / setup | Esquemas por capa, Volume, referencias (máscara de tierra, celdas navegables, puertos), tablas de plata y permisos por rol | 2, 5 |
| `01_landing.py` | daily / landing | Descarga y verifica el zip del día; deja el CSV en `landing/`. Publica la fecha resuelta como valor de tarea | 1, 3 |
| `02_bronze.py` | daily / bronze | Auto Loader a `bronze.ais` con checkpoint; solo append | 1 |
| `03_silver.py` | daily / silver | Compuertas, cuarentena con causa, deduplicación por (`MMSI`, `BaseDateTime`) con `MERGE`, métricas de calidad | 1, 2 |
| `04_gold.py` | daily / gold | `vessel_daily_track`, `port_presence`, `traffic_cell_hourly` para la fecha; contrato como comentarios | 2, 5 |
| `05_ml_train.py` | train / train | Experimento MLflow (3 corridas por entrenamiento), registro en Unity Catalog y alias `champion` | 6 |
| `06_ml_score.py` | daily / ml_score | Puntúa la fecha con `@champion` y escribe `gold.vessel_speed_anomalies` | 6 |

Los notebooks usan el formato fuente de Databricks (`.py` con `# COMMAND ----------`): en Databricks se abren como notebooks y en git se revisan como código.

## Puesta en marcha

Requiere la [CLI de Databricks](https://docs.databricks.com/dev-tools/cli/) autenticada contra el workspace institucional con OAuth (`databricks auth login --host <workspace> --profile institucional`; el workspace no permite tokens personales) y el catálogo `proyecto` con `USE CATALOG` y `CREATE SCHEMA` para el grupo. El job de setup crea todo lo demás, incluida la semana de referencia de la Entrega 1.

```bash
databricks bundle validate -p institucional
databricks bundle deploy -t dev -p institucional --var alert_email=<correo del equipo>
databricks bundle run ocean_watch_setup -t dev
databricks bundle run ocean_watch_daily -t dev                          # entrega 2023-06-08
databricks bundle run ocean_watch_daily -t dev                          # 2023-06-09, y así
databricks bundle run ocean_watch_train -t dev                          # con varios días en oro
```

`dev` despliega en `/Workspace/Shared/ocean_watch_analytics/<usuario>/dev` con jobs `[dev <usuario>] …`, para probar cambios antes del PR. `prod` (`/Workspace/Shared/ocean_watch_analytics/prod`) lo despliega la GitHub Action [`deploy.yml`](../../.github/workflows/deploy.yml) en cada merge a `main`, después de los tests y de `bundle validate`, solo si la variable del repo `DEPLOY_ENABLED` vale `true`. Usa los secretos del repo `DATABRICKS_HOST` y `DATABRICKS_TOKEN`. Hoy está apagado: el workspace institucional no permite tokens personales, así que `prod` se despliega a mano desde `main`, con sesión OAuth: `databricks bundle deploy -t prod -p institucional`.

Para la ejecución programada: `databricks bundle deploy -t dev --var alert_email=... --var schedule_status=UNPAUSED`. En `prod` el horario está despausado por defecto.

Si el mismo computador ya desplegó el bundle en otro workspace (por ejemplo, Free Edition), mover antes `.databricks/bundle/<target>` a otra carpeta: la CLI reutiliza ese estado local y trataría de actualizar los recursos del otro workspace.

**Permisos.** Los jobs, la consola y la carpeta del bundle dan `CAN_MANAGE` a `mine-group3`. El código y el tablero viven en `/Workspace/Shared`, que todo usuario del workspace puede editar por herencia; por eso la CI valida sin `--strict`, que convierte ese aviso en error.

**dev y prod comparten tablas.** `config.py` fija `proyecto.equipo3_*` para los dos targets, así que una corrida de `dev` escribe en las mismas tablas, Volume y checkpoints que `prod`. No correr los dos a la vez.

## Demostraciones para la sustentación

| Qué | Cómo | Evidencia |
|---|---|---|
| Checkpoint de bronce | Correr `daily` dos veces con la misma fecha | La segunda corrida de `bronze` procesa 0 filas |
| Tolerancia a fallos de plata | `bundle run ocean_watch_daily --params simular_fallo=si` | `silver` falla en el intento 1 y termina en el reintento; conteos de plata y cuarentena iguales a una corrida sin fallo. Hecha en prod el 2026-11-26 (run 1024350428611280) |
| Dependencias | La misma corrida | `gold` y `ml_score` no arrancan hasta que `silver` termina |
| Backfill | `--params fecha=2023-06-09` sobre un día ya cargado | `landing` lo omite y `gold` reescribe el día con los mismos conteos |
| Duplicados entre lotes | Copiar a `landing/` un CSV ya cargado con otro nombre | `quality_metrics.duplicado_contra_plata` = filas del archivo; plata no crece |
| Programación | Despausar el horario | Historial de corridas disparadas por el horario. En prod: 12 seguidas, del 2026-11-26 21:00 al 2026-11-27 08:00 |
| Permisos y linaje | `00_setup` y Catalog Explorer | `SHOW GRANTS` por esquema; pestaña Lineage de `gold.port_presence` |

## Pruebas locales

```bash
pip install -r requirements-dev.txt        # requiere Java 17
pytest tests
```

Las funciones H3 nativas de Databricks se emulan con la librería `h3` (mismos identificadores). Auto Loader, el registro de modelos en Unity Catalog, los permisos y el job solo se pueden probar en Databricks.
