# Pipeline de la Entrega 2

Cada notebook es una tarea del job de Databricks (requisito 3). El número indica el orden de ejecución y las dependencias.

| Notebook | Tarea | Requisito | Estado |
|---|---|---|---|
| `00_setup` | Catálogo, esquemas por capa, Volume y permisos por rol | 5 | Pendiente |
| `01_landing` | Descarga y descomprime el zip del día (parámetro `fecha`) en `landing/` | 1, 3 | Pendiente |
| `02_bronze` | Auto Loader de `landing/` a bronce, con checkpoint y columnas de auditoría | 1 | Primera versión |
| `03_silver` | Compuertas de calidad, cuarentena con causa, tabla de métricas y deduplicación por (`MMSI`, `BaseDateTime`) | 1, 2 | Pendiente |
| `04_gold` | Trayectorias diarias, presencia en puertos y matriz de tráfico por celda y hora | 2 | Pendiente |
| `05_ml` | Características, experimento MLflow y modelo registrado | 6 | Pendiente |

## Código compartido

Los notebooks importan `src/oceanwatch` agregando la carpeta `src` del repositorio al `sys.path`:

```python
import os, sys
sys.path.append(os.path.abspath("../../src"))

from oceanwatch import config
from oceanwatch.schema import AIS_SCHEMA, CSV_OPTIONS
```

`02_bronze` todavía usa `workspace.default` y su propia copia del esquema; se migra a `oceanwatch.config` en un PR aparte.
