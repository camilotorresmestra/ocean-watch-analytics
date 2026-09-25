# ocean-watch-analytics

Proyecto del curso Soluciones Intensivas en Datos (MINE 4213, Uniandes 2026-20). OceanWatch Analytics: el lakehouse del tráfico marítimo, construido en Databricks Free Edition con datos AIS de NOAA.

## Contenido del repositorio

| Archivo | Descripción |
|---|---|
| `ingest.ipynb` | Notebook único de la Entrega 1: ingesta, exploración, preguntas de negocio, almacenamiento y gobernanza |
| `bitacora.md` | Registro de avance, issues y decisiones por clase |
| `SID26.2.ProyectoEntrega1.pdf` | Enunciado de la Entrega 1 |

## Cómo ejecutar

1. Importar el repositorio en Databricks (Git folder) o subir `ingest.ipynb`.
2. Ejecutar el notebook completo en cómputo serverless. La descarga de los 7 días toma entre 20 y 30 minutos.
3. Ejecuciones posteriores omiten los días ya descargados y verificados.

## Datos

- **Fuente:** [NOAA Marine Cadastre, AIS Vessel Traffic](https://hub.marinecadastre.gov/pages/vesseltraffic). Un zip por día con un CSV de posiciones.
- **Corpus:** 1 al 7 de junio de 2023, `https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_DD.zip`. Unos 330 MB comprimidos y 900 MB descomprimidos por día, 6,0 GB en total.
- **Volumen real:** 60.533.559 filas y 31.871 `MMSI` distintos. El enunciado estima "del orden de 150 millones de filas"; la cifra real es menor y la reconciliación de 1.6 confirma que cada CSV se cargó completo (el día 1 tiene 8.808.904 filas en el CSV y en la tabla).
- **Puertos:** World Port Index de la NGA, cargado en 1.8 desde el archivo Access (`WPI.mdb`).
- **Tipos de buque:** catálogo AIS de códigos `VesselType` (ITU-R M.1371, documentado por NOAA Marine Cadastre), cargado en 3b.

## Objetos en Unity Catalog

| Objeto | Contenido | Se crea en |
|---|---|---|
| `ocean_watch` | Catálogo del proyecto | 1.1 |
| `ocean_watch.raw` | Datos tal como se publican en la fuente y tablas de referencia | 1.1 |
| `ocean_watch.raw.ais_raw` (Volume) | `csv/`: los 7 CSV. `_manifest/`: un JSON por día con la evidencia de integridad. `opt/`: variantes de la sección 4 | 1.1 |
| `ocean_watch.raw.ais` | Tabla Delta con todas las posiciones de la semana | 1.4 |
| `ocean_watch.raw.world_port_index` | Puertos del World Port Index | 1.8 |
| `ocean_watch.raw.ais_quality_report` | Diagnóstico consolidado de las reglas R1 a R9 | 2.6 |
| `ocean_watch.raw.ais_daily_profile` | Perfil de volumen por día | 2.6 |
| `ocean_watch.raw.vessel_type_catalog` | Catálogo de tipos de buque AIS | 3b |
| `ocean_watch.curated` | Tablas derivadas de la limpieza | Anexo A |
| `ocean_watch.curated.ais_descarte_total` | Resultado del descarte total | Anexo A |

## 1. Ingesta

### Flujo

| Paso | Qué hace | Evidencia que deja |
|---|---|---|
| 1.1 | Crea catálogo, esquema y Volume | Objetos en Unity Catalog con comentarios |
| 1.2 | Descarga con reintentos, verifica integridad y descomprime en el Volume | Manifiesto JSON por día y tabla resumen |
| 1.3 | Valida el encabezado de cada CSV | `assert` por archivo |
| 1.4 | Lee con esquema explícito y escribe `ocean_watch.raw.ais` | Tabla Delta |
| 1.5 | Agrega comentarios y propiedades a la tabla | `DESCRIBE TABLE EXTENDED` |
| 1.6 | Reconcilia filas del CSV contra filas de la tabla | Tabla de reconciliación por archivo |
| 1.7 | Limpieza opcional de artefactos de la primera versión | — |

### Descarga y reintentos

- La descarga se hace en streaming (bloques de 8 MB) para no cargar el zip completo en memoria.
- Hasta 5 intentos con backoff exponencial: 10, 20, 40 y 80 segundos entre intentos. Un intento se aborta si pasan 120 segundos sin recibir datos.
- Los errores HTTP 4xx (salvo 429) no se reintentan, porque indican una URL equivocada y no un fallo transitorio.
- Los nombres de archivo usan el día con dos dígitos (`AIS_2023_06_01`) y se construyen en un solo lugar para que las rutas no se desalineen.

### Verificación de integridad

Se hace dentro de cada intento; si alguna falla, el archivo se vuelve a descargar.

1. Los bytes recibidos deben coincidir con el `Content-Length` del servidor (detecta descargas truncadas).
2. El zip debe contener exactamente el CSV esperado.
3. `ZipFile.testzip()` recalcula el CRC-32 del contenido (detecta corrupción).
4. Tras descomprimir, el tamaño del CSV en el Volume debe coincidir con el declarado en el zip.

NOAA no publica checksums, así que se calcula el SHA-256 del zip y se guarda en el manifiesto junto con el ETag, los tamaños, el CRC y el número de intentos. Esto permite detectar si la fuente cambia en el futuro.

### Idempotencia

El zip se descarga al disco local (`/tmp`), se descomprime directamente en el Volume y se borra. El manifiesto se escribe al final, solo si todo salió bien: es la marca de "día ingerido". Si existe el manifiesto y el CSV tiene el tamaño esperado, el día se omite.

### Validación del encabezado

Con `header=True` y un esquema explícito, Spark no compara los nombres del encabezado contra el esquema (`enforceSchema=true` por defecto). Si NOAA cambiara el orden de las columnas, los valores quedarían en la columna equivocada sin error. Por eso se compara el encabezado de cada archivo con las 17 columnas esperadas antes de leer.

### Esquema

Todas las columnas son `nullable`: son lecturas de sensores con datos imperfectos, y en la capa raw no se rechaza ninguna fila. Las limpiezas se hacen en la Entrega 2.

| Columna | Tipo | Justificación |
|---|---|---|
| `MMSI` | `STRING` | Identificador, no cantidad. Como texto se puede verificar que tenga 9 dígitos (hay valores de 7 como `9110192`) |
| `BaseDateTime` | `TIMESTAMP` | Formato `yyyy-MM-dd'T'HH:mm:ss`, en UTC. La sesión se fija en UTC |
| `LAT`, `LON` | `DOUBLE` | Grados decimales, rangos válidos [-90, 90] y [-180, 180] |
| `SOG` | `DOUBLE` | Velocidad sobre el fondo, nudos |
| `COG` | `DOUBLE` | Rumbo sobre el fondo, grados |
| `Heading` | `DOUBLE` | Viene como `511.0`. Con `INT`, Spark en modo `PERMISSIVE` dejaría toda la columna en `null` sin avisar. 511 = no disponible |
| `VesselName`, `CallSign` | `STRING` | Texto libre, con frecuencia vacío |
| `IMO` | `STRING` | Viene como `IMO9074729`, vacío o `IMO0000000` |
| `VesselType` | `INT` | Código AIS de tipo de buque (30 pesca, 7x carga, 8x tanquero…) |
| `Status` | `INT` | Estado de navegación, 0 a 15 |
| `Length`, `Width`, `Draft` | `DOUBLE` | Metros |
| `Cargo` | `INT` | Código de tipo de carga, casi siempre vacío |
| `TransceiverClass` | `STRING` | `A` o `B` |
| `_corrupt_record` | `STRING` | Línea original cuando algún campo no se pudo convertir al tipo declarado |

Se verificó localmente que las 8.808.904 filas del día 1 convierten sin errores a estos tipos.

La tabla agrega tres columnas de linaje: `source_file` (desde `_metadata.file_name`, que reemplaza a `input_file_name()` en Unity Catalog), `day` (fecha UTC de `BaseDateTime`) e `ingested_at`.

### Escritura

- Los 7 CSV se leen en una sola lectura (`csv/*.csv`) para que Spark paralelice entre archivos.
- Se escribe con nombre totalmente calificado y `mode("overwrite")`: cada ejecución reemplaza la tabla completa. La primera versión usaba `saveAsTable("ais")`, que resolvía a `workspace.default.ais` y crecía en cada ejecución.
- La tabla queda sin particionar ni clusterizar a propósito: es la línea base del experimento de almacenamiento (sección 4).

### Reconciliación

Por cada archivo se cuentan las líneas no vacías del CSV menos el encabezado y se comparan con las filas cargadas; si no coinciden, el notebook falla. También se reportan, como insumo para la exploración de calidad:

- `corrupt_rows`: filas con algún campo que no se pudo convertir.
- `rows_outside_file_day`: filas cuyo `BaseDateTime` no cae en el día del nombre del archivo.

## 2. Exploración y perfilamiento

Mide volumen, composición y calidad de `ocean_watch.raw.ais`. Una fila es una posición; un buque es un `MMSI` distinto. Las métricas de flota (dimensiones, buques únicos) deduplican por `MMSI` antes de agregar para que los buques que más transmiten no sesguen los percentiles.

Diagnóstico de calidad sobre 60.533.559 filas (detalle en la Ficha 2.6 del notebook y en `ocean_watch.raw.ais_quality_report`):

| Regla | Categoría | Filas | % |
|---|---|---|---|
| R5a `IMO` ausente | No disponible | 39.624.930 | 65,46% |
| R4 `Heading = 511` | No disponible | 33.535.628 | 55,40% |
| R5b `IMO` con formato irregular | Inválido | 400.162 | 0,66% |
| R1 `MMSI` distinto de 9 dígitos | Inválido | 49.897 | 0,08% |
| R3 `SOG` sobre 35 nudos | Inválido | 26.757 | 0,04% |
| R9 Duplicado por (`MMSI`, `BaseDateTime`) | Duplicado | 3.344 | 0,01% |
| R8 Duplicado exacto | Duplicado | 2.776 | 0,00% |
| R2 Coordenadas fuera de rango | Inválido | 0 | 0% |
| R6 `_corrupt_record` | Integridad de lectura | 0 | 0% |
| R7 Fila fuera del día del archivo | Integridad temporal | 0 | 0% |

Las reglas separan valores de no disponible (reservados por el estándar AIS), valores ausentes y valores inválidos, para no mezclar causas en un solo porcentaje de "datos sucios". El Anexo A muestra que descartar toda fila marcada eliminaría casi toda la flota Clase B, por lo que la limpieza de la Entrega 2 debe conservar los valores de no disponible.

## 3. Preguntas de negocio

Cada pregunta tiene su consulta, su plan (`explain("formatted")`) y una ficha con resultado, lectura del plan y decisión técnica.

| Pregunta | Respuesta | Decisión técnica principal |
|---|---|---|
| 3a Buques distintos por día | Entre 19.615 y 21.153 por día; 31.871 en la semana | `count_distinct` en producción: `approx_count_distinct` sobrestima entre 1,8% y 9,6%, tanto como la variación real entre días. El exacto usa dos intercambios y el aproximado uno |
| 3b Tipos de buque con más tráfico | `Towing` (31) y `Pleasure craft` (37) suman el 51,27% de las posiciones; `Cargo` y `Tanker` son los más rápidos | Catálogo de tipos como tabla gobernada, unido con `broadcast`. `SOG = 102,3` (no disponible) queda fuera del promedio |
| 3c Buques con más distancia | Entre 3.943 y 5.771 km en la semana; cruceros y buques de carga de línea regular dominan el top 10 | Se descartan segmentos con velocidad implícita sobre 35 nudos: el 0,09% de los segmentos, que sumaba el 82,9% de la distancia sin filtro. Sin filtro el primero sumaba 7,9 millones de km, imposible físicamente. Un solo intercambio por `MMSI` para deduplicar, ordenar y agregar |
| 3d Celdas con más tráfico | Seattle y San Diego concentran las celdas H3 de resolución 8 con más posiciones; 3 de 10 cruzan con el WPI | Cruce con el WPI en la celda madre de resolución 6, porque el punto del puerto cae a kilómetros del tráfico |
| 3e Buques de toda la semana | 39,74% transmitió los 7 días; 18,81% un solo día, sobre todo en Annapolis, Seattle y Fort Lauderdale | `broadcast` explícito de la lista de visitantes para evitar redistribuir 60,5 millones de filas |

## 4. Almacenamiento óptimo

Propósito declarado: el tablero diario del operador, que consulta las posiciones de un día dentro de una zona marítima (bahía de San Diego, tomada de 3d).

| Decisión | Elección |
|---|---|
| Formato de archivo | Parquet |
| Formato de tabla | Delta, necesario para `OPTIMIZE` |
| Layout | Partición por `day` y `OPTIMIZE ... ZORDER BY (LAT, LON)` dentro de cada partición |
| Tamaño de archivo | `delta.targetFileSize = 32mb` antes de `OPTIMIZE`: con el tamaño por defecto cada día quedaría en un solo archivo y el Z-order no tendría archivos que descartar |
| Variantes comparadas | CSV, Parquet por `day`, Delta por `day`, Delta por `day` con `ZORDER`; `CLUSTER BY` como variante opcional |

La evidencia se registra en las secciones 4.5 y 4.6 del notebook: bytes y archivos de la versión vigente de cada tabla (`DESCRIBE DETAIL`), archivos leídos por la consulta (`_metadata.file_path`, equivalente a `INPUT_FILE_NAME`, que Unity Catalog no admite) y planes antes y después de `OPTIMIZE`. Resultados de la semana, para la consulta de un día en la bahía de San Diego (139.844 filas en todas las variantes):

| Variante | Tamaño | % frente al CSV | Archivos | Archivos leídos |
|---|---|---|---|---|
| CSV | 6,04 GB | 100% | 7 | |
| Parquet por `day` | 1,75 GB | 29,04% | 42 | 6 (14,29%) |
| Delta por `day` | 1,78 GB | 29,47% | 7 | 1 (14,29%) |
| Delta por `day` + `ZORDER` | 1,52 GB | 25,16% | 60 | 1 (1,67%) |

La partición descarta 6 de los 7 días. El Z-order reduce los datos leídos entre 6 y 15 veces: la consulta pasa de abrir el archivo completo del día (253 a 288 MB) a abrir 1 de 60 archivos de 19 a 43 MB.

La 4.6 compara con una consulta no declarada, la trayectoria de un buque por `MMSI` durante la semana. Es la única que lee más archivos después del Z-order (de 6 a 7), porque la partición obliga a abrir al menos un archivo por día y el Z-order reparte un buque que se desplaza entre zonas. En volumen de datos también mejora (unos 190 MB frente a 1,6 GB). El costo real del layout elegido es frente a un layout por `MMSI`, que haría rápida la trayectoria y lento el tablero.

## 5. Gobernanza

- Todo queda en el catálogo `ocean_watch`: `raw` para los datos de la fuente y las tablas de referencia, `curated` para las tablas derivadas.
- La capa `raw` no se modifica después de la carga.
- Todas las tablas tienen comentario de tabla. `ais`, `ais_descarte_total`, `world_port_index` y `vessel_type_catalog` tienen además comentario por columna, y `ais` tiene propiedades con fuente y cobertura.
- Los nombres de tabla se escriben siempre completos (`catalogo.esquema.tabla`).
- La sección 5 del notebook consulta `information_schema` y muestra, por tabla, el comentario y cuántas columnas tienen comentario. Pendiente: comentarios de columna en `ais_quality_report` y `ais_daily_profile`, que hoy solo tienen comentario de tabla.

## Bitácora y aportes

`bitacora.md` registra, por fecha, los issues encontrados, las decisiones y quién hizo cada parte.
