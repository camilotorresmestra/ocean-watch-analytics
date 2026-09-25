# Bitacora

Este doc es para hacer tracking de todos los issues y decisiones de diseño que se tomen al rededor del proyecto. 

## 2026-09-22 · Arranque del repositorio

Autor: Camilo.

- Repositorio, `.gitignore`, README y esta bitácora.
- `notebooks/00test.ipynb` con la configuración inicial del catálogo y el volumen en Databricks.
- Primera versión de `ingest.ipynb`: descarga de 3 de los 7 días y carga a `ocean_watch.raw.ais`.
- Enunciado (`SID26.2.ProyectoEntrega1.pdf`) agregado al repo como referencia.

## 2026-09-23 · Ingesta (requisito 1)

Autor: Juan.

Reescritura de `ingest.ipynb`.

Issues encontrados en la primera versión:

- `saveAsTable("ais")` sin calificar escribía en `workspace.default.ais` en vez de `ocean_watch.raw.ais`; por eso la tabla crecía en cada ejecución.
- Las rutas mezclaban `ais_2023_06_0{DAY}` y `ais_2023_06_{DAY}`, y los `try/except` ocultaban los errores de lectura.
- `Heading` estaba declarado como `IntegerType`, pero NOAA lo publica como `511.0`: en modo `PERMISSIVE` toda la columna habría quedado en `null` sin error.
- El `CREATE TABLE` no coincidía con el esquema de lectura (`NavStatus` vs `Status`, tipos distintos).
- Solo se descargaban 3 de los 7 días y no había verificación de integridad.

Decisiones:

- Descarga en streaming con reintentos y backoff exponencial (10, 20, 40, 80 s); los HTTP 4xx no se reintentan.
- Integridad: bytes recibidos vs `Content-Length`, CRC-32 con `ZipFile.testzip()` y tamaño del CSV extraído vs el declarado en el zip. NOAA no publica checksums; se guarda el SHA-256 del zip en un manifiesto por día (`_manifest/`) para trazabilidad.
- Idempotencia: un día con manifiesto y CSV del tamaño esperado no se vuelve a descargar.
- Se valida el encabezado de cada CSV porque Spark no lo compara contra el esquema explícito.
- Columna `_corrupt_record` para contar filas que no se pueden convertir al tipo declarado (insumo para la exploración de calidad).
- La tabla raw queda sin particionar a propósito: es la línea base para el experimento de almacenamiento (requisito 4).
- Verificado localmente con el día 1: 8.808.904 filas, todas convierten a los tipos declarados.

## 2026-09-23 · World Port Index y primera versión de las preguntas (requisito 3)

Autor: Nicolas.

- Carga del World Port Index a `ocean_watch.raw.world_port_index` (sección 1.8), insumo del cruce de la pregunta 3d.
- Primera versión de las preguntas 3a a 3e: buques únicos por día, tipos de buque y velocidad promedio, distancia por buque, zonas de mayor tráfico con H3 y buques de un solo día.
- Estas versiones se revisaron el 2026-09-25 (planes de ejecución, catálogo de tipos y corrección de distancias en 3c).

## 2026-09-24 · Exploración, perfilamiento y almacenamiento (requisitos 2 y 4)

Autor: Camilo.

Reescritura de la sección 2 y construcción completa de la sección 4 de `ingest.ipynb`, más un Anexo A de limpieza. Alcance: cierre del diagnóstico de calidad y del experimento de almacenamiento para un propósito.

Issues encontrados en la versión anterior de la sección 2:

- La celda de volumen por día calculaba `approx_count_distinct("BaseDateTime")` bajo el nombre `posiciones`. Esa expresión cuenta marcas de tiempo distintas y satura cerca de 86.400 (segundos por día), por lo que los valores (81.392 a 90.701) no correspondían al número real de filas. La lectura de "actividad que sube el jueves y cae el sábado" se apoyaba en ese conteo y quedó retirada.
- La regla de "fila fuera del día del archivo" comparaba `to_date(BaseDateTime)` contra la columna `day`. Como `day` se deriva en 1.4 con `F.to_date("BaseDateTime")`, la comparación enfrentaba una columna contra su propia derivación y el resultado de 0 filas era cierto por construcción, sin aportar evidencia. Se corrigió comparando contra la fecha extraída del nombre del archivo de origen (`source_file`) con `regexp_extract`.
- Faltaba por completo la regla de `SOG` sobre el umbral físico (R3), presente en el enunciado del ítem 2.
- El perfil de `Length`/`Width` corría sobre las filas (pulsos de transmisión) en vez de sobre buques distintos, lo que sesgaba los percentiles hacia los buques que transmiten con más frecuencia. Se corrigió con `dropDuplicates(["MMSI"])` antes de agregar.

Decisiones de la sección 2:

- Se documentan tres categorías de hallazgo por regla: valor de no disponible (reservado por el estándar AIS, por ejemplo `Heading = 511`, `SOG = 102.3`, `COG = 360.0`, `IMO0000000`), valor ausente (columna nula) y valor inválido (rompe un límite físico o de formato). Cada regla declara su categoría en la tabla consolidada, para no reportar un solo porcentaje de "datos sucios" que mezcle causas distintas.
- Las 8 reglas de fila (R1, R2, R3, R4, R5a, R5b, R6, R7) se calculan en un único `.agg()` sobre `ocean_watch.raw.ais`, para evitar una pasada por regla sobre 60,5 millones de filas. Los duplicados (R8, R9) requieren `groupBy` con shuffle y se calculan aparte.
- Umbral de `SOG` en 35 nudos, expuesto como constante (`SOG_MAX`). El percentil 99 de la columna es 22 nudos, lo que respalda el umbral como holgado. Se identificó que los códigos de tipo de buque 40, 51 y 55 (embarcación rápida, búsqueda y rescate, cumplimiento de la ley) superan 35 nudos en operación normal y concentran el 19,5% de las filas marcadas: queda registrado como refinamiento pendiente (umbral por tipo de buque).
- La caja envolvente real de `LAT`/`LON` (lat 0,62 a 89,66; lon -179,95 a 146,32) excede el área esperada de aguas de EE. UU. La regla de rango físico global no la captura porque son coordenadas válidas; queda registrada una regla de rango por área de interés como mejora pendiente.
- Tabla cruzada de nulos por `TransceiverClass`: confirma que `Status` y `Cargo` dependen por completo del tipo de mensaje AIS (Clase A los reporta al 100%, Clase B al 0%), mientras que `Draft` tiene un vacío real dentro de Clase A (47,5%), atribuible a que es un dato de ingreso manual.
- Resultados consolidados persistidos en Delta: `ocean_watch.raw.ais_quality_report` y `ocean_watch.raw.ais_daily_profile`, con comentario de tabla, para que el resto del equipo no repita el cálculo sobre la tabla completa.

Sección 4 (almacenamiento óptimo para un propósito):

- Propósito de consulta declarado: tablero diario del operador de tráfico, que filtra por `day` y por una zona marítima (bahía de San Diego, tomada de los resultados propios de la pregunta 3d, cruzados con el World Port Index).
- Diseño: Parquet como formato de archivo, Delta como formato de tabla (requisito para medir `OPTIMIZE`), partición por `day` (respaldada por la evidencia de R7 y por el tamaño parejo de las particiones en 2.1) y `OPTIMIZE ... ZORDER BY (LAT, LON)` dentro de cada partición para el filtro espacial.
- Se descartó agrupamiento líquido (`CLUSTER BY`) como opción principal porque sustituye a la partición y no se combina con ella; el propósito declarado siempre filtra por fecha exacta, por lo que la partición por `day` ya descarta 6 de las 7 particiones sin abrir un archivo. La variante líquida queda disponible para comparar detrás de una bandera (`EJECUTAR_LIQUID`), sin ejecutarse por defecto.
- Evidencia definida: bytes en disco y número de archivos por variante (CSV, Parquet, Delta sin y con `ZORDER`), archivos leídos por la consulta declarada (`input_file_name()`), plan de ejecución (`explain("formatted")`) antes y después de `OPTIMIZE`.
- Se declaró explícitamente el costo que el layout elegido impone sobre un propósito no declarado (reconstrucción de trayectoria por `MMSI`): el `ZORDER` por `LAT`/`LON` reparte las posiciones de un mismo buque entre más archivos. Queda medido en la sección 4.6 como tradeoff documentado, no oculto.
- Pendiente: correr las celdas de escritura de variantes y `OPTIMIZE`, y trasladar las cifras reales a las tablas de evidencia de 4.5 y 4.6 (quedaron con estructura lista y celdas vacías).
- Nota posterior: resuelto el 2026-09-25. La medición de archivos leídos pasó a `_metadata.file_path` y la corrida mostró que la trayectoria no se reparte en muchos más archivos (de 6 a 7, con menos bytes leídos). Ver la entrada de esa fecha.

Anexo A (limpieza por descarte, exploratorio):

- Se comparan dos criterios de descarte: total (elimina toda fila marcada por cualquiera de las 9 reglas) y razonado (conserva los valores de no disponible de R4 y R5a, elimina solo error de contenido y duplicados).
- El descarte total no es válido como base de análisis: R4 marca 55,40% de las filas y R5a 65,46%, ambos previstos por el estándar AIS. Aplicar el descarte total eliminaría casi toda la flota de Clase B (32,96% de las posiciones) y los tipos de buque 31 y 37, que juntos son 51,27% del tráfico.
- El resultado del descarte total se persiste en `ocean_watch.curated.ais_descarte_total` (esquema `curated`, nuevo, separado de `raw`), particionado por `day`, con comentario de tabla y comentarios de columna reutilizados de `COLUMN_COMMENTS` (1.5), para trazabilidad de gobernanza.

## 2026-09-25 · Preguntas de negocio, gobernanza y documentación (requisitos 3, 5 y 6)

Autor: Juan.

Issues encontrados:

- 3c daba distancias imposibles: el primer buque sumaba 7.931.112 km en la semana, unas 198 vueltas a la Tierra. La causa son saltos entre posiciones consecutivas (coordenadas erróneas o `MMSI` compartidos). En una prueba con el día 1, 6.260 segmentos (0,07%) sumaban el 81% de la distancia sin filtro.
- 3b agrupaba por código `VesselType` sin usar el catálogo de tipos que pide el enunciado.
- Ninguna pregunta mostraba su plan de ejecución, que el enunciado pide para justificar las decisiones.
- `world_port_index` no tenía comentarios.

Decisiones:

- 3a: `count_distinct` en producción. El aproximado sobrestima entre 1,8% y 9,6%, del mismo tamaño que la variación real entre días.
- 3b: catálogo AIS como tabla gobernada (`ocean_watch.raw.vessel_type_catalog`), unido con `broadcast`. `SOG = 102,3` fuera del promedio.
- 3c: filtros de R1, R2 y R9, y descarte de segmentos con velocidad implícita sobre `SOG_MAX`. La deduplicación usa `lag` sobre la misma ventana para que todo el cálculo use un solo intercambio.
- 3d: se documentó el cruce en resolución 6 y su limitación de borde; Seattle probablemente no cruza por esa razón.
- 3e: `broadcast` explícito de la lista de visitantes de un solo día.
- Todas las preguntas tienen `explain("formatted")` y una ficha. Las afirmaciones sobre los planes se verificaron en una corrida local de Spark 3.5 con el día 1.
- Sección 4, corregida antes de su primera corrida y probada localmente con Delta sobre el día 1:
  - `input_file_name()` reemplazado por `_metadata.file_path`, porque Unity Catalog no admite la función.
  - Bytes y archivos de las variantes Delta medidos con `DESCRIBE DETAIL`. Recorrer el directorio contaba también los archivos que `OPTIMIZE` reemplaza y que siguen en disco hasta un `VACUUM`, así que la medición "después" habría mostrado más archivos que "antes".
  - `delta.targetFileSize = 32mb` antes de `OPTIMIZE`. Con el tamaño por defecto cada día quedaba en un solo archivo, y la mejora en archivos leídos venía de la compactación, no del Z-order. Con 32 MB, la consulta del tablero lee 1 de 8 archivos.
  - El buque de ejemplo de 4.6 ya no está fijo: se elige el que pasa por la zona y más se desplaza. La prueba mostró que la trayectoria también mejora frente a la tabla sin ordenar (de 12 a 2 archivos); el texto de 4.6 se corrigió para decirlo.
- Sección 5 con el inventario de objetos y una consulta a `information_schema` como evidencia.
- README actualizado con las secciones 2 a 5 y la diferencia entre las 60.533.559 filas reales y la estimación de 150 millones del enunciado.
- Se eliminó `notebooks/00test.ipynb`: su configuración ya está en la sección 1.

Corrida completa en Databricks (serverless), sin errores. Resultados trasladados al notebook:

- 3c con filtro: el primer buque queda en 5.771 km y todos bajo la cota de 10.890 km. Se descartó el 0,09% de los segmentos, que sumaba el 82,9% de la distancia sin filtro. JUSTIN PAUL ECKSTEIN (primer lugar, 18,5 nudos sostenidos) queda señalado para revisión con el umbral por tipo de buque.
- Sección 4: con `delta.targetFileSize = 32mb`, `OPTIMIZE` dejó 60 archivos. La consulta del tablero lee 1 de 60 (1,67%) frente a 1 de 7 (14,29%) sin Z-order, y el tamaño baja de 1,78 GB a 1,52 GB. La trayectoria lee 7 archivos en vez de 6, pero unos 190 MB en vez de 1,6 GB.
- Anexo A: el descarte total conserva el 24,54% de las filas y el razonado el 99,22%.
- Sección 5: faltan comentarios de columna en `ais_quality_report` (0 de 4) y `ais_daily_profile` (1 de 4).

## Aportes por integrante

| Integrante | Aporte |
|---|---|
| Juan | Sección 1 (ingesta, integridad, esquema, reconciliación), README; sección 3 revisada (planes, catálogo de tipos, corrección de 3c) y sección 5 |
| Nicolas | Carga del World Port Index (1.8) y primera versión de las preguntas 3a a 3e |
| Camilo | Sección 2 (perfilamiento y reglas R1 a R9), diseño de la sección 4 y Anexo A |
