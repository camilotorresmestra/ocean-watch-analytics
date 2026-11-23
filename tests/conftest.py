"""Spark local con Delta y emulación de las funciones H3 nativas de Databricks.

En Databricks `h3_longlatash3`, `h3_toparent` y `h3_kring` son funciones SQL nativas.
Aquí se registran como UDF con la librería `h3`, que produce los mismos identificadores.
"""

import os
import shutil
import sys
import tempfile
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
# Los workers de Spark deben usar el mismo intérprete que pytest (con h3 instalado).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
# PySpark interpreta los datetime sin zona en la zona del proceso; los datos AIS están en UTC.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    import h3
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    from pyspark.sql.types import ArrayType, LongType

    warehouse = tempfile.mkdtemp(prefix="ow_warehouse_")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("oceanwatch-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    s = configure_spark_with_delta_pip(builder).getOrCreate()

    s.udf.register(
        "h3_longlatash3",
        lambda lon, lat, res: None if lon is None or lat is None else h3.str_to_int(h3.latlng_to_cell(lat, lon, res)),
        LongType(),
    )
    s.udf.register(
        "h3_toparent",
        lambda cell, res: None if cell is None else h3.str_to_int(h3.cell_to_parent(h3.int_to_str(cell), res)),
        LongType(),
    )
    s.udf.register(
        "h3_kring",
        lambda cell, k: None if cell is None else [h3.str_to_int(c) for c in h3.grid_disk(h3.int_to_str(cell), k)],
        ArrayType(LongType()),
    )
    yield s
    s.stop()
    shutil.rmtree(warehouse, ignore_errors=True)
