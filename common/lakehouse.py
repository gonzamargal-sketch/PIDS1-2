"""
PIDS Parte 2 — Tier frío: tabla Iceberg sobre MinIO.

Usamos PyIceberg y no Spark+Iceberg a propósito. Spark sigue en el
proyecto para el streaming, que es donde aporta, pero hacer que escriba
en Iceberg exige encajar versiones de Spark, Scala, iceberg-spark-runtime,
hadoop-aws y el SDK de AWS. Es donde se atascan estos proyectos y no
tenemos tres semanas para pelearnos con JARs.

PyIceberg es Iceberg en Python puro: mismo formato, mismos snapshots,
mismos metadatos, sin JVM. A nuestro volumen va sobrado.

    catálogo   SQL sobre el PostgreSQL que ya tenemos (un contenedor menos
               que un Hive Metastore)
    almacén    s3://lakehouse/ en MinIO
    partición  por MES de event_time
    formato    Parquet comprimido con ZSTD

POR QUÉ PARTICIÓN MENSUAL Y NO DIARIA
    Con años de datos, particionar por día son 365 particiones al año y un
    problema de ficheros pequeños que degrada las consultas. Iceberg guarda
    estadísticas min/max por fichero en los manifiestos, así que con
    partición mensual seguimos teniendo poda a nivel de día. Lo mejor de
    los dos mundos.

POR QUÉ ZSTD Y NO SNAPPY
    Snappy está pensado para datos que se leen constantemente. El tier frío
    se escribe una vez y se lee poco: ese es justo el perfil donde ZSTD
    gana, en torno a un 30-40% menos de tamaño. Va directo al requisito de
    "barato" de E8.
"""

from __future__ import annotations

import logging

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import (
    DecimalType, IntegerType, ListType, NestedField,
    StringType, TimestamptzType,
)

from .config import ICEBERG, MINIO, PG

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# ESQUEMA
# Refleja taxi_trips de PostgreSQL. Los ids son estables: NO los
# renuméreis, Iceberg los usa para seguir la evolución del esquema.
# ─────────────────────────────────────────────────────────────
ESQUEMA = Schema(
    NestedField(1,  "trip_id",               StringType(),      required=False),
    # Tiempo de sistema: gobierna el ciclo de vida y particiona la tabla
    NestedField(2,  "event_time",            TimestamptzType(), required=True),
    # Tiempo de negocio: cuándo ocurrió el viaje
    NestedField(3,  "tpep_pickup_datetime",  TimestamptzType(), required=True),
    NestedField(4,  "tpep_dropoff_datetime", TimestamptzType(), required=False),

    NestedField(5,  "vendor_id",             IntegerType(),     required=False),
    NestedField(6,  "passenger_count",       IntegerType(),     required=False),
    NestedField(7,  "trip_distance",         DecimalType(10, 2), required=False),
    NestedField(8,  "ratecode_id",           IntegerType(),     required=False),
    NestedField(9,  "store_and_fwd_flag",    StringType(),      required=False),
    NestedField(10, "pu_location_id",        IntegerType(),     required=False),
    NestedField(11, "do_location_id",        IntegerType(),     required=False),
    NestedField(12, "payment_type",          IntegerType(),     required=False),
    NestedField(13, "fare_amount",           DecimalType(12, 2), required=False),
    NestedField(14, "extra",                 DecimalType(10, 2), required=False),
    NestedField(15, "mta_tax",               DecimalType(10, 2), required=False),
    NestedField(16, "tip_amount",            DecimalType(12, 2), required=False),
    NestedField(17, "tolls_amount",          DecimalType(12, 2), required=False),
    NestedField(18, "improvement_surcharge", DecimalType(10, 2), required=False),
    NestedField(19, "total_amount",          DecimalType(12, 2), required=False),
    NestedField(20, "congestion_surcharge",  DecimalType(10, 2), required=False),

    NestedField(21, "origen",                StringType(),      required=False),
    NestedField(22, "fichero_origen",        StringType(),      required=False),
    NestedField(23, "esquema_version",       StringType(),      required=False),
    NestedField(24, "avisos", ListType(element_id=25,
                                       element_type=StringType(),
                                       element_required=False), required=False),
)

# Partición por mes de event_time (campo 2)
PARTICION = PartitionSpec(
    PartitionField(source_id=2, field_id=1000,
                   transform=MonthTransform(), name="event_month")
)

PROPIEDADES = {
    "write.parquet.compression-codec": ICEBERG.compresion,   # zstd
    "write.parquet.compression-level": "3",
    # Ficheros grandes desde el principio: prevenir el problema de los
    # ficheros pequeños en vez de tener que compactar después, que es
    # justo lo que PyIceberg hace peor que Spark.
    "write.target-file-size-bytes": str(128 * 1024 * 1024),
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "20",
}

# Equivalente en PyArrow, para construir los lotes que se escriben
ESQUEMA_ARROW = pa.schema([
    pa.field("trip_id",               pa.string()),
    pa.field("event_time",            pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("tpep_pickup_datetime",  pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("tpep_dropoff_datetime", pa.timestamp("us", tz="UTC")),
    pa.field("vendor_id",             pa.int32()),
    pa.field("passenger_count",       pa.int32()),
    pa.field("trip_distance",         pa.decimal128(10, 2)),
    pa.field("ratecode_id",           pa.int32()),
    pa.field("store_and_fwd_flag",    pa.string()),
    pa.field("pu_location_id",        pa.int32()),
    pa.field("do_location_id",        pa.int32()),
    pa.field("payment_type",          pa.int32()),
    pa.field("fare_amount",           pa.decimal128(12, 2)),
    pa.field("extra",                 pa.decimal128(10, 2)),
    pa.field("mta_tax",               pa.decimal128(10, 2)),
    pa.field("tip_amount",            pa.decimal128(12, 2)),
    pa.field("tolls_amount",          pa.decimal128(12, 2)),
    pa.field("improvement_surcharge", pa.decimal128(10, 2)),
    pa.field("total_amount",          pa.decimal128(12, 2)),
    pa.field("congestion_surcharge",  pa.decimal128(10, 2)),
    pa.field("origen",                pa.string()),
    pa.field("fichero_origen",        pa.string()),
    pa.field("esquema_version",       pa.string()),
    pa.field("avisos",                pa.list_(pa.string())),
])


# ─────────────────────────────────────────────────────────────
def obtener_catalogo(warehouse: str | None = None) -> SqlCatalog:
    """Catálogo Iceberg SQL respaldado por el PostgreSQL del proyecto.

    Si se pasa `warehouse` (por ejemplo file:///tmp/x), se usa el sistema
    de ficheros local en vez de MinIO. Sirve para los tests.
    """
    warehouse = warehouse or ICEBERG.warehouse or None
    if warehouse:
        conf = {"uri": PG.dsn.replace("postgresql://", "postgresql+psycopg2://"),
                "warehouse": warehouse}
    else:
        conf = {
            "uri": PG.dsn.replace("postgresql://", "postgresql+psycopg2://"),
            "warehouse": f"s3://{MINIO.bucket_lakehouse}/",
            "s3.endpoint": MINIO.url,
            "s3.access-key-id": MINIO.access_key,
            "s3.secret-access-key": MINIO.secret_key,
            # MinIO sirve los buckets por ruta, no por subdominio
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        }
    return SqlCatalog(ICEBERG.catalogo, **conf)


def obtener_tabla(catalogo: SqlCatalog | None = None,
                  crear: bool = True) -> Table:
    """Devuelve la tabla del tier frío, creándola si hace falta.

    Idempotente: se puede llamar en cada ejecución del job de archivado.
    """
    cat = catalogo or obtener_catalogo()

    try:
        cat.create_namespace(ICEBERG.namespace)
        log.info("Namespace %s creado", ICEBERG.namespace)
    except NamespaceAlreadyExistsError:
        pass

    try:
        return cat.load_table(ICEBERG.identificador)
    except NoSuchTableError:
        if not crear:
            raise
        log.info("Creando tabla %s (partición mensual, %s)",
                 ICEBERG.identificador, ICEBERG.compresion)
        return cat.create_table(
            identifier=ICEBERG.identificador,
            schema=ESQUEMA,
            partition_spec=PARTICION,
            properties=PROPIEDADES,
        )


def estadisticas(tabla: Table) -> dict:
    """Filas, bytes y ficheros del tier frío.

    Sale de los manifiestos de Iceberg, sin leer los datos, así que es
    barato. Es lo que el DAG vuelca en cold_stats para que Grafana lo
    pinte, porque Grafana no sabe leer Iceberg.
    """
    snap = tabla.current_snapshot()
    if snap is None:
        return {"filas": 0, "bytes": 0, "ficheros": 0,
                "bytes_por_fila": None, "snapshot_id": None, "particiones": 0}

    filas = bytes_ = ficheros = 0
    particiones = set()
    for tarea in tabla.scan().plan_files():
        f = tarea.file
        filas    += f.record_count
        bytes_   += f.file_size_in_bytes
        ficheros += 1
        if f.partition is not None:
            particiones.add(str(f.partition))

    return {
        "filas": filas,
        "bytes": bytes_,
        "ficheros": ficheros,
        "bytes_por_fila": round(bytes_ / filas, 2) if filas else None,
        "snapshot_id": snap.snapshot_id,
        "particiones": len(particiones),
    }


def contar_particion(tabla: Table, desde, hasta) -> int:
    """Cuenta EXACTAMENTE las filas con event_time en [desde, hasta).

    Es la VERIFICACIÓN de la máquina de estados del archivado: lo que
    devuelve esto tiene que coincidir con lo que había en Postgres antes
    de desalojar la partición. Sin esta comprobación, la mudanza no es
    segura.

    CUIDADO, aquí es fácil equivocarse: sumar `record_count` de
    `plan_files()` NO vale. plan_files() devuelve los FICHEROS que podrían
    contener filas coincidentes (tras podar por partición y por min/max),
    y record_count cuenta TODAS las filas de cada fichero, no las que casan
    con el filtro. Sobrecuenta, la verificación nunca cuadraría y el
    archivado se quedaría bloqueado para siempre.

    Hay que leer de verdad, pero proyectando una sola columna: así se lee
    solo esa columna del Parquet y sigue siendo barato.
    """
    filtro = (f"event_time >= '{desde.isoformat()}' "
              f"and event_time < '{hasta.isoformat()}'")
    return tabla.scan(row_filter=filtro,
                      selected_fields=("event_time",)).to_arrow().num_rows


# ─────────────────────────────────────────────────────────────
# CONVERSIÓN pandas -> PyArrow
#
# OJO con los decimales: PyArrow NO sabe convertir float64 a decimal128
# directamente, falla con "Got bytestring of length 8 (expected 16)".
# Hay que pasar por objetos Decimal de Python. Esta función lo encapsula
# para que nadie se lo vuelva a encontrar.
# ─────────────────────────────────────────────────────────────
def df_a_arrow(df: "pd.DataFrame") -> pa.Table:
    """Convierte un DataFrame validado al esquema Arrow de la tabla Iceberg."""
    import pandas as pd
    from decimal import Decimal

    n = len(df)
    columnas = []

    for campo in ESQUEMA_ARROW:
        nombre, tipo = campo.name, campo.type

        if nombre not in df.columns:
            columnas.append(pa.nulls(n, type=tipo))
            continue

        serie = df[nombre]

        if pa.types.is_decimal(tipo):
            escala = tipo.scale
            valores = [
                None if pd.isna(v) else Decimal(f"{float(v):.{escala}f}")
                for v in serie
            ]
            columnas.append(pa.array(valores, type=tipo))

        elif pa.types.is_timestamp(tipo):
            s = pd.to_datetime(serie, errors="coerce", utc=True)
            columnas.append(pa.array(s, type=tipo))

        elif pa.types.is_integer(tipo):
            s = pd.to_numeric(serie, errors="coerce")
            valores = [None if pd.isna(v) else int(v) for v in s]
            columnas.append(pa.array(valores, type=tipo))

        elif pa.types.is_list(tipo):
            valores = [
                list(v) if isinstance(v, (list, tuple)) else ([] if v is None else [str(v)])
                for v in serie
            ]
            columnas.append(pa.array(valores, type=tipo))

        else:  # string
            valores = [None if pd.isna(v) else str(v) for v in serie]
            columnas.append(pa.array(valores, type=tipo))

    return pa.Table.from_arrays(columnas, schema=ESQUEMA_ARROW)
