"""
PIDS Parte 2 — API · Lectura del tier frío (P3, T3.2).

Iceberg sobre MinIO con PyIceberg. Dos cosas que importan aquí:

PROYECTAR SOLO LAS COLUMNAS QUE SE PIDEN
    `selected_fields` hace que Parquet lea solo esas columnas del
    fichero. Es la ventaja del formato columnar y es lo que hace que el
    frío sea utilizable en vez de solo barato.

LA PODA LA HACE ICEBERG, NO NOSOTROS
    La tabla está particionada por mes de event_time, pero además
    Iceberg guarda min/max por fichero en los manifiestos, así que un
    filtro por día sigue descartando ficheros enteros sin abrirlos
    (§3.4 / common/lakehouse.py). Por eso el row_filter va siempre con
    las dos cotas, aunque el rango sea de un solo día.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pyiceberg.table import Table

# Mismas columnas y mismo orden que lectura_hot.COLUMNAS: los dos tiers
# tienen que devolver filas con la misma forma para poder fusionarlas.
COLUMNAS = (
    "trip_id",
    "event_time",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "vendor_id",
    "passenger_count",
    "trip_distance",
    "pu_location_id",
    "do_location_id",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "origen",
)


def _serializable(valor: Any) -> Any:
    if valor is None:
        return None
    if isinstance(valor, datetime):
        return valor.isoformat()
    if isinstance(valor, Decimal):
        return float(valor)
    return valor


def leer_cold(
    tabla: Table,
    desde: datetime,
    hasta: datetime,
    limite: int,
) -> list[dict[str, Any]]:
    """Filas del tier frío con event_time en [desde, hasta).

    `limit` se pasa al scan de PyIceberg para no materializar la
    partición entera en memoria cuando solo se piden unas cuantas filas.
    """
    filtro = (
        f"event_time >= '{desde.isoformat()}' "
        f"and event_time < '{hasta.isoformat()}'"
    )

    arrow = tabla.scan(
        row_filter=filtro,
        selected_fields=COLUMNAS,
        limit=limite,
    ).to_arrow()

    filas: list[dict[str, Any]] = []
    for fila in arrow.to_pylist():
        filas.append({clave: _serializable(valor) for clave, valor in fila.items()})

    # El scan no garantiza orden: lo impone el router al fusionar, pero se
    # ordena aquí también para que una consulta solo-fría salga ordenada.
    filas.sort(key=lambda f: f["event_time"])
    return filas
