"""
PIDS Parte 2 — API · Lectura del tier caliente (P3, T3.2).

PostgreSQL, tabla taxi_trips particionada por día sobre event_time. Al
filtrar por event_time, el planificador poda particiones enteras: es
justo la razón por la que la tabla está particionada así.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from psycopg2.extensions import connection as PGConnection

from api.dependencias import dict_cursor

# Las columnas que se devuelven. Se listan explícitamente en vez de usar
# SELECT * para que hot y cold devuelvan exactamente la misma forma: si
# no, fusionar dos tramos daría filas con claves distintas según el tier.
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
    """Convierte los tipos de psycopg2 que JSON no sabe serializar."""
    if isinstance(valor, datetime):
        return valor.isoformat()
    if isinstance(valor, Decimal):
        return float(valor)
    return valor


def leer_hot(
    conn: PGConnection,
    desde: datetime,
    hasta: datetime,
    limite: int,
) -> list[dict[str, Any]]:
    """Filas del tier caliente con event_time en [desde, hasta)."""
    sql = f"""
        SELECT {', '.join(COLUMNAS)}
        FROM taxi_trips
        WHERE event_time >= %s AND event_time < %s
        ORDER BY event_time
        LIMIT %s
    """
    with dict_cursor(conn) as cur:
        cur.execute(sql, (desde, hasta, limite))
        crudas = cur.fetchall()

    return [
        {clave: _serializable(valor) for clave, valor in dict(fila).items()}
        for fila in crudas
    ]
