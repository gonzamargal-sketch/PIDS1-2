"""
PIDS Parte 2 — API · Instrumentación de query_log (P3, T3.1).

Esta es la pieza que más importa de T3.1: si query_log se rellena desde
el primer endpoint, v_latencia_por_tier (postgres/init/02_funciones.sql)
empieza a devolver percentiles reales desde el día uno, y las métricas de
la semana 3 salen solas en vez de tener que inventarlas al final.
"""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from psycopg2.extensions import connection as PGConnection


class Cronometro:
    """Mide cuánto tarda un bloque, en milisegundos."""

    def __init__(self) -> None:
        self._inicio = time.perf_counter()

    @property
    def transcurrido_ms(self) -> float:
        return round((time.perf_counter() - self._inicio) * 1000, 2)


@contextmanager
def cronometrar() -> Iterator[Cronometro]:
    yield Cronometro()


def ahora_utc() -> datetime:
    return datetime.now(timezone.utc)


def registrar_consulta(
    conn: PGConnection,
    *,
    endpoint: str,
    data_source: str,
    rango_desde: datetime | None,
    rango_hasta: datetime | None,
    filas: int,
    latencia_ms: float,
    parametros: dict[str, Any] | None = None,
) -> None:
    """Inserta una fila en query_log y confirma la transacción.

    data_source debe ser 'hot' | 'cold' | 'mixto' (CHECK de la tabla).
    Para endpoints que no tocan datos de viajes (p.ej. /lifecycle/policy)
    se usa 'hot' porque la consulta en sí va contra PostgreSQL.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO query_log
                (endpoint, data_source, rango_desde, rango_hasta,
                 filas, latencia_ms, parametros)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                endpoint,
                data_source,
                rango_desde,
                rango_hasta,
                filas,
                latencia_ms,
                json.dumps(parametros or {}, default=str),
            ),
        )
    conn.commit()
