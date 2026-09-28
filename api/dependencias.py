"""
PIDS Parte 2 — API · Dependencias compartidas (P3, T3.1).

Una conexión a PostgreSQL por request (FastAPI Depends) y un catálogo
Iceberg cacheado a nivel de proceso. Todo lo demás (router_tiers,
lectura_hot, lectura_cold, las rutas) pasa por aquí en vez de abrir sus
propias conexiones.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Generator

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PGConnection

from common.config import PG
from common.lakehouse import obtener_catalogo, obtener_tabla
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table

# Valor real de retention_policy.dataset para este proyecto (ver
# postgres/init/03_politicas.sql: los dos INSERT usan dataset='trips').
DATASET = "trips"


def get_conn() -> Generator[PGConnection, None, None]:
    """Conexión a PostgreSQL para la duración de un request.

    Conexión simple por request, sin pool: es lo que pide el esqueleto de
    T3.1. Si en la demo se nota lento por abrir/cerrar constantemente, se
    cambia por psycopg2.pool.ThreadedConnectionPool sin tocar quien la usa,
    porque todo el mundo pasa por este Depends.
    """
    conn = psycopg2.connect(**PG.kwargs)
    try:
        yield conn
    finally:
        conn.close()


def dict_cursor(conn: PGConnection):
    """Cursor que devuelve dicts en vez de tuplas posicionales."""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


@lru_cache(maxsize=1)
def _catalogo_iceberg() -> SqlCatalog:
    # El catálogo SQL abre su propia conexión a Postgres internamente
    # (common.lakehouse.obtener_catalogo); cachearlo evita reabrirla en
    # cada consulta al tier frío.
    return obtener_catalogo()


def get_tabla_iceberg() -> Table:
    """Handle a la tabla del tier frío (lakehouse.trips).

    load_table() de PyIceberg ya es barato (lee metadatos, no datos), así
    que no hace falta cachear la tabla en sí, solo el catálogo.
    """
    return obtener_tabla(catalogo=_catalogo_iceberg(), crear=False)
