"""
PIDS Parte 2 — API · Rutas de /trips (P3, T3.2).

El endpoint que enseña el router en el vídeo (§10, minuto 4:30): una
consulta solo-caliente (rápida), una solo-fría (lenta) y una que cruza la
frontera, con su data_source y su coverage.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg2.extensions import connection as PGConnection
from pyiceberg.table import Table

from api.dependencias import get_conn, get_tabla_iceberg
from api.instrumentacion import ahora_utc, cronometrar, registrar_consulta
from api.router_tiers import consultar

router = APIRouter(tags=["trips"])


def _con_tz(momento: datetime) -> datetime:
    """Un datetime sin zona se interpreta como UTC.

    Si se dejara naive, compararlo con los límites de los tramos (que
    son 00:00 UTC de cada día, con zona) lanzaría TypeError.
    """
    return momento if momento.tzinfo else momento.replace(tzinfo=timezone.utc)


@router.get("/trips")
def trips(
    desde: datetime = Query(..., description="Inicio del rango, ISO 8601. Inclusivo."),
    hasta: datetime = Query(..., description="Fin del rango, ISO 8601. Exclusivo."),
    limite: int = Query(1000, ge=1, le=100_000),
    conn: PGConnection = Depends(get_conn),
    tabla_fria: Table = Depends(get_tabla_iceberg),
) -> dict:
    """Viajes en un rango de event_time, vengan del tier que vengan.

    El rango se filtra por event_time (tiempo de sistema), que es lo que
    determina en qué tier vive cada fila. Para consultas analíticas por
    fecha de viaje habría que filtrar por tpep_pickup_datetime, pero eso
    no dice nada sobre el tier y obligaría a tocar los dos siempre.
    """
    desde, hasta = _con_tz(desde), _con_tz(hasta)
    if desde >= hasta:
        raise HTTPException(400, "El parámetro 'desde' debe ser anterior a 'hasta'.")

    with cronometrar() as crono:
        filas, plan = consultar(conn, tabla_fria, desde, hasta, limite)
        latencia_ms = crono.transcurrido_ms

        registrar_consulta(
            conn,
            endpoint="/trips",
            data_source=plan.data_source,
            rango_desde=desde,
            rango_hasta=hasta,
            filas=len(filas),
            latencia_ms=latencia_ms,
            parametros={
                "desde": desde.isoformat(),
                "hasta": hasta.isoformat(),
                "limite": limite,
                "frontera": plan.frontera.isoformat(),
            },
        )

    return {
        "data": filas,
        "meta": {
            "data_source": plan.data_source,
            "coverage": [tramo.como_coverage() for tramo in plan.tramos],
            "as_of": ahora_utc().isoformat(),
            "latency_ms": latencia_ms,
            "rows": len(filas),
        },
    }
