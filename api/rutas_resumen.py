"""
PIDS Parte 2 — API · Ruta /trips/resumen (Parte 3).

Totales de un periodo (viajes, ingresos, propinas, distancia…) sumados en
los dos tiers. Es lo que consulta el chatbot para preguntas como «¿cuánto
se facturó en JFK en marzo?». Lleva el mismo meta que /trips: el chatbot
lo usa para decir de dónde sale el dato y si es fresco o histórico.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg2.extensions import connection as PGConnection
from pyiceberg.table import Table

from api.dependencias import get_conn, get_tabla_iceberg
from api.instrumentacion import ahora_utc, cronometrar, registrar_consulta
from api.resumen import Agrupar, resumir
from api.rutas_trips import _con_tz

router = APIRouter(tags=["trips"])


@router.get("/trips/resumen")
def trips_resumen(
    desde: datetime = Query(..., description="Inicio del rango, ISO 8601. Inclusivo."),
    hasta: datetime = Query(..., description="Fin del rango, ISO 8601. Exclusivo."),
    zona: int | None = Query(None, ge=1, le=265, description="Zona de recogida (pu_location_id)."),
    agrupar: Agrupar = Query("ninguno", description="Desglose: por día, por zona de recogida o ninguno."),
    top: int = Query(10, ge=1, le=265, description="Con agrupar=zona, cuántas zonas devolver (las de más ingresos)."),
    conn: PGConnection = Depends(get_conn),
    tabla_fria: Table = Depends(get_tabla_iceberg),
) -> dict:
    """Totales y medias de los viajes con event_time en [desde, hasta)."""
    desde, hasta = _con_tz(desde), _con_tz(hasta)
    if desde >= hasta:
        raise HTTPException(400, "El parámetro 'desde' debe ser anterior a 'hasta'.")

    with cronometrar() as crono:
        datos, plan = resumir(conn, tabla_fria, desde, hasta, zona, agrupar, top)
        latencia_ms = crono.transcurrido_ms
        viajes = datos["totales"]["viajes"]

        registrar_consulta(
            conn,
            endpoint="/trips/resumen",
            data_source=plan.data_source,
            rango_desde=desde,
            rango_hasta=hasta,
            filas=viajes,
            latencia_ms=latencia_ms,
            parametros={"zona": zona, "agrupar": agrupar, "top": top},
        )

    return {
        "data": datos,
        "meta": {
            "data_source": plan.data_source,
            "coverage": [tramo.como_coverage() for tramo in plan.tramos],
            "as_of": ahora_utc().isoformat(),
            "latency_ms": latencia_ms,
            "rows": viajes,
        },
    }
