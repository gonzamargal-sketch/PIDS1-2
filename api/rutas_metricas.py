"""
PIDS Parte 2 — API · Rutas de /metrics (P3, T3.3).

Sirve por HTTP las vistas de métricas de §7. Grafana lee PostgreSQL
directamente (T4.4), así que este endpoint no es para los dashboards:
es para poder enseñar los números en el vídeo sin abrir un psql, y para
que cualquiera compruebe una métrica desde el navegador.
"""

from __future__ import annotations

from decimal import Decimal
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from psycopg2.extensions import connection as PGConnection

from api.dependencias import dict_cursor, get_conn
from api.instrumentacion import ahora_utc, cronometrar, registrar_consulta

router = APIRouter(tags=["metrics"])

# Las cuatro vistas que pide T3.3. La lista blanca no es decorativa: el
# nombre entra en el SQL por interpolación (no se puede parametrizar un
# identificador), así que solo pueden llegar valores de este diccionario.
VISTAS = {
    "coste_por_tier": "v_coste_por_tier",
    "latencia_por_tier": "v_latencia_por_tier",
    "calidad": "v_calidad",
    "metricas_caliente": "v_metricas_caliente",
    # Alias cómodos para el vídeo
    "coste": "v_coste_por_tier",
    "latencia": "v_latencia_por_tier",
    "caliente": "v_metricas_caliente",
}


def _serializable(valor: Any) -> Any:
    if isinstance(valor, (datetime, date)):
        return valor.isoformat()
    if isinstance(valor, Decimal):
        return float(valor)
    return valor


@router.get("/metrics/{nombre}")
def metrics(nombre: str, conn: PGConnection = Depends(get_conn)) -> dict:
    vista = VISTAS.get(nombre)
    if vista is None:
        raise HTTPException(
            404,
            f"Métrica desconocida: '{nombre}'. Disponibles: {sorted(set(VISTAS))}",
        )

    with cronometrar() as crono:
        with dict_cursor(conn) as cur:
            cur.execute(f"SELECT * FROM {vista}")
            filas = [
                {k: _serializable(v) for k, v in dict(f).items()}
                for f in cur.fetchall()
            ]
        latencia_ms = crono.transcurrido_ms

        registrar_consulta(
            conn,
            endpoint=f"/metrics/{nombre}",
            data_source="hot",
            rango_desde=None,
            rango_hasta=None,
            filas=len(filas),
            latencia_ms=latencia_ms,
            parametros={"nombre": nombre, "vista": vista},
        )

    return {
        "data": filas,
        "meta": {
            "data_source": "hot",
            "coverage": [],
            "as_of": ahora_utc().isoformat(),
            "latency_ms": latencia_ms,
            "rows": len(filas),
        },
    }
