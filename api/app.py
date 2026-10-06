"""
PIDS Parte 2 — API de acceso a los tiers.

Esqueleto del paso 0 (A1): solo /health, para que el perfil core levante
verde desde el primer día. A partir de aquí el fichero es de P3 (T3.1),
que monta las dependencias, la instrumentación de query_log y registra
los routers.

A PARTIR DE ESTE COMMIT NADIE MÁS EDITA ESTE FICHERO (regla de T3.1 en
docs/TAREAS.md): T3.2 y T3.3 solo tocan sus propios módulos de rutas y
se registran aquí una única vez, lo que les permite ir en paralelo.
"""

from __future__ import annotations

from fastapi import Depends, FastAPI
from psycopg2.extensions import connection as PGConnection

from api.dependencias import dict_cursor, get_conn, get_tabla_iceberg
from api.instrumentacion import ahora_utc, cronometrar, registrar_consulta
from api.rutas_ciclo_vida import router as router_ciclo_vida
from api.rutas_control import router as router_control
from api.rutas_ingesta import router as router_ingesta
from api.rutas_metricas import router as router_metricas
from api.rutas_resumen import router as router_resumen
from api.rutas_trips import router as router_trips
from api.rutas_web import estaticos
from api.rutas_web import router as router_web
from common.lakehouse import estadisticas

app = FastAPI(title="PIDS Parte 2 · E8", version="1.0")

app.include_router(router_resumen)  # antes que /trips por claridad; no se pisan
app.include_router(router_trips)
app.include_router(router_metricas)
app.include_router(router_ciclo_vida)
app.include_router(router_ingesta)
app.include_router(router_control)
app.include_router(router_web)
app.mount("/app", estaticos, name="web")


@app.get("/health")
def health() -> dict:
    return {"estado": "ok"}


@app.get("/stats")
def stats(conn: PGConnection = Depends(get_conn)) -> dict:
    """Foto rápida de ambos tiers: filas en caliente + estadísticas del frío.

    Las estadísticas del frío salen de estadisticas() en
    common/lakehouse.py, que lee los manifiestos de Iceberg sin tocar los
    datos, así que es barato calcularlas en cada llamada.
    """
    with cronometrar() as crono:
        with dict_cursor(conn) as cur:
            cur.execute("SELECT count(*) AS filas FROM taxi_trips")
            hot = cur.fetchone()

        tabla = get_tabla_iceberg()
        cold = estadisticas(tabla)

        latencia_ms = crono.transcurrido_ms

        registrar_consulta(
            conn,
            endpoint="/stats",
            data_source="mixto",
            rango_desde=None,
            rango_hasta=None,
            filas=hot["filas"] + cold["filas"],
            latencia_ms=latencia_ms,
        )

    return {
        "data": {"hot": dict(hot), "cold": cold},
        "meta": {
            "data_source": "mixto",
            "coverage": [],
            "as_of": ahora_utc().isoformat(),
            "latency_ms": latencia_ms,
            "rows": hot["filas"] + cold["filas"],
        },
    }
