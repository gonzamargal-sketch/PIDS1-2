"""
PIDS Parte 2 — API · Controles de la pestaña «En vivo» (P3).

Las dos únicas rutas que escriben datos de viajes, y solo en el caliente
o en cuarentena, por el mismo camino que el flujo en vivo:

- POST /ingesta/muestras: mete unos viajes de ejemplo, elegidos para que
  se vea cada salida del contrato de datos (caliente, caliente con aviso,
  cuarentena por cada tipo de rechazo y un mensaje ilegible). Es lo que
  hace scripts/anadir_viaje.py, sin terminal.
- GET/PUT /simulador: el interruptor del simulador. No para el contenedor
  (la API no tiene acceso a Docker): cambia simulador_control.activo y el
  simulador lo lee cada segundo (simulador/control.py).

Ninguna se apunta en query_log: no son consultas, y las latencias de
Grafana son las de leer datos.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from psycopg2.extensions import connection as PGConnection
from pydantic import BaseModel

from api.dependencias import dict_cursor, get_conn
from api.instrumentacion import ahora_utc, cronometrar
from common import mensajes

router = APIRouter(tags=["controles"])

FICHERO = "frontend"
# Si el último latido es más viejo que esto, el simulador no está levantado
LATIDO_MAX_S = 5

# (qué es, qué se espera, argumentos de mensajes.viaje_manual). Uno de cada
# salida del contrato (common/validacion.py), para que un clic lo enseñe todo
MUESTRAS = [
    ("Viaje normal por Manhattan", "caliente", {}),
    ("Al aeropuerto JFK, con buena propina", "caliente",
     {"distancia": 17.5, "minutos": 38, "importe": 70.0, "propina": 15.0, "zona_destino": 132}),
    ("Devolución (importe negativo)", "caliente con aviso", {"importe": -20.0}),
    ("30 millas en 5 minutos", "caliente con aviso", {"distancia": 30.0, "minutos": 5}),
    ("600 millas", "cuarentena", {"distancia": 600.0}),
    ("Baja a la vez que sube (0 minutos)", "cuarentena", {"minutos": 0}),
    ("Zona de origen 999", "cuarentena", {"zona_origen": 999}),
]
NO_ES_JSON = "esto no es json"


def _meta(filas: int, latencia_ms: float) -> dict:
    return {
        "data_source": "hot",
        "coverage": [],
        "as_of": ahora_utc().isoformat(),
        "latency_ms": latencia_ms,
        "rows": filas,
    }


@router.post("/ingesta/muestras")
def meter_muestras(conn: PGConnection = Depends(get_conn)) -> dict:
    """Mete los viajes de ejemplo y dice dónde ha acabado cada uno."""
    with cronometrar() as crono:
        msgs = [mensajes.viaje_manual(**args, fichero_origen=FICHERO) for _, _, args in MUESTRAS]
        mensajes.asegurar_particiones(conn)
        res = mensajes.escribir(conn, msgs + [NO_ES_JSON], origen="manual")

        ids = [m["trip_id"] for m in msgs]
        with dict_cursor(conn) as cur:
            cur.execute(
                """SELECT trip_id::text, avisos FROM taxi_trips
                    WHERE trip_id = ANY(%s::uuid[]) AND event_time >= NOW() - interval '1 hour'""",
                (ids,),
            )
            en_caliente = {f["trip_id"]: f["avisos"] or [] for f in cur.fetchall()}
            cur.execute(
                """SELECT payload->>'trip_id' AS trip_id, motivos FROM trips_cuarentena
                    WHERE payload->>'trip_id' = ANY(%s) AND recibido_en >= NOW() - interval '1 hour'""",
                (ids,),
            )
            en_cuarentena = {f["trip_id"]: f["motivos"] for f in cur.fetchall()}
        conn.rollback()
        latencia_ms = crono.transcurrido_ms

    resultado = []
    for (que, esperado, _), m in zip(MUESTRAS, msgs):
        tid = m["trip_id"]
        if tid in en_caliente:
            destino, motivos = "caliente", en_caliente[tid]
        else:
            destino, motivos = "cuarentena", en_cuarentena.get(tid, [])
        resultado.append({"muestra": que, "esperado": esperado, "destino": destino,
                          "motivos": motivos, "trip_id": tid})
    resultado.append({"muestra": f"Mensaje que no es JSON: «{NO_ES_JSON}»", "esperado": "cuarentena",
                      "destino": "cuarentena", "motivos": [mensajes.MOTIVO_NO_ES_JSON], "trip_id": None})

    return {
        "data": {"caliente": res.insertados, "cuarentena": res.cuarentena, "muestras": resultado},
        "meta": _meta(len(resultado), latencia_ms),
    }


class Interruptor(BaseModel):
    activo: bool


def _estado(conn: PGConnection) -> dict:
    with dict_cursor(conn) as cur:
        try:
            cur.execute(
                """SELECT activo, cambiado_en, latido, emitidos, eps, sumidero,
                          extract(epoch FROM NOW() - latido) AS hace_s
                     FROM simulador_control WHERE id = 1"""
            )
        except Exception:  # noqa: BLE001 — base sin postgres/init/08_p3.sql
            conn.rollback()
            raise HTTPException(
                503, "Falta la tabla simulador_control: aplicad postgres/init/08_p3.sql "
                     "(docker compose exec -T postgres psql -U pids -d pids < postgres/init/08_p3.sql).")
        f = cur.fetchone()
    if f is None:
        raise HTTPException(503, "simulador_control está vacía: volved a aplicar postgres/init/08_p3.sql.")
    hace = None if f["hace_s"] is None else round(float(f["hace_s"]), 1)
    return {
        "activo": f["activo"],
        "levantado": hace is not None and hace <= LATIDO_MAX_S,
        "cambiado_en": f["cambiado_en"].isoformat(),
        "latido": f["latido"].isoformat() if isinstance(f["latido"], datetime) else None,
        "latido_hace_s": hace,
        "emitidos": f["emitidos"],
        "eps": float(f["eps"]) if f["eps"] is not None else None,
        "sumidero": f["sumidero"],
    }


@router.get("/simulador")
def ver_simulador(conn: PGConnection = Depends(get_conn)) -> dict:
    """Si el simulador está encendido y si está levantado (latido reciente)."""
    with cronometrar() as crono:
        estado = _estado(conn)
        conn.rollback()
        latencia_ms = crono.transcurrido_ms
    return {"data": estado, "meta": _meta(1, latencia_ms)}


@router.put("/simulador")
def cambiar_simulador(cuerpo: Interruptor, conn: PGConnection = Depends(get_conn)) -> dict:
    """Enciende o apaga el simulador. Tarda como mucho un segundo en notarse."""
    with cronometrar() as crono:
        _estado(conn)  # da un 503 claro si falta la tabla
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE simulador_control
                      SET activo = %s,
                          cambiado_en = CASE WHEN activo <> %s THEN NOW() ELSE cambiado_en END
                    WHERE id = 1""",
                (cuerpo.activo, cuerpo.activo),
            )
        conn.commit()
        estado = _estado(conn)
        conn.rollback()
        latencia_ms = crono.transcurrido_ms
    return {"data": estado, "meta": _meta(1, latencia_ms)}
