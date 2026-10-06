"""
PIDS Parte 2 — API · Ruta /ingesta (P3, pestaña «En vivo» del frontend).

Lo que está entrando AHORA: viajes por intervalo que llegan al caliente y
a cuarentena, a qué ritmo, cuándo llegó el último y con cuánto retraso
respecto a su evento. Es para enseñar el flujo en vivo desde el navegador
en vez de con logs y psql.

Dos decisiones:

1. Se cuenta por hora de LLEGADA (ingested_at / recibido_en), no por
   event_time. Si el consumidor se cae, los eventos esperan en Kafka y
   llegan de golpe al volver: por llegada se ve el hueco y luego el
   pico, y el retraso (llegada − evento) sube y vuelve a ~0. Es lo mismo
   que el LAG de Kafka, medido en segundos y sin hablar con Kafka.
2. NO se apunta en query_log. El frontend la llama cada 2 s: si contara,
   las latencias del caliente de Grafana serían casi todas de esta ruta
   y no de las consultas a /trips, que son las que mide la métrica.

Se excluye origen='carga_inicial': esto es el flujo en vivo (stream y
manual), y una recarga del histórico taparía todo lo demás. Lo metido a
mano va además en su propia lista (`a_mano`), que con el simulador a 200/s
en la de últimos llegados no se llegaría a ver.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg2.extensions import connection as PGConnection

from api.dependencias import dict_cursor, get_conn
from api.instrumentacion import cronometrar

router = APIRouter(tags=["ingesta"])

MAX_INTERVALOS = 300
ULTIMOS_VIAJES = 8
ULTIMOS_RECHAZOS = 6
A_MANO = 10   # caben las 8 muestras del botón del frontend
# Ventana para el ritmo "ahora": más corta que la gráfica, pero bastante
# larga para no depender de en qué segundo cayó cada lote del consumidor
SEGUNDOS_RITMO = 10


def _nativo(v: Any) -> Any:
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


def _filas(cur) -> list[dict]:
    return [{k: _nativo(v) for k, v in dict(f).items()} for f in cur.fetchall()]


@router.get("/ingesta")
def ingesta(
    ventana: int = Query(300, ge=30, le=3600, description="Segundos hacia atrás que cubre la serie."),
    paso: int = Query(5, ge=1, le=300, description="Segundos por intervalo de la serie."),
    conn: PGConnection = Depends(get_conn),
) -> dict:
    """Llegadas del flujo en vivo al caliente y a cuarentena."""
    if ventana // paso > MAX_INTERVALOS:
        raise HTTPException(400, f"Demasiados intervalos: ventana/paso no puede pasar de {MAX_INTERVALOS}.")

    with cronometrar() as crono:
        with dict_cursor(conn) as cur:
            # Todo en una transacción: NOW() es el mismo en las cuatro consultas
            cur.execute("SELECT NOW() AS ahora")
            ahora = cur.fetchone()["ahora"]

            # Intervalos alineados a múltiplos de `paso` desde la época, para
            # que no bailen entre una llamada y la siguiente. El intervalo en
            # curso no entra: a medio llenar parecería una caída del ritmo
            cur.execute(
                """
                WITH lim AS (
                    SELECT to_timestamp(floor(extract(epoch FROM %(ahora)s::timestamptz) / %(paso)s) * %(paso)s) AS fin
                ),
                cubos AS (
                    SELECT generate_series(lim.fin - make_interval(secs => %(ventana)s),
                                           lim.fin - make_interval(secs => %(paso)s),
                                           make_interval(secs => %(paso)s)) AS t
                    FROM lim
                ),
                cal AS (
                    SELECT to_timestamp(floor(extract(epoch FROM ingested_at) / %(paso)s) * %(paso)s) AS t,
                           count(*) AS n,
                           avg(extract(epoch FROM ingested_at - event_time)) AS retraso
                    FROM taxi_trips
                    WHERE ingested_at >= (SELECT min(t) FROM cubos) AND origen <> 'carga_inicial'
                    GROUP BY 1
                ),
                cua AS (
                    SELECT to_timestamp(floor(extract(epoch FROM recibido_en) / %(paso)s) * %(paso)s) AS t,
                           count(*) AS n
                    FROM trips_cuarentena
                    WHERE recibido_en >= (SELECT min(t) FROM cubos) AND origen <> 'carga_inicial'
                    GROUP BY 1
                )
                SELECT cubos.t,
                       COALESCE(cal.n, 0) AS caliente,
                       COALESCE(cua.n, 0) AS cuarentena,
                       round(cal.retraso::numeric, 1) AS retraso_s
                FROM cubos
                LEFT JOIN cal USING (t)
                LEFT JOIN cua USING (t)
                ORDER BY cubos.t
                """,
                {"ahora": ahora, "paso": paso, "ventana": ventana},
            )
            serie = _filas(cur)

            cur.execute(
                """
                SELECT
                    (SELECT count(*) FROM taxi_trips
                      WHERE ingested_at >= %(ahora)s::timestamptz - make_interval(secs => %(s)s)
                        AND origen <> 'carga_inicial') AS caliente,
                    (SELECT count(*) FROM trips_cuarentena
                      WHERE recibido_en >= %(ahora)s::timestamptz - make_interval(secs => %(s)s)
                        AND origen <> 'carga_inicial') AS cuarentena
                """,
                {"ahora": ahora, "s": SEGUNDOS_RITMO},
            )
            recientes = cur.fetchone()

            # Lo último que ha llegado. El límite de un día evita recorrer el
            # índice entero cuando no hay flujo en vivo (solo carga_inicial)
            cur.execute(
                """
                SELECT ingested_at, event_time, origen, fichero_origen,
                       pu_location_id, do_location_id, trip_distance, total_amount, avisos,
                       round(extract(epoch FROM ingested_at - event_time)::numeric, 1) AS retraso_s
                FROM taxi_trips
                WHERE ingested_at >= %(ahora)s::timestamptz - interval '1 day'
                  AND origen <> 'carga_inicial'
                ORDER BY ingested_at DESC
                LIMIT %(n)s
                """,
                {"ahora": ahora, "n": ULTIMOS_VIAJES},
            )
            ultimos = _filas(cur)

            cur.execute(
                """
                SELECT recibido_en, origen, motivos, left(payload::text, 160) AS payload
                FROM trips_cuarentena
                WHERE recibido_en >= %(ahora)s::timestamptz - interval '1 day'
                  AND origen <> 'carga_inicial'
                ORDER BY recibido_en DESC
                LIMIT %(n)s
                """,
                {"ahora": ahora, "n": ULTIMOS_RECHAZOS},
            )
            rechazos = _filas(cur)

            # Lo metido a mano en la última hora, vaya al caliente o a
            # cuarentena. Las condiciones son las de los índices parciales de
            # postgres/init/08_p3.sql, tal cual, para que se usen
            cur.execute(
                """
                (SELECT ingested_at AS llego, 'caliente' AS destino, avisos AS motivos,
                        pu_location_id, do_location_id, trip_distance, total_amount, NULL AS payload
                   FROM taxi_trips
                  WHERE (origen = 'manual' OR fichero_origen = 'anadir_viaje.py')
                    AND ingested_at >= %(ahora)s::timestamptz - interval '1 hour'
                  ORDER BY ingested_at DESC LIMIT %(n)s)
                UNION ALL
                (SELECT recibido_en, 'cuarentena', motivos,
                        NULL, NULL, NULL, NULL, left(payload::text, 160)
                   FROM trips_cuarentena
                  WHERE (origen = 'manual'
                         OR payload->>'fichero_origen' = 'anadir_viaje.py'
                         OR motivos @> ARRAY['mensaje_ilegible'])
                    AND recibido_en >= %(ahora)s::timestamptz - interval '1 hour'
                  ORDER BY recibido_en DESC LIMIT %(n)s)
                ORDER BY llego DESC
                LIMIT %(n)s
                """,
                {"ahora": ahora, "n": A_MANO},
            )
            a_mano = _filas(cur)
        conn.rollback()  # solo lectura: cerrar la transacción sin dejar nada abierto
        latencia_ms = crono.transcurrido_ms

    hace_s = None
    if ultimos:
        # Un lote que confirma justo después de empezar esta transacción
        # trae un ingested_at unos ms posterior a NOW(): no hay "hace" negativo
        hace_s = max(0.0, round((ahora - datetime.fromisoformat(ultimos[0]["ingested_at"])).total_seconds(), 1))

    return {
        "data": {
            "ahora": ahora.isoformat(),
            "ventana_s": ventana,
            "paso_s": paso,
            "serie": serie,
            "totales": {
                "caliente": sum(c["caliente"] for c in serie),
                "cuarentena": sum(c["cuarentena"] for c in serie),
            },
            "ritmo": {
                "segundos": SEGUNDOS_RITMO,
                "caliente_por_s": round(recientes["caliente"] / SEGUNDOS_RITMO, 1),
                "cuarentena_por_s": round(recientes["cuarentena"] / SEGUNDOS_RITMO, 1),
            },
            "ultimo": {
                "hace_s": hace_s,
                "retraso_s": ultimos[0]["retraso_s"] if ultimos else None,
            },
            "ultimos_viajes": ultimos,
            "ultimos_rechazos": rechazos,
            "a_mano": a_mano,
        },
        "meta": {
            "data_source": "hot",
            "coverage": [],
            "as_of": ahora.isoformat(),
            "latency_ms": latencia_ms,
            "rows": len(serie),
        },
    }
