"""
PIDS Parte 2 — API · Agregados sobre los dos tiers (Parte 3).

/trips devuelve filas sueltas con un tope de 100.000, así que no sirve
para «¿cuánto se facturó en marzo?»: el total saldría cortado. Aquí se
suma DENTRO de cada tier (SQL en PostgreSQL, pyarrow sobre el scan de
Iceberg) y se juntan las sumas, con el mismo plan de tramos que /trips.

SE JUNTAN SUMAS, NO MEDIAS
    Cada tramo devuelve sumas y conteos; las medias se calculan al final
    sobre el total. Promediar medias de tramos con distinto número de
    viajes daría un resultado falso.

LA ZONA ES LA DE RECOGIDA
    `zona` filtra por pu_location_id (dónde empieza el viaje), que es lo
    que se entiende por «los ingresos de una zona».
"""

from __future__ import annotations

from datetime import date
from typing import Any, Literal

import pyarrow as pa
import pyarrow.compute as pc
from psycopg2.extensions import connection as PGConnection
from pyiceberg.table import Table

from api.dependencias import dict_cursor
from api.router_tiers import Plan, planificar

Agrupar = Literal["ninguno", "dia", "zona"]

# Medidas que se suman en cada tier. viajes es el conteo.
SUMAS = ("ingresos", "tarifas", "propinas", "distancia", "pasajeros")
COLUMNA = {
    "ingresos": "total_amount",
    "tarifas": "fare_amount",
    "propinas": "tip_amount",
    "distancia": "trip_distance",
    "pasajeros": "passenger_count",
}


def _vacio() -> dict[str, float]:
    return {"viajes": 0, **{m: 0.0 for m in SUMAS}}


def _acumular(destino: dict[str, float], origen: dict[str, Any]) -> None:
    destino["viajes"] += int(origen.get("viajes") or 0)
    for m in SUMAS:
        destino[m] += float(origen.get(m) or 0)


def _hot(conn: PGConnection, desde, hasta, zona: int | None, agrupar: Agrupar) -> dict[Any, dict]:
    clave = {
        "ninguno": "NULL",
        "dia": "(event_time AT TIME ZONE 'UTC')::date",
        "zona": "pu_location_id",
    }[agrupar]
    sumas = ", ".join(f"sum({COLUMNA[m]}) AS {m}" for m in SUMAS)
    sql = f"""
        SELECT {clave} AS clave, count(*) AS viajes, {sumas}
        FROM taxi_trips
        WHERE event_time >= %s AND event_time < %s
          {"AND pu_location_id = %s" if zona is not None else ""}
        GROUP BY 1
    """
    params = [desde, hasta] + ([zona] if zona is not None else [])
    with dict_cursor(conn) as cur:
        cur.execute(sql, params)
        return {f["clave"]: dict(f) for f in cur.fetchall()}


def _cold(tabla: Table, desde, hasta, zona: int | None, agrupar: Agrupar) -> dict[Any, dict]:
    filtro = f"event_time >= '{desde.isoformat()}' and event_time < '{hasta.isoformat()}'"
    if zona is not None:
        filtro += f" and pu_location_id == {int(zona)}"
    campos = ("event_time", "pu_location_id", *COLUMNA.values())
    datos = tabla.scan(row_filter=filtro, selected_fields=campos).to_arrow()
    if datos.num_rows == 0:
        return {}

    if agrupar == "dia":
        # Día UTC: se quita la zona (los valores ya están en UTC) y se trunca
        dia = pc.cast(datos["event_time"].cast(pa.timestamp("us")), pa.date32())
        datos = datos.append_column("clave", dia)
    elif agrupar == "zona":
        datos = datos.append_column("clave", datos["pu_location_id"])
    else:
        datos = datos.append_column("clave", pa.array([0] * datos.num_rows, pa.int8()))

    agregados = datos.group_by("clave").aggregate(
        [("event_time", "count")] + [(COLUMNA[m], "sum") for m in SUMAS]
    )
    salida: dict[Any, dict] = {}
    for fila in agregados.to_pylist():
        clave = None if agrupar == "ninguno" else fila["clave"]
        salida[clave] = {
            "viajes": fila["event_time_count"],
            **{m: fila[f"{COLUMNA[m]}_sum"] for m in SUMAS},
        }
    return salida


def _medidas(acc: dict[str, float]) -> dict[str, Any]:
    """Totales redondeados y medias calculadas sobre el total."""
    n = acc["viajes"]
    return {
        "viajes": int(n),
        "ingresos": round(acc["ingresos"], 2),
        "tarifas": round(acc["tarifas"], 2),
        "propinas": round(acc["propinas"], 2),
        "distancia_total_mi": round(acc["distancia"], 1),
        "importe_medio": round(acc["ingresos"] / n, 2) if n else None,
        "propina_media": round(acc["propinas"] / n, 2) if n else None,
        "distancia_media_mi": round(acc["distancia"] / n, 2) if n else None,
        "pasajeros_medios": round(acc["pasajeros"] / n, 2) if n else None,
    }


def resumir(
    conn: PGConnection,
    tabla_fria: Table,
    desde,
    hasta,
    zona: int | None = None,
    agrupar: Agrupar = "ninguno",
    top: int = 10,
) -> tuple[dict[str, Any], Plan]:
    plan = planificar(conn, desde, hasta)
    total = _vacio()
    por_tier = {"hot": _vacio(), "cold": _vacio()}
    grupos: dict[Any, dict[str, float]] = {}

    for tramo in plan.tramos:
        leer = _cold if tramo.tier == "cold" else _hot
        fuente = tabla_fria if tramo.tier == "cold" else conn
        for clave, valores in leer(fuente, tramo.desde, tramo.hasta, zona, agrupar).items():
            _acumular(total, valores)
            _acumular(por_tier[tramo.tier], valores)
            if agrupar != "ninguno":
                _acumular(grupos.setdefault(clave, _vacio()), valores)

    datos: dict[str, Any] = {
        "periodo": {"desde": desde.isoformat(), "hasta": hasta.isoformat()},
        "zona_recogida": zona,
        "totales": _medidas(total),
        "por_tier": {t: _medidas(v) for t, v in por_tier.items() if v["viajes"]},
    }
    if agrupar == "dia":
        datos["por_dia"] = [
            {"dia": (c.isoformat() if isinstance(c, date) else str(c)), **_medidas(v)}
            for c, v in sorted(grupos.items())
        ]
    elif agrupar == "zona":
        ordenadas = sorted(grupos.items(), key=lambda kv: kv[1]["ingresos"], reverse=True)
        datos["por_zona"] = [{"zona": c, **_medidas(v)} for c, v in ordenadas[:top]]
        datos["zonas_con_viajes"] = len(grupos)
    return datos, plan
