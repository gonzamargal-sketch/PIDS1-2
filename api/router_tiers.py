"""
PIDS Parte 2 — API · El router de tiers (P3, T3.2).

El componente con más chicha del proyecto. Recibe un rango de fechas, lo
corta por la frontera de retención, decide qué tiers tocar, consulta,
fusiona y anota de dónde viene cada dato.

LA FRONTERA SALE DE retention_policy, NO DE UNA CONSTANTE
    frontera = NOW() - umbral_intervalo('trips', 'ARCHIVE')

    Se lee en cada request a propósito. En el vídeo (§10, minuto 2:30) se
    baja el umbral a 5 minutos con un PUT y el router tiene que empezar a
    mandar a Iceberg acto seguido, sin reiniciar nada. Si la frontera
    estuviera cacheada o en un .py, ese momento no funcionaría.

    Se reutiliza la función SQL umbral_intervalo() en vez de reconstruir
    el INTERVAL en Python: la conversión de unidades ya está resuelta ahí
    (postgres/init/01_esquema.sql) y así no hay dos verdades.

SE CORTA POR event_time, NUNCA POR tpep_pickup_datetime
    §3.1 de ARQUITECTURA.md: event_time es el tiempo de sistema y es la
    única columna que gobierna el ciclo de vida. tpep_pickup_datetime es
    el dato de negocio (2020) y no dice nada sobre en qué tier está la
    fila. Cortar por la columna equivocada es el error más fácil de
    cometer en todo el proyecto.

MODELO DE MUDANZA => TRAMOS DISJUNTOS
    Los tiers no se solapan (§1: "mudanza (disjunto)"), así que fusionar
    es concatenar y ordenar, sin deduplicar. Si algún día se pasara a un
    modelo con solape habría que deduplicar por (trip_id, event_time).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from fastapi import HTTPException
from psycopg2.extensions import connection as PGConnection
from pyiceberg.table import Table

from api.dependencias import DATASET, dict_cursor
from api.lectura_cold import leer_cold
from api.lectura_hot import leer_hot


@dataclass
class Tramo:
    """Un trozo del rango pedido que se resuelve contra un tier concreto."""
    desde: datetime
    hasta: datetime
    tier: str  # 'hot' | 'cold'

    def como_coverage(self) -> dict[str, Any]:
        return {
            "desde": self.desde.isoformat(),
            "hasta": self.hasta.isoformat(),
            "tier": self.tier,
        }


@dataclass
class Plan:
    """Cómo se va a resolver una consulta: qué tramos y contra qué tier."""
    frontera: datetime
    tramos: list[Tramo] = field(default_factory=list)

    @property
    def data_source(self) -> str:
        tiers = {t.tier for t in self.tramos}
        if tiers == {"hot", "cold"}:
            return "mixto"
        if tiers == {"cold"}:
            return "cold"
        return "hot"


def frontera_retencion(conn: PGConnection) -> datetime:
    """Instante a partir del cual una fila sigue estando en el caliente.

        event_time >= frontera  -> PostgreSQL (hot)
        event_time <  frontera  -> Iceberg    (cold)

    Devuelve un datetime con tzinfo, porque lo calcula PostgreSQL con
    NOW() y TIMESTAMPTZ.
    """
    with dict_cursor(conn) as cur:
        cur.execute(
            "SELECT NOW() - umbral_intervalo(%s, 'ARCHIVE') AS frontera",
            (DATASET,),
        )
        fila = cur.fetchone()

    if fila is None or fila["frontera"] is None:
        # umbral_intervalo devuelve NULL si no hay política ARCHIVE activa.
        raise HTTPException(
            status_code=503,
            detail=(
                f"No hay política ARCHIVE activa para dataset='{DATASET}' en "
                "retention_policy: el router no puede decidir la frontera."
            ),
        )
    return fila["frontera"]


def planificar(conn: PGConnection, desde: datetime, hasta: datetime) -> Plan:
    """Corta [desde, hasta) por la frontera y decide qué tiers tocar.

    Orden de decisión de §6: PostgreSQL si el rango cae en caliente →
    Iceberg si cae en frío → los dos y fusión si cruza la frontera.
    """
    frontera = frontera_retencion(conn)
    plan = Plan(frontera=frontera)

    # Parte fría: [desde, min(hasta, frontera))
    cold_desde, cold_hasta = desde, min(hasta, frontera)
    if cold_desde < cold_hasta:
        plan.tramos.append(Tramo(cold_desde, cold_hasta, "cold"))

    # Parte caliente: [max(desde, frontera), hasta)
    hot_desde, hot_hasta = max(desde, frontera), hasta
    if hot_desde < hot_hasta:
        plan.tramos.append(Tramo(hot_desde, hot_hasta, "hot"))

    return plan


def consultar(
    conn: PGConnection,
    tabla_fria: Table,
    desde: datetime,
    hasta: datetime,
    limite: int,
) -> tuple[list[dict[str, Any]], Plan]:
    """Ejecuta el plan y fusiona los resultados de los tiers implicados.

    El `limite` es global a la consulta, no por tier: se reparte gastando
    primero el tramo más antiguo (el frío) para que el resultado sea
    contiguo en el tiempo y no queden huecos raros en medio.
    """
    plan = planificar(conn, desde, hasta)
    filas: list[dict[str, Any]] = []

    for tramo in plan.tramos:
        restante = limite - len(filas)
        if restante <= 0:
            break
        if tramo.tier == "cold":
            filas.extend(leer_cold(tabla_fria, tramo.desde, tramo.hasta, restante))
        else:
            filas.extend(leer_hot(conn, tramo.desde, tramo.hasta, restante))

    # Tramos disjuntos: fusionar es concatenar y ordenar por event_time.
    filas.sort(key=lambda f: f["event_time"])
    return filas, plan
