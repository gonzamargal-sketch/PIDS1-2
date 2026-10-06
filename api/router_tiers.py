"""
PIDS Parte 2 — API · El router de tiers (P3, T3.2).

El componente con más chicha del proyecto. Recibe un rango de fechas, lo
corta por la frontera de retención, decide qué tiers tocar, consulta,
fusiona y anota de dónde viene cada dato.

LA FRONTERA SALE DE DÓNDE ESTÁN LOS DATOS, NO DEL UMBRAL
    retention_policy decide CUÁNDO se mueve un día (lo hace el DAG
    archivar), pero no dónde está cada fila en este momento. Cortar en
    NOW() - umbral fallaba de dos formas:

      - el archivado mueve DÍAS COMPLETOS (particiones_a_archivar() corta
        en (NOW() - umbral)::DATE), así que las filas del día de la
        frontera anteriores a esa hora seguían en PostgreSQL y el router
        las pedía a Iceberg: no salían en ninguna consulta;
      - en la demo, tras bajar el umbral a 5 minutos con el PUT, el
        router mandaba al frío días que el DAG aún no había movido (y el
        de hoy no se mueve hasta mañana), y /trips salía vacío.

    Por eso se decide día a día, en cada request, leyendo el propio
    PostgreSQL (ver dias_en_caliente()):

        día en caliente  <=>  su partición existe y no está DESALOJADO
                              en archival_jobs
        día en frío      <=>  es anterior al día más antiguo del caliente,
                              o archival_jobs lo marca DESALOJADO

    Mientras una partición está ESCRIBIENDO, ESCRITO o VERIFICADO sus
    filas siguen en PostgreSQL, así que se leen de ahí: es la copia que
    seguro está completa. Y si el job deja un día en ERROR y sigue con los
    siguientes, ese día se sigue leyendo del caliente aunque los
    posteriores ya estén en el frío: el plan puede tener más de un tramo
    de cada tier.

    Sigue sin cachearse nada: tras el PUT de la demo, en cuanto el DAG
    desaloja una partición, la siguiente consulta ya la pide a Iceberg.

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
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from psycopg2.extensions import connection as PGConnection
from pyiceberg.table import Table

from api.dependencias import dict_cursor
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
    frontera: datetime  # 00:00 UTC del día más antiguo del caliente
    tramos: list[Tramo] = field(default_factory=list)

    @property
    def data_source(self) -> str:
        tiers = {t.tier for t in self.tramos}
        if tiers == {"hot", "cold"}:
            return "mixto"
        if tiers == {"cold"}:
            return "cold"
        return "hot"


def _inicio_dia(dia: date) -> datetime:
    """00:00 UTC del día: es donde empiezan las particiones diarias.

    crear_particion_dia() usa p_dia::TIMESTAMPTZ y la base está en UTC.
    """
    return datetime.combine(dia, time.min, tzinfo=timezone.utc)


@dataclass
class EstadoTiers:
    """Qué días están en el caliente en este momento."""
    dias_hot: set[date]
    dias_desalojados: set[date]
    primer_dia_hot: date

    def tier(self, dia: date) -> str:
        if dia < self.primer_dia_hot or dia in self.dias_desalojados:
            return "cold"
        # Días sin partición posteriores al primero (p. ej. futuros): si
        # hubiera filas, estarían en taxi_trips_default, que es caliente.
        return "hot"


def dias_en_caliente(conn: PGConnection) -> EstadoTiers:
    """Lee de PostgreSQL qué días siguen en el caliente.

    Las particiones se sacan de v_particiones (la misma vista que usa
    particiones_a_archivar()) y el estado de archival_jobs, así que el
    router y el job de archivado ven la misma verdad.
    """
    with dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT p.dia, COALESCE(a.estado, 'PENDIENTE') AS estado
            FROM v_particiones p
            LEFT JOIN archival_jobs a ON a.particion = p.dia
            WHERE p.dia IS NOT NULL
            """
        )
        particiones = cur.fetchall()
        cur.execute(
            "SELECT particion FROM archival_jobs WHERE estado = 'DESALOJADO'"
        )
        desalojados = {f["particion"] for f in cur.fetchall()}
        cur.execute("SELECT CURRENT_DATE AS hoy")
        hoy = cur.fetchone()["hoy"]

    dias_hot = {f["dia"] for f in particiones if f["dia"] not in desalojados}
    # Sin particiones en caliente, todo lo anterior a hoy está en el frío.
    return EstadoTiers(
        dias_hot=dias_hot,
        dias_desalojados=desalojados,
        primer_dia_hot=min(dias_hot) if dias_hot else hoy,
    )


def planificar(conn: PGConnection, desde: datetime, hasta: datetime) -> Plan:
    """Corta [desde, hasta) en tramos según el tier de cada día.

    Orden de decisión de §6: PostgreSQL si el rango cae en caliente →
    Iceberg si cae en frío → los dos y fusión si cruza la frontera. Los
    días consecutivos del mismo tier se juntan en un solo tramo.
    """
    estado = dias_en_caliente(conn)
    plan = Plan(frontera=_inicio_dia(estado.primer_dia_hot))

    desde_utc = desde.astimezone(timezone.utc)
    ultimo = (hasta.astimezone(timezone.utc) - timedelta(microseconds=1)).date()
    dia = desde_utc.date()
    while dia <= ultimo:
        tier = estado.tier(dia)
        inicio = max(desde, _inicio_dia(dia))
        fin = min(hasta, _inicio_dia(dia + timedelta(days=1)))
        if plan.tramos and plan.tramos[-1].tier == tier:
            plan.tramos[-1].hasta = fin
        else:
            plan.tramos.append(Tramo(inicio, fin, tier))
        dia += timedelta(days=1)

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
