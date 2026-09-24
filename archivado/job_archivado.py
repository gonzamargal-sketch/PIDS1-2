#!/usr/bin/env python3
"""
PIDS Parte 2 — Paso 8 (T1.1): job de archivado caliente -> frío.

Es la pieza que hace visible E8. Recorre la máquina de estados de §3.2
por cada partición diaria que supera la política vigente:

    1. particiones_a_archivar()           candidatas según retention_policy
    2. ESCRIBIENDO                        +1 intento, filas_origen contadas
    3. Postgres -> Iceberg                df_a_arrow + transacción de Iceberg
    4. ESCRITO                            snapshot_id, filas y bytes escritos
    5. VERIFICA                           conteo Iceberg == conteo Postgres,
                                          si no cuadra -> ERROR y no se borra
    6. desalojar_particion()              DROP de la partición, DESALOJADO

REGLA DE ORO: nunca se borra sin haber verificado. La hace cumplir la
propia base de datos (desalojar_particion rechaza cualquier estado que no
sea VERIFICADO), no solo este script.

POR QUÉ ES SEGURO RELANZARLO EN CUALQUIER MOMENTO
    · La escritura en Iceberg es un OVERWRITE del día, no un append: en
      una sola transacción de Iceberg se borran las filas de ese día que
      hubiera y se escriben las nuevas. Si el job murió después de
      escribir pero antes de marcar ESCRITO, la siguiente ejecución vuelve
      a escribir el día entero y no duplica nada.
    · Cada estado se confirma en Postgres antes de dar el paso siguiente,
      así que al relanzar se retoma desde donde se quedó.
    · Mientras se procesa una partición se tiene un LOCK SHARE ROW
      EXCLUSIVE sobre ella: se puede leer, pero nadie puede insertar (el
      conteo no se mueve entre la verificación y el DROP) y una segunda
      ejecución concurrente no puede cogerla a la vez.
    · Lo ya DESALOJADO no vuelve a salir como candidato: lanzarlo dos
      veces seguidas, la segunda no hace nada.

Uso:
    python archivado/job_archivado.py
    python -m archivado.job_archivado --dia 2026-08-15
    python -m archivado.job_archivado --simulacro      # solo enseña candidatas
"""

from __future__ import annotations

import os
import sys
import time
import logging
import warnings
import argparse
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path

import pandas as pd
import psycopg2
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import esquema, lakehouse   # noqa: E402
from archivado import estado            # noqa: E402

log = logging.getLogger("archivado")

# Filas que se leen de Postgres y se convierten a Arrow de cada vez. Acota
# la memoria del job (Airflow tiene 1 GB) sin fragmentar en ficheros
# pequeños: cada lote acaba en un Parquet de unos MB.
LOTE_FILAS = 250_000
# Una partición que falla este número de veces se deja en ERROR hasta que
# alguien la mire. Evita que el DAG machaque cada hora algo que no cuadra.
MAX_INTENTOS = 3
# Cuánto se espera al lock de la partición antes de saltarla. Si hay un
# INSERT en curso lo normal es que termine en milisegundos; si no llega,
# otra ejecución del job la tiene cogida.
ESPERA_LOCK = "10s"

# Columnas que se llevan a Iceberg. ingested_at no: es metadato del
# caliente y no existe en el esquema del frío.
COLUMNAS = (["trip_id::text AS trip_id", "event_time"]
            + esquema.COLUMNAS_NEGOCIO
            + ["origen", "fichero_origen", "esquema_version", "avisos"])

# Solo para las pruebas (T1.3): mata el proceso en seco en el punto más
# delicado, para comprobar que al relanzar se retoma sin duplicar.
PUNTOS_CAIDA = ("tras-escribir", "tras-escrito")


@dataclass
class Resumen:
    desalojadas: list[date] = field(default_factory=list)
    errores: list[date] = field(default_factory=list)
    saltadas: list[date] = field(default_factory=list)
    filas: int = 0

    @property
    def ok(self) -> bool:
        return not self.errores


def nombre_particion(dia: date) -> str:
    return f"taxi_trips_{dia:%Y_%m_%d}"


def rango_dia(dia: date) -> tuple[datetime, datetime]:
    desde = datetime.combine(dia, dtime.min, tzinfo=timezone.utc)
    return desde, desde + timedelta(days=1)


def filtro_dia(dia: date):
    desde, hasta = rango_dia(dia)
    return And(GreaterThanOrEqual("event_time", desde.isoformat()),
               LessThan("event_time", hasta.isoformat()))


def _caer(punto: str | None, aqui: str) -> None:
    if punto == aqui:
        log.warning("SIMULANDO CAÍDA %s (solo pruebas)", aqui)
        logging.shutdown()
        os._exit(137)


# ─────────────────────────────────────────────────────────────
# Pasos
# ─────────────────────────────────────────────────────────────
def bloquear(conn_lock, dia: date) -> bool:
    """Abre la transacción larga de la partición y la bloquea.

    Devuelve False si no se consigue el lock a tiempo.
    """
    with conn_lock.cursor() as cur:
        # La escritura en Iceberg puede tardar más que el
        # idle_in_transaction_session_timeout del servidor (60 s)
        cur.execute("SET idle_in_transaction_session_timeout = 0")
        cur.execute("SET lock_timeout = %s", (ESPERA_LOCK,))
        try:
            cur.execute(f'LOCK TABLE "{nombre_particion(dia)}" '
                        f"IN SHARE ROW EXCLUSIVE MODE")
        except psycopg2.errors.LockNotAvailable:
            conn_lock.rollback()
            return False
    return True


def contar_postgres(conn_lock, dia: date) -> int:
    """Conteo exacto de la partición y comprobación de su frontera.

    Si las filas de la partición no caen todas en [00:00, 24:00) UTC del
    día, la partición se creó con otra zona horaria: el filtro de Iceberg
    no las encontraría todas y la verificación fallaría sin explicación.
    Mejor pararlo aquí con un mensaje claro.
    """
    desde, hasta = rango_dia(dia)
    with conn_lock.cursor() as cur:
        cur.execute("SELECT contar_particion(%s) AS n", (dia,))
        n = cur.fetchone()["n"]
        cur.execute(f"""SELECT count(*) AS fuera FROM "{nombre_particion(dia)}"
                         WHERE event_time < %s OR event_time >= %s""", (desde, hasta))
        fuera = cur.fetchone()["fuera"]
    if fuera:
        raise RuntimeError(f"{fuera} filas de {nombre_particion(dia)} caen fuera de "
                           f"[{desde}, {hasta}): la partición no está alineada a UTC")
    return n


def escribir(conn_lock, tabla, dia: date, lote: int) -> int:
    """Copia la partición a Iceberg como un overwrite del día.

    Todo va en UNA transacción de Iceberg: el borrado de lo que hubiera de
    ese día y todos los lotes. O se ve todo o no se ve nada.
    """
    escritas = 0
    # Cursor con nombre = cursor en el servidor: trae las filas por lotes
    # en vez de cargar la partición entera en memoria.
    with conn_lock.cursor(name=f"archivar_{dia:%Y%m%d}") as cur:
        cur.itersize = lote
        cur.execute(f'SELECT {", ".join(COLUMNAS)} FROM "{nombre_particion(dia)}"')

        with tabla.transaction() as tx:
            # La primera vez no hay nada que borrar y PyIceberg avisa: es
            # lo esperado, el borrado solo actúa cuando se está reintentando
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", "Delete operation did not match")
                tx.delete(filtro_dia(dia))
            while True:
                filas = cur.fetchmany(lote)
                if not filas:
                    break
                df = pd.DataFrame.from_records(filas)
                tx.append(lakehouse.df_a_arrow(df),
                          snapshot_properties={"pids.archivado.particion": str(dia)})
                escritas += len(df)
                log.info("    lote de %s filas (acumulado %s)",
                         f"{len(df):,}", f"{escritas:,}")
    return escritas


def bytes_en_iceberg(tabla, dia: date) -> int:
    # Los ficheros del archivado solo contienen filas de un día, así que la
    # poda por min/max deja exactamente los de esta partición.
    return sum(t.file.file_size_in_bytes
               for t in tabla.scan(row_filter=filtro_dia(dia)).plan_files())


def archivar_particion(conn, conn_lock, tabla, dia: date, lote: int,
                       max_intentos: int, caida: str | None, resumen: Resumen) -> None:
    job = estado.obtener(conn, dia)
    actual = job["estado"] if job else estado.PENDIENTE

    if actual == estado.DESALOJADO:
        return
    if actual == estado.ERROR and job["intentos"] >= max_intentos:
        log.warning("  %s en ERROR tras %d intentos; se salta (%s)",
                    dia, job["intentos"], job["error"])
        resumen.saltadas.append(dia)
        return

    if not bloquear(conn_lock, dia):
        log.warning("  %s bloqueada por otra sesión; se deja para la próxima", dia)
        resumen.saltadas.append(dia)
        return

    desde, hasta = rango_dia(dia)
    try:
        filas_pg = contar_postgres(conn_lock, dia)

        # ── 2-4. Escritura ────────────────────────────────────
        if actual in (estado.PENDIENTE, estado.ESCRIBIENDO, estado.ERROR):
            if actual != estado.PENDIENTE:
                log.info("  %s estaba en %s: se reescribe el día entero", dia, actual)
            estado.marcar_escribiendo(conn, dia, filas_pg)
            t0 = time.monotonic()
            escritas = escribir(conn_lock, tabla, dia, lote)
            _caer(caida, "tras-escribir")
            tabla.refresh()
            snap = tabla.current_snapshot()
            estado.marcar_escrito(conn, dia, escritas,
                                  snap.snapshot_id if snap else None,
                                  bytes_en_iceberg(tabla, dia))
            log.info("  %s ESCRITO: %s filas en %.1f s",
                     dia, f"{escritas:,}", time.monotonic() - t0)
            _caer(caida, "tras-escrito")
            actual = estado.ESCRITO

        # ── 5. Verificación ───────────────────────────────────
        if actual == estado.ESCRITO:
            tabla.refresh()
            filas_ice = lakehouse.contar_particion(tabla, desde, hasta)
            if filas_ice != filas_pg:
                msg = (f"Verificación fallida: Iceberg tiene {filas_ice} filas "
                       f"y Postgres {filas_pg}. No se borra nada.")
                log.error("  %s %s", dia, msg)
                estado.marcar_error(conn, dia, msg)
                conn_lock.rollback()
                resumen.errores.append(dia)
                return
            estado.marcar_verificado(conn, dia)
            log.info("  %s VERIFICADO: %s = %s", dia, f"{filas_ice:,}", f"{filas_pg:,}")
            actual = estado.VERIFICADO

        # ── 6. Desalojo ───────────────────────────────────────
        # Se hace en la conexión que tiene el lock: el DROP y el paso a
        # DESALOJADO se confirman juntos y liberan el lock en el mismo COMMIT.
        if actual == estado.VERIFICADO:
            log.info("  %s", estado.desalojar(conn_lock, dia))
            resumen.desalojadas.append(dia)
            resumen.filas += filas_pg

    except Exception as e:   # noqa: BLE001 — una partición no tumba el resto
        conn_lock.rollback()
        log.exception("  %s falló", dia)
        try:
            estado.marcar_error(conn, dia, f"{type(e).__name__}: {e}")
        except estado.TransicionInvalida:
            pass   # VERIFICADO: basta con reintentar el desalojo
        resumen.errores.append(dia)
    finally:
        if conn_lock.status != psycopg2.extensions.STATUS_READY:
            conn_lock.rollback()


# ─────────────────────────────────────────────────────────────
def ejecutar(dsn: str | None = None, dia: date | None = None,
             lote: int = LOTE_FILAS, max_intentos: int = MAX_INTENTOS,
             simulacro: bool = False, caida: str | None = None) -> Resumen:
    """Punto de entrada reutilizable (lo llama el DAG `archivar` de P4)."""
    conn = estado.conectar(dsn)        # transiciones de estado, commit a commit
    conn_lock = estado.conectar(dsn)   # transacción larga con el lock
    resumen = Resumen()
    try:
        pol = estado.politica(conn, "ARCHIVE")
        if pol is None:
            raise SystemExit("No hay política ARCHIVE activa en retention_policy")
        log.info("Política vigente: archivar a los %s %s (%s)",
                 pol["umbral_valor"], pol["umbral_unidad"], pol["intervalo"])

        with conn.cursor() as cur:
            cur.execute("SELECT * FROM particiones_a_archivar()")
            candidatas = cur.fetchall()
        conn.commit()
        if dia:
            candidatas = [c for c in candidatas if c["dia"] == dia]

        log.info("%d partición(es) candidata(s)", len(candidatas))
        for c in candidatas:
            log.info("  %s  edad=%sd  ~%s filas  %s",
                     c["dia"], c["edad_dias"], f"{c['filas_estimadas']:,}", c["estado"])
        if simulacro or not candidatas:
            return resumen

        tabla = lakehouse.obtener_tabla()
        for c in candidatas:
            log.info("─" * 60)
            log.info("%s", c["particion"])
            archivar_particion(conn, conn_lock, tabla, c["dia"], lote,
                               max_intentos, caida, resumen)
    finally:
        conn.close()
        conn_lock.close()

    log.info("═" * 60)
    log.info("ARCHIVADO TERMINADO: %d desalojada(s), %s filas movidas, "
             "%d error(es), %d saltada(s)",
             len(resumen.desalojadas), f"{resumen.filas:,}",
             len(resumen.errores), len(resumen.saltadas))
    return resumen


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%H:%M:%S")
    p = argparse.ArgumentParser(description="Archivado caliente -> frío (E8)")
    p.add_argument("--dsn", default=None, help="Por defecto, el de common.config")
    p.add_argument("--dia", type=date.fromisoformat, default=None,
                   help="Archivar solo esta partición (YYYY-MM-DD)")
    p.add_argument("--lote", type=int, default=LOTE_FILAS)
    p.add_argument("--max-intentos", type=int, default=MAX_INTENTOS)
    p.add_argument("--simulacro", action="store_true",
                   help="Solo lista las candidatas, no mueve nada")
    p.add_argument("--simular-caida", choices=PUNTOS_CAIDA, default=None,
                   help=argparse.SUPPRESS)
    a = p.parse_args()
    r = ejecutar(a.dsn, a.dia, a.lote, a.max_intentos, a.simulacro, a.simular_caida)
    # Código distinto de cero para que Airflow marque la tarea y la reintente
    return 0 if r.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
