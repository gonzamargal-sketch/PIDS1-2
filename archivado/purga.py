#!/usr/bin/env python3
"""
PIDS Parte 2 — Paso 8 (T1.2): purga del tier frío.

Cierra el ciclo de vida. Aplica la política
('trips', 'cold', NULL, 7, 'years', 'DELETE') en dos pasos, y los dos son
mecanismos NATIVOS de Iceberg: no hemos inventado un sistema de retención
por encima.

    1. DELETE de las particiones mensuales que superan la custodia.
       Se borran meses ENTEROS: el corte se redondea al primer día del mes,
       así que un mes solo cae cuando todo él ha superado el umbral. Como
       la tabla está particionada por mes, Iceberg quita los ficheros sin
       reescribir ninguno (operación DELETE, no OVERWRITE).

    2. expire_snapshots. Tras el DELETE las filas ya no se ven, pero los
       Parquet siguen en MinIO porque los snapshots anteriores los
       referencian (es lo que permite el time travel). Expirar esos
       snapshots es lo que hace que dejen de ocupar.

OJO, PyIceberg 0.12 SOLO HACE LA MITAD DEL PASO 2
    `expire_snapshots()` quita los snapshots de los metadatos pero no borra
    ningún fichero (en Java lo hace; en PyIceberg aún no). Si nos
    quedáramos ahí, el bucket no bajaría ni un byte. Por eso aquí se
    calcula qué ficheros eran alcanzables antes de expirar y cuáles lo
    siguen siendo después, y se borra la diferencia. Es exactamente lo
    que hace la limpieza de ficheros alcanzables de Iceberg en Java:
    solo se borra lo que ningún snapshot vivo necesita.

Uso:
    python archivado/purga.py
    python archivado/purga.py --simulacro          # qué borraría, sin borrar
    python archivado/purga.py --retener-minutos 60 # conserva 1 h de time travel
"""

from __future__ import annotations

import sys
import logging
import warnings
import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyiceberg.expressions import LessThan

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import lakehouse    # noqa: E402
from archivado import estado    # noqa: E402

log = logging.getLogger("purga")

# Cuánto time travel se conserva por defecto. Cero: todo snapshot que no
# sea el actual se expira. Suficiente para este proyecto, en el que nadie
# consulta versiones antiguas del frío; subidlo si algún día hace falta.
RETENER_MINUTOS = 0


@dataclass
class Resumen:
    corte: datetime | None = None
    meses: list[str] | None = None
    filas_borradas: int = 0
    snapshots_expirados: int = 0
    ficheros_borrados: int = 0
    bytes_liberados: int = 0


def _indice_mes(dt: datetime) -> int:
    """Meses desde 1970-01: es como guarda Iceberg la partición month()."""
    return (dt.year - 1970) * 12 + dt.month - 1


def _nombre_mes(indice: int) -> str:
    return f"{1970 + indice // 12}-{indice % 12 + 1:02d}"


def calcular_corte(conn) -> datetime | None:
    """Primer instante que se CONSERVA: inicio del mes de (ahora - umbral).

    Lo calcula Postgres a partir de la política, igual que hace
    particiones_a_archivar() para el caliente.
    """
    pol = estado.politica(conn, "DELETE")
    if pol is None or pol["tier_origen"] != "cold":
        return None
    log.info("Política vigente: borrar del frío a los %s %s",
             pol["umbral_valor"], pol["umbral_unidad"])
    with conn.cursor() as cur:
        cur.execute("SELECT date_trunc('month', NOW() - %s) AS corte",
                    (pol["intervalo"],))
        corte = cur.fetchone()["corte"]
    conn.commit()
    return corte


def meses_a_borrar(tabla, corte: datetime) -> dict[int, int]:
    """{mes: filas} de las particiones enteramente anteriores al corte.

    Sale de los manifiestos (inspect.partitions), sin leer datos.
    """
    limite = _indice_mes(corte)
    meses: dict[int, int] = {}
    if tabla.current_snapshot() is None:
        return meses
    for fila in tabla.inspect.partitions().to_pylist():
        mes = fila["partition"]["event_month"]
        if mes is not None and mes < limite:
            meses[mes] = meses.get(mes, 0) + fila["record_count"]
    return meses


def ficheros_alcanzables(tabla) -> dict[str, int]:
    """{ruta: bytes} de todo lo que necesita algún snapshot vivo.

    Manifest lists, manifiestos y los ficheros de datos VIVOS de cada uno
    (las entradas DELETED de un manifiesto solo son historia: el snapshot
    que las contiene ya no lee esos ficheros).
    """
    io = tabla.io
    rutas: dict[str, int] = {}
    for snap in tabla.metadata.snapshots:
        rutas.setdefault(snap.manifest_list, 0)
        for m in snap.manifests(io):
            rutas[m.manifest_path] = m.manifest_length
            for e in m.fetch_manifest_entry(io, discard_deleted=True):
                rutas[e.data_file.file_path] = e.data_file.file_size_in_bytes
    return rutas


def expirar(tabla, retener_minutos: int, simulacro: bool, r: Resumen) -> None:
    limite = datetime.now(timezone.utc) - timedelta(minutes=retener_minutos)
    actual = tabla.current_snapshot()
    viejos = [s for s in tabla.metadata.snapshots
              if s.timestamp_ms < limite.timestamp() * 1000
              and (actual is None or s.snapshot_id != actual.snapshot_id)]
    if not viejos:
        log.info("expire_snapshots: nada que expirar")
        return

    antes = ficheros_alcanzables(tabla)
    if simulacro:
        log.info("expire_snapshots: expiraría %d snapshot(s)", len(viejos))
        return

    tabla.maintenance.expire_snapshots().older_than(limite).commit()
    tabla.refresh()
    r.snapshots_expirados = len(viejos)

    despues = ficheros_alcanzables(tabla)
    huerfanos = {ruta: b for ruta, b in antes.items() if ruta not in despues}
    for ruta in huerfanos:
        tabla.io.delete(ruta)
    r.ficheros_borrados = len(huerfanos)
    r.bytes_liberados = sum(huerfanos.values())
    log.info("expire_snapshots: %d snapshot(s) expirados, %d fichero(s) borrados "
             "de MinIO (%.1f kB liberados)",
             r.snapshots_expirados, r.ficheros_borrados, r.bytes_liberados / 1024)


def ejecutar(dsn: str | None = None, retener_minutos: int = RETENER_MINUTOS,
             simulacro: bool = False) -> Resumen:
    """Punto de entrada reutilizable (lo llama el DAG `purga_final` de P4)."""
    r = Resumen()
    conn = estado.conectar(dsn)
    try:
        r.corte = calcular_corte(conn)
    finally:
        conn.close()
    if r.corte is None:
        raise SystemExit("No hay política DELETE activa para el tier frío")

    tabla = lakehouse.obtener_tabla()
    meses = meses_a_borrar(tabla, r.corte)
    r.meses = [_nombre_mes(m) for m in sorted(meses)]
    r.filas_borradas = sum(meses.values())
    log.info("Corte: se conserva desde %s", r.corte.date())

    if meses:
        for m in sorted(meses):
            log.info("  %s  %s filas", _nombre_mes(m), f"{meses[m]:,}")
        if simulacro:
            log.info("SIMULACRO: se borrarían %d mes(es), %s filas",
                     len(meses), f"{r.filas_borradas:,}")
        else:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", "Delete operation did not match")
                tabla.delete(LessThan("event_time", r.corte.isoformat()),
                             snapshot_properties={"pids.purga.corte": r.corte.isoformat()})
            tabla.refresh()
            log.info("DELETE: %d mes(es), %s filas fuera de la tabla",
                     len(meses), f"{r.filas_borradas:,}")
    else:
        log.info("Ningún mes supera la custodia")

    # Se expira siempre, haya habido DELETE o no: también recoge los
    # ficheros que dejaron atrás los reintentos del archivado (overwrite)
    expirar(tabla, retener_minutos, simulacro, r)
    return r


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%H:%M:%S")
    p = argparse.ArgumentParser(description="Purga del tier frío (E8)")
    p.add_argument("--dsn", default=None, help="Por defecto, el de common.config")
    p.add_argument("--retener-minutos", type=int, default=RETENER_MINUTOS,
                   help="Snapshots más recientes que esto no se expiran")
    p.add_argument("--simulacro", action="store_true",
                   help="Dice qué borraría, sin borrar nada")
    a = p.parse_args()
    ejecutar(a.dsn, a.retener_minutos, a.simulacro)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
