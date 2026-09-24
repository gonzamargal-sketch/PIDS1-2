"""
PIDS Parte 2 — Simulador del flujo en vivo (P2, T2.1).

Reproduce 2020 a partir de la semilla de mil viajes reales, como si
estuvieran ocurriendo ahora:

    semilla CSV ─▶ muestreo ─▶ jitter ─▶ event_time = ahora ─▶ sumidero
                                            (+ suciedad extra)

LAS DOS COLUMNAS DE TIEMPO (§3.1, lo más fácil de liar del proyecto)
    event_time            se RE-ESTAMPA a "ahora". Es el tiempo de sistema:
                          particiona el caliente y decide cuándo se archiva.
    tpep_pickup_datetime  se QUEDA en 2020 (movido a un día cualquiera del
                          año por el jitter). Es el tiempo de negocio.
    Si se re-estampara la fecha de recogida, las consultas "viajes de
    marzo" dejarían de tener sentido; si no se re-estampara event_time,
    todo estaría caducado desde el primer segundo.

SUMIDEROS
    --sumidero NOMBRE carga simulador/sumidero_NOMBRE.py. Cada módulo
    define una clase `Sumidero` con esta interfaz, que no cambia:

        Sumidero()                          conecta con lo que necesite
        .enviar(mensajes: list[dict])       entrega un lote; lanza si falla
        .cerrar()                           vacía buffers y desconecta

    Los mensajes siguen el formato de common/mensajes.py. Añadir un
    sumidero nuevo es añadir un fichero, sin tocar este.

Uso:
    python -m simulador.simulador --sumidero postgres --eps 200
    python -m simulador.simulador --sumidero postgres --eps 0 --total 1000000
    python -m simulador.simulador --sumidero kafka
"""

from __future__ import annotations

import argparse
import importlib
import logging
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from common import esquema
from common.config import SIMULADOR

from . import jitter

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("simulador")

TICK_S = 1.0          # cada cuánto se emite un lote con --eps > 0
LOG_CADA_S = 10.0


def cargar_sumidero(nombre: str):
    modulo = importlib.import_module(f"simulador.sumidero_{nombre}")
    return modulo.Sumidero()


def _iso(v) -> str | None:
    return None if pd.isna(v) else v.isoformat()


def _num(v):
    if pd.isna(v):
        return None
    return v.item() if hasattr(v, "item") else v


def a_mensajes(df: pd.DataFrame, ahora: datetime, fichero: str) -> list[dict]:
    """Filas tipadas -> mensajes JSON con el formato de common/mensajes.py.

    Los event_time del lote se reparten en el último tick para que no
    salgan todos idénticos, pero nunca por delante de "ahora".
    """
    n = len(df)
    mensajes = []
    registros = df.to_dict("records")
    for i, fila in enumerate(registros):
        et = ahora - timedelta(seconds=TICK_S * (n - 1 - i) / max(n, 1))
        msg = {
            "trip_id": str(uuid.uuid4()),
            "event_time": et.isoformat(),
            "tpep_pickup_datetime": _iso(fila["tpep_pickup_datetime"]),
            "tpep_dropoff_datetime": _iso(fila["tpep_dropoff_datetime"]),
        }
        for c in esquema.COLUMNAS_NEGOCIO:
            if c not in msg:
                msg[c] = _num(fila[c])
        msg["esquema_version"] = esquema.ESQUEMA_VERSION
        msg["fichero_origen"] = fichero
        mensajes.append(msg)
    return mensajes


class Simulador:
    def __init__(self, semilla: pd.DataFrame, fichero: str, seed: int,
                 con_jitter: bool, pct_suciedad: float):
        self.semilla = semilla.reset_index(drop=True)
        self.fichero = fichero
        self.rng = np.random.default_rng(seed)
        self.con_jitter = con_jitter
        self.pct_suciedad = pct_suciedad

    def lote(self, n: int) -> list[dict]:
        idx = self.rng.integers(0, len(self.semilla), n)
        df = self.semilla.iloc[idx].reset_index(drop=True)
        if self.con_jitter:
            df = jitter.perturbar(df, self.rng)
        mensajes = a_mensajes(df, datetime.now(timezone.utc), self.fichero)

        if self.pct_suciedad > 0:
            sucios = self.rng.random(n) < self.pct_suciedad / 100.0
            for i in np.flatnonzero(sucios):
                mensajes[i] = jitter.ensuciar(mensajes[i], self.rng)
        return mensajes


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--sumidero", default="kafka",
                    help="postgres | kafka (carga simulador/sumidero_<nombre>.py)")
    ap.add_argument("--eps", type=float, default=SIMULADOR.eventos_por_segundo,
                    help="eventos por segundo; 0 = tan rápido como se pueda")
    ap.add_argument("--total", type=int, default=0,
                    help="parar tras N eventos; 0 = sin fin")
    ap.add_argument("--lote", type=int, default=1000,
                    help="tamaño máximo de lote que se entrega al sumidero")
    ap.add_argument("--semilla-csv", default=SIMULADOR.semilla_csv)
    ap.add_argument("--seed", type=int, default=SIMULADOR.seed)
    ap.add_argument("--pct-suciedad", type=float, default=SIMULADOR.pct_suciedad_extra)
    ap.add_argument("--sin-jitter", action="store_true",
                    help="SOLO para depurar: las métricas de compresión dejan de valer")
    args = ap.parse_args(argv)

    con_jitter = SIMULADOR.jitter and not args.sin_jitter
    if not con_jitter:
        log.warning("JITTER DESACTIVADO: filas repetidas, el ratio de compresión "
                    "del frío saldrá inflado. No midáis nada así.")

    semilla = esquema.leer_csv(args.semilla_csv)
    fichero = Path(args.semilla_csv).name
    sim = Simulador(semilla, fichero, args.seed, con_jitter, args.pct_suciedad)
    sumidero = cargar_sumidero(args.sumidero)

    parar = False

    def _parar(signum, _frame):
        nonlocal parar
        log.info("Señal %s: terminando el lote en curso", signum)
        parar = True

    signal.signal(signal.SIGTERM, _parar)
    signal.signal(signal.SIGINT, _parar)

    log.info("Emitiendo a '%s' · %s ev/s · total=%s · jitter=%s · suciedad extra=%.1f%%",
             args.sumidero, args.eps or "máx", args.total or "∞", con_jitter, args.pct_suciedad)

    emitidos = 0
    inicio = ultimo_log = time.monotonic()
    siguiente = inicio
    try:
        while not parar and (args.total == 0 or emitidos < args.total):
            if args.eps > 0:
                n = max(1, round(args.eps * TICK_S))
            else:
                n = args.lote
            if args.total:
                n = min(n, args.total - emitidos)

            for i in range(0, n, args.lote):
                sumidero.enviar(sim.lote(min(args.lote, n - i)))
            emitidos += n

            ahora = time.monotonic()
            if ahora - ultimo_log >= LOG_CADA_S:
                log.info("%d emitidos (%.0f ev/s de media)", emitidos,
                         emitidos / (ahora - inicio))
                ultimo_log = ahora

            if args.eps > 0:
                siguiente += TICK_S
                espera = siguiente - time.monotonic()
                if espera > 0:
                    time.sleep(espera)
                else:
                    siguiente = time.monotonic()   # vamos tarde: no acumular deuda
    finally:
        sumidero.cerrar()
        dur = time.monotonic() - inicio
        log.info("FIN: %d eventos emitidos en %.1f s (%.0f ev/s)",
                 emitidos, dur, emitidos / dur if dur else 0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
