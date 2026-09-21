#!/usr/bin/env python3
"""
PIDS Parte 2 — Paso 0: descarga del dataset a la zona bronze.

Descarga 2020 Yellow Taxi Trip Data (24.648.499 filas) desde el portal
NYC Open Data paginando la API SODA, y lo deja en ficheros CSV troceados.

Por qué paginar y no usar la descarga directa:
    La descarga directa (rows.csv?accessType=DOWNLOAD) genera el fichero al
    vuelo en el servidor y con 24M de filas se corta con cierta frecuencia.
    Probadla primero; este script es el plan B fiable.

Por qué $order=:id (IMPORTANTE):
    Socrata NO garantiza un orden estable entre peticiones si no se le pide
    explícitamente. Sin $order, paginar con $offset devuelve filas repetidas
    y se salta otras, sin dar ningún error. Con $order=:id el orden es
    determinista y la paginación es correcta.

App token (recomendado):
    Sin token, Socrata limita la tasa de peticiones y acabaréis con 429.
    Es gratis: perfil -> Developer Settings -> crear App Token.
    Luego:  export SOCRATA_APP_TOKEN=xxxxxxxx

Uso:
    python descargar_bronze.py
    python descargar_bronze.py --salida ./bronze --filas-por-fichero 1000000
    python descargar_bronze.py --max-filas 500000        # prueba rápida

Si se corta, se relanza el mismo comando y continúa donde iba.
"""

import os
import sys
import csv
import time
import json
import argparse
import logging
from pathlib import Path

import requests

# ─────────────────────────────────────────────────────────────
DATASET_ID   = "kxp8-n2sj"
BASE_URL     = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.csv"
TOTAL_FILAS  = 24_648_499          # según los metadatos del portal
PAGINA       = 50_000              # máximo práctico por petición en SODA
MAX_REINTENTOS = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bronze")


# ─────────────────────────────────────────────────────────────
def pedir_pagina(session: requests.Session, offset: int, limite: int, token: str | None) -> str:
    """Pide una página a la API SODA y devuelve el CSV como texto."""
    params = {
        "$limit":  limite,
        "$offset": offset,
        "$order":  ":id",          # <-- imprescindible para paginar bien
    }
    headers = {"X-App-Token": token} if token else {}

    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            r = session.get(BASE_URL, params=params, headers=headers, timeout=180)
            if r.status_code == 429:
                espera = min(60, 2 ** intento * 5)
                log.warning(f"429 (límite de tasa). Esperando {espera}s… "
                            f"{'Configura SOCRATA_APP_TOKEN para evitarlo.' if not token else ''}")
                time.sleep(espera)
                continue
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            if intento == MAX_REINTENTOS:
                raise
            espera = 2 ** intento
            log.warning(f"Fallo en offset {offset} ({e}). Reintento {intento}/{MAX_REINTENTOS} en {espera}s")
            time.sleep(espera)

    raise RuntimeError(f"No se pudo descargar el offset {offset}")


def leer_checkpoint(ruta: Path) -> dict:
    if ruta.exists():
        return json.loads(ruta.read_text())
    return {"offset": 0, "fichero_idx": 0, "filas_totales": 0}


def guardar_checkpoint(ruta: Path, estado: dict) -> None:
    ruta.write_text(json.dumps(estado, indent=2))


# ─────────────────────────────────────────────────────────────
def descargar(salida: Path, filas_por_fichero: int, max_filas: int, token: str | None) -> None:
    salida.mkdir(parents=True, exist_ok=True)
    ckpt_path = salida / "_checkpoint.json"
    estado = leer_checkpoint(ckpt_path)

    objetivo = min(max_filas, TOTAL_FILAS) if max_filas > 0 else TOTAL_FILAS

    if estado["offset"] > 0:
        log.info(f"Reanudando desde la fila {estado['offset']:,} "
                 f"(fichero {estado['fichero_idx']})")
    else:
        log.info(f"Descarga nueva. Objetivo: {objetivo:,} filas")

    if not token:
        log.warning("Sin SOCRATA_APP_TOKEN: la descarga irá más lenta por el "
                    "límite de tasa. Es gratis y se saca del perfil del portal.")

    session = requests.Session()
    cabecera = None
    t0 = time.time()

    fichero_actual = None
    escritor = None
    filas_en_fichero = 0

    try:
        while estado["offset"] < objetivo:
            limite = min(PAGINA, objetivo - estado["offset"])
            texto = pedir_pagina(session, estado["offset"], limite, token)

            lineas = texto.splitlines()
            if not lineas:
                log.info("Respuesta vacía: se acabaron los datos.")
                break

            if cabecera is None:
                cabecera = lineas[0]
            filas = lineas[1:]

            if not filas:
                log.info("Página sin filas: fin del dataset.")
                break

            # Abrir fichero nuevo si toca
            if fichero_actual is None or filas_en_fichero >= filas_por_fichero:
                if fichero_actual:
                    fichero_actual.close()
                    estado["fichero_idx"] += 1
                nombre = salida / f"yellow_2020_part_{estado['fichero_idx']:04d}.csv"
                fichero_actual = open(nombre, "w", encoding="utf-8", newline="")
                fichero_actual.write(cabecera + "\n")
                filas_en_fichero = 0
                log.info(f"Escribiendo en {nombre.name}")

            fichero_actual.write("\n".join(filas) + "\n")
            filas_en_fichero  += len(filas)
            estado["offset"]  += len(filas)
            estado["filas_totales"] += len(filas)

            guardar_checkpoint(ckpt_path, estado)

            # Progreso
            pct  = 100 * estado["offset"] / objetivo
            tasa = estado["offset"] / max(time.time() - t0, 1)
            queda = (objetivo - estado["offset"]) / max(tasa, 1)
            log.info(f"{estado['offset']:>10,} / {objetivo:,}  ({pct:5.1f}%)  "
                     f"~{tasa:,.0f} filas/s  ETA {queda/60:.0f} min")

            if len(filas) < limite:
                log.info("Última página recibida.")
                break

    finally:
        if fichero_actual:
            fichero_actual.close()
        session.close()

    dur = time.time() - t0
    log.info("=" * 60)
    log.info(f"Descarga terminada: {estado['filas_totales']:,} filas "
             f"en {estado['fichero_idx'] + 1} ficheros ({dur/60:.1f} min)")
    log.info(f"Carpeta: {salida.resolve()}")
    log.info("=" * 60)


# ─────────────────────────────────────────────────────────────
def verificar(salida: Path) -> None:
    """Cuenta las filas de todos los ficheros descargados y avisa de desajustes."""
    ficheros = sorted(salida.glob("yellow_2020_part_*.csv"))
    if not ficheros:
        log.error("No hay ficheros que verificar.")
        return

    total = 0
    for f in ficheros:
        with open(f, encoding="utf-8") as fh:
            n = sum(1 for _ in fh) - 1          # menos la cabecera
        total += n
        log.info(f"  {f.name:<32} {n:>10,} filas  ({f.stat().st_size/1e6:7.1f} MB)")

    tam = sum(f.stat().st_size for f in ficheros) / 1e9
    log.info("-" * 60)
    log.info(f"TOTAL: {total:,} filas en {len(ficheros)} ficheros, {tam:.2f} GB")
    log.info(f"Esperado: {TOTAL_FILAS:,} filas")
    if total == TOTAL_FILAS:
        log.info("Coincide. Descarga completa.")
    else:
        log.warning(f"Faltan {TOTAL_FILAS - total:,} filas. Relanza el script para continuar.")


# ─────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(description="Descarga del dataset 2020 Yellow Taxi a bronze")
    p.add_argument("--salida", type=Path, default=Path("./bronze"),
                   help="Carpeta destino (por defecto ./bronze)")
    p.add_argument("--filas-por-fichero", type=int, default=1_000_000,
                   help="Filas por fichero CSV (por defecto 1.000.000)")
    p.add_argument("--max-filas", type=int, default=0,
                   help="Límite de filas, para pruebas (0 = todas)")
    p.add_argument("--verificar", action="store_true",
                   help="Solo cuenta lo descargado, no descarga nada")
    args = p.parse_args()

    if args.verificar:
        verificar(args.salida)
        return

    token = os.getenv("SOCRATA_APP_TOKEN")
    descargar(args.salida, args.filas_por_fichero, args.max_filas, token)
    log.info("Verificando…")
    verificar(args.salida)


if __name__ == "__main__":
    main()
