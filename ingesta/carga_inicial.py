#!/usr/bin/env python3
"""
PIDS Parte 2 — Paso 5: carga inicial BRONZE -> tiers.

    bronze (crudo)  ->  contrato de datos  ->  Iceberg      (más viejo que la política)
                                 |         ->  PostgreSQL   (dentro de la política)
                                 +-------->  trips_cuarentena

CADA FILA VA AL TIER QUE LE TOCA POR EDAD
    Los datos son de 2026, desde el 1 de enero hasta hoy (los genera
    scripts/generar_datos_sinteticos.py). Aquí event_time =
    tpep_pickup_datetime, porque para el histórico el tiempo de sistema y
    el de negocio coinciden, y con eso se decide el tier con la MISMA
    regla que particiones_a_archivar():

        día de event_time <  (ahora - umbral ARCHIVE)::date  ->  Iceberg
        el resto                                              ->  PostgreSQL

    Así el reparto queda como si el sistema llevara funcionando desde
    enero: el frío con el histórico, el caliente con los últimos días, sin
    solape (el modelo mudanza). Y como el caliente ya tiene días
    anteriores a hoy, en cuanto se baja la política el job de archivado
    tiene particiones que mover.

Uso:
    python ingesta/carga_inicial.py                  # todo lo de bronze
    python ingesta/carga_inicial.py --max-filas 5000 # prueba rápida
    python ingesta/carga_inicial.py --local datos/sinteticos/sintetico_3000000.csv
    python ingesta/carga_inicial.py --recrear        # borra la carga anterior y la rehace
"""

from __future__ import annotations

import io
import sys
import uuid
import json
import logging
import argparse
from pathlib import Path

if sys.stdout.encoding is not None and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

import boto3
import pandas as pd
import psycopg2
import psycopg2.extras
from botocore.client import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import esquema, validacion, lakehouse   # noqa: E402
from common.config import MINIO, PG, ICEBERG        # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("carga")

PREFIJO = "nyc-taxi/2026"
LOTE = 200_000        # filas por append a Iceberg
LOTE_PG = 5_000       # filas por INSERT en PostgreSQL


def cliente_s3():
    return boto3.client(
        "s3", endpoint_url=MINIO.url,
        aws_access_key_id=MINIO.access_key,
        aws_secret_access_key=MINIO.secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1")


def listar_bronze(s3) -> list[str]:
    claves, token = [], None
    while True:
        kw = {"Bucket": MINIO.bucket_bronze, "Prefix": PREFIJO}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        claves += [o["Key"] for o in r.get("Contents", []) if o["Key"].endswith(".csv")]
        if not r.get("IsTruncated"):
            break
        token = r["NextContinuationToken"]
    return sorted(claves)


def leer_de_bronze(s3, clave: str, max_filas: int | None) -> pd.DataFrame:
    cuerpo = s3.get_object(Bucket=MINIO.bucket_bronze, Key=clave)["Body"].read()
    df = pd.read_csv(io.BytesIO(cuerpo), nrows=max_filas, low_memory=False)
    df = esquema.normalizar_columnas(df)
    df = esquema.parsear_fechas(df)
    return esquema.aplicar_tipos(df)


def guardar_cuarentena(rechazados: pd.DataFrame, fichero: str) -> int:
    """Los rechazados no se tiran: se guardan con su motivo en PostgreSQL."""
    if rechazados.empty:
        return 0
    conn = psycopg2.connect(**PG.kwargs)
    try:
        cur = conn.cursor()
        filas = []
        for _, r in rechazados.iterrows():
            payload = {k: (None if pd.isna(v) else str(v))
                       for k, v in r.items() if k != "motivos"}
            filas.append(("carga_inicial", fichero, list(r["motivos"]),
                          json.dumps(payload)))
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO trips_cuarentena (origen, fichero_origen, motivos, payload)
               VALUES %s""", filas, page_size=1000)
        conn.commit()
        return len(filas)
    finally:
        conn.close()


def preparar(df: pd.DataFrame, avisos, fichero: str) -> pd.DataFrame:
    """Añade las columnas de sistema antes de escribir en Iceberg."""
    v = df.copy()
    # Para el histórico, tiempo de sistema = tiempo de negocio
    v["event_time"] = v["tpep_pickup_datetime"]
    v["trip_id"] = [str(uuid.uuid4()) for _ in range(len(v))]
    v["origen"] = "carga_inicial"
    v["fichero_origen"] = fichero
    v["esquema_version"] = esquema.ESQUEMA_VERSION
    v["avisos"] = avisos
    return v


def conectar_pg():
    conn = psycopg2.connect(**PG.kwargs)
    with conn.cursor() as cur:
        # Las particiones diarias van a medianoche UTC
        cur.execute("SET TIME ZONE 'UTC'")
    conn.commit()
    return conn


def dia_de_corte(conn) -> "pd.Timestamp":
    """Primer día que se queda en caliente, con la política vigente."""
    with conn.cursor() as cur:
        cur.execute("SELECT (NOW() - umbral_intervalo('trips','ARCHIVE'))::date")
        corte = cur.fetchone()[0]
    conn.commit()
    return pd.Timestamp(corte)


def a_postgres(conn, v: pd.DataFrame) -> int:
    """Inserta en el caliente, creando antes las particiones diarias."""
    if v.empty:
        return 0
    cols = (["trip_id", "event_time"] + esquema.COLUMNAS_NEGOCIO
            + ["origen", "fichero_origen", "esquema_version", "avisos"])
    with conn.cursor() as cur:
        for dia in sorted(v["event_time"].dt.date.unique()):
            cur.execute("SELECT crear_particion_dia(%s)", (dia,))
        datos = v[cols].astype(object).where(v[cols].notna(), None)
        datos["avisos"] = [list(a) for a in v["avisos"]]
        psycopg2.extras.execute_values(
            cur,
            f"INSERT INTO taxi_trips ({', '.join(cols)}) VALUES %s",
            datos.itertuples(index=False, name=None), page_size=LOTE_PG)
    conn.commit()
    return len(v)


def cargar(max_filas: int | None, local: Path | None, recrear: bool) -> int:
    cat = lakehouse.obtener_catalogo()
    conn = conectar_pg()

    if recrear:
        try:
            cat.drop_table(ICEBERG.identificador)
            log.warning("Tabla %s eliminada", ICEBERG.identificador)
        except Exception:
            pass
        with conn.cursor() as cur:
            cur.execute("DELETE FROM taxi_trips WHERE origen = 'carga_inicial'")
            log.warning("%s filas de una carga anterior borradas del caliente",
                        f"{cur.rowcount:,}")
        conn.commit()

    corte = dia_de_corte(conn)
    log.info("Reparto por edad: antes del %s -> Iceberg, desde ese día -> PostgreSQL",
             corte.date())

    tabla = lakehouse.obtener_tabla(cat)
    antes = lakehouse.estadisticas(tabla)
    log.info("Tabla %s — antes: %s filas", ICEBERG.identificador, f"{antes['filas']:,}")

    # Fuente: un CSV local o todo lo que haya en bronze
    if local:
        fuentes = [("local", local.name, lambda: esquema.leer_csv(local, max_filas))]
    else:
        s3 = cliente_s3()
        claves = listar_bronze(s3)
        if not claves:
            log.error("Bronze vacío. Ejecuta antes: python ingesta/subir_bronze.py")
            return 1
        log.info("%d fichero(s) en bronze", len(claves))
        fuentes = [("bronze", Path(k).name,
                    (lambda k=k: leer_de_bronze(s3, k, max_filas))) for k in claves]

    total_ok = total_ko = total_pg = 0

    for origen, nombre, leer in fuentes:
        log.info("─" * 60)
        log.info("%s: %s", origen, nombre)

        df = leer()
        log.info("  %s filas leídas", f"{len(df):,}")

        res = validacion.validar(df)
        log.info("  válidas %s | rechazadas %s (%.2f%%) | con avisos %s",
                 f"{len(res.validos):,}", f"{len(res.rechazados):,}",
                 res.resumen["pct_rechazo"], f"{res.resumen['con_avisos']:,}")

        n_ko = guardar_cuarentena(res.rechazados, nombre)
        total_ko += n_ko

        if res.validos.empty:
            continue

        v = preparar(res.validos, res.validos["avisos"], nombre)

        caliente = v["event_time"] >= corte
        n_pg = a_postgres(conn, v.loc[caliente])
        total_pg += n_pg
        log.info("  %s filas al caliente (PostgreSQL)", f"{n_pg:,}")
        v = v.loc[~caliente]

        # Escribir por lotes: ficheros grandes desde el principio, que es
        # como evitamos el problema de los ficheros pequeños sin compactar
        for ini in range(0, len(v), LOTE):
            trozo = v.iloc[ini:ini + LOTE]
            tabla.append(lakehouse.df_a_arrow(trozo))
            total_ok += len(trozo)
            log.info("  append %s filas (acumulado %s)",
                     f"{len(trozo):,}", f"{total_ok:,}")

    conn.close()
    despues = lakehouse.estadisticas(tabla)
    log.info("═" * 60)
    log.info("CARGA INICIAL TERMINADA")
    log.info("  insertadas en Iceberg : %s", f"{total_ok:,}")
    log.info("  insertadas en caliente: %s", f"{total_pg:,}")
    log.info("  a cuarentena          : %s", f"{total_ko:,}")
    log.info("  tabla ahora           : %s filas, %s ficheros, %s particiones",
             f"{despues['filas']:,}", despues["ficheros"], despues["particiones"])
    log.info("  tamaño                : %.1f MB  (%.1f B/fila)",
             despues["bytes"] / 1e6, despues["bytes_por_fila"] or 0)
    log.info("  snapshot              : %s", despues["snapshot_id"])
    log.info("═" * 60)
    log.info("Comparad ese B/fila con el de PostgreSQL:")
    log.info("  SELECT * FROM v_coste_por_tier;")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Carga inicial bronze -> Iceberg")
    p.add_argument("--max-filas", type=int, default=None,
                   help="Límite de filas por fichero (para pruebas)")
    p.add_argument("--local", type=Path, default=None,
                   help="Cargar un CSV local en vez de bronze")
    p.add_argument("--recrear", action="store_true",
                   help="Borra la tabla Iceberg y la carga anterior del caliente, "
                        "y lo vuelve a cargar todo")
    a = p.parse_args()
    return cargar(a.max_filas, a.local, a.recrear)


if __name__ == "__main__":
    raise SystemExit(main())
