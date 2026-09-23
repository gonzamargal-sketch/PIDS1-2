"""
PIDS Parte 2 — Contrato de datos: esquema y lectura.

Un solo sitio define cómo se lee el dataset NYC Yellow Taxi y cómo se
traduce al esquema de PostgreSQL. Lo importan el cargador inicial, el
simulador y el consumidor de Kafka, para que no se desincronicen.

DOS TRAMPAS QUE ESTE MÓDULO RESUELVE
────────────────────────────────────
1. El portal devuelve NOMBRES DE COLUMNA DISTINTOS según cómo bajes:
       export CSV  ->  VendorID, PULocationID, RatecodeID   (CamelCase)
       API SODA    ->  vendorid, pulocationid, ratecodeid   (minúsculas)
   Si el equipo mezcla ambas descargas, el cargador falla en unos ficheros
   y no en otros. Aquí el mapeo es insensible a mayúsculas.

2. Y FORMATOS DE FECHA DISTINTOS:
       export CSV  ->  01/01/2020 12:28:15 AM     (americano con AM/PM)
       API SODA    ->  2020-01-01T00:28:15.000    (ISO)
   Y esto es peor, porque si se deja que pandas lo adivine, en algún
   fichero confundirá día y mes SIN DAR NINGÚN ERROR. Tendríais viajes
   mal fechados y no os enteraríais.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

ESQUEMA_VERSION = "1.0"

# Formatos de fecha observados en las dos vías de descarga
FORMATO_FECHA_EXPORT = "%m/%d/%Y %I:%M:%S %p"   # 01/01/2020 12:28:15 AM
FORMATO_FECHA_API = "ISO8601"                   # 2020-01-01T00:28:15.000

# ─────────────────────────────────────────────────────────────
# Mapeo columna del portal -> columna de PostgreSQL.
# Las claves van en minúsculas: la normalización las pasa a minúsculas
# antes de buscar, así funcionan tanto "VendorID" como "vendorid".
# ─────────────────────────────────────────────────────────────
MAPEO_COLUMNAS: dict[str, str] = {
    "vendorid":              "vendor_id",
    "tpep_pickup_datetime":  "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count":       "passenger_count",
    "trip_distance":         "trip_distance",
    "ratecodeid":            "ratecode_id",
    "store_and_fwd_flag":    "store_and_fwd_flag",
    "pulocationid":          "pu_location_id",
    "dolocationid":          "do_location_id",
    "payment_type":          "payment_type",
    "fare_amount":           "fare_amount",
    "extra":                 "extra",
    "mta_tax":               "mta_tax",
    "tip_amount":            "tip_amount",
    "tolls_amount":          "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount":          "total_amount",
    "congestion_surcharge":  "congestion_surcharge",
}

COLUMNAS_FECHA = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

# Columnas que admiten NULL en el dataset real de 2020 (~810k nulos en total)
COLUMNAS_NULLABLE = {
    "passenger_count", "ratecode_id", "store_and_fwd_flag",
    "congestion_surcharge", "tpep_dropoff_datetime",
}

# Orden canónico para insertar en taxi_trips (sin las de sistema)
COLUMNAS_NEGOCIO = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "vendor_id", "passenger_count", "trip_distance", "ratecode_id",
    "store_and_fwd_flag", "pu_location_id", "do_location_id", "payment_type",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge",
]

# Columnas que añade el sistema
COLUMNAS_SISTEMA = ["event_time", "origen", "fichero_origen", "esquema_version", "avisos"]

DTYPES = {
    "vendor_id":             "Int16",
    "passenger_count":       "Int16",
    "trip_distance":         "float64",
    "ratecode_id":           "Int16",
    "store_and_fwd_flag":    "string",
    "pu_location_id":        "Int16",
    "do_location_id":        "Int16",
    "payment_type":          "Int16",
    "fare_amount":           "float64",
    "extra":                 "float64",
    "mta_tax":               "float64",
    "tip_amount":            "float64",
    "tolls_amount":          "float64",
    "improvement_surcharge": "float64",
    "total_amount":          "float64",
    "congestion_surcharge":  "float64",
}


# ─────────────────────────────────────────────────────────────
def normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """Renombra las columnas del portal al esquema de PostgreSQL.

    Acepta indistintamente los nombres del export CSV (CamelCase) y los
    de la API SODA (minúsculas).
    """
    renombrado = {}
    desconocidas = []

    for col in df.columns:
        destino = MAPEO_COLUMNAS.get(col.strip().lower())
        if destino:
            renombrado[col] = destino
        else:
            desconocidas.append(col)

    if desconocidas:
        log.warning("Columnas no reconocidas y descartadas: %s", desconocidas)

    df = df.rename(columns=renombrado)
    faltan = set(MAPEO_COLUMNAS.values()) - set(df.columns)
    if faltan:
        log.warning("Faltan columnas esperadas: %s", sorted(faltan))
        for c in faltan:
            df[c] = pd.NA

    return df[[c for c in MAPEO_COLUMNAS.values() if c in df.columns]]


def parsear_fechas(df: pd.DataFrame) -> pd.DataFrame:
    """Parsea las columnas de fecha probando los dos formatos conocidos.

    Nunca deja que pandas infiera: con fechas americanas la inferencia
    confunde día y mes en silencio.
    """
    for col in COLUMNAS_FECHA:
        if col not in df.columns:
            continue
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue

        serie = df[col].astype("string")
        muestra = serie.dropna()

        parseada = None
        if len(muestra) > 0:
            # ¿Formato americano con AM/PM?
            if muestra.iloc[0].strip().count("/") == 2:
                parseada = pd.to_datetime(serie, format=FORMATO_FECHA_EXPORT, errors="coerce")
                usado = FORMATO_FECHA_EXPORT
            else:
                parseada = pd.to_datetime(serie, format=FORMATO_FECHA_API, errors="coerce")
                usado = FORMATO_FECHA_API

            fallidas = parseada.isna().sum() - serie.isna().sum()
            if fallidas > 0:
                log.warning("%s: %d valores no parseables con %s", col, fallidas, usado)

        df[col] = parseada if parseada is not None else pd.NaT

    return df


def aplicar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte a los tipos del esquema, tolerando basura sin reventar."""
    for col, tipo in DTYPES.items():
        if col not in df.columns:
            continue
        try:
            if tipo.startswith("Int"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(tipo)
            elif tipo == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(tipo)
        except (ValueError, TypeError) as e:
            log.warning("No se pudo convertir %s a %s: %s", col, tipo, e)

    if "store_and_fwd_flag" in df.columns:
        df["store_and_fwd_flag"] = df["store_and_fwd_flag"].str.strip().str[:1]

    return df


def leer_csv(ruta: str | Path, max_filas: int | None = None) -> pd.DataFrame:
    """Lee un CSV del dataset y lo devuelve normalizado y tipado.

    Es el único punto de entrada para leer datos crudos. Si algo cambia en
    el formato de origen, se arregla aquí y no en cinco sitios.
    """
    ruta = Path(ruta)
    log.info("Leyendo %s", ruta)

    df = pd.read_csv(ruta, nrows=max_filas, low_memory=False)
    log.info("  %d filas, %d columnas en origen", len(df), len(df.columns))

    df = normalizar_columnas(df)
    df = parsear_fechas(df)
    df = aplicar_tipos(df)

    log.info("  normalizado a %d columnas", len(df.columns))
    return df
