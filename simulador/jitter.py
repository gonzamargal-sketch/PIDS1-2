"""
PIDS Parte 2 — Jitter y suciedad del simulador (P2, T2.1).

EL JITTER NO ES UN ADORNO
    Si el simulador repite las mismas mil filas, la codificación por
    diccionario de Parquet comprime los valores idénticos casi gratis y
    el ratio del frío sale ~35x en vez de ~7,5x. Nadie se lo creería,
    con razón. Aquí cada viaje sale distinto de su semilla:

      fecha de negocio  el viaje ocurre AHORA: termina en el último minuto
                        y empieza lo que dure antes. Nunca queda en el
                        futuro, que la validación lo rechazaría
      distancia         factor lognormal (~±15%); la duración se escala
                        con el mismo factor para que la velocidad siga
                        siendo creíble
      importes          tarifa y propina con su propio factor; el total
                        se ajusta sumando la diferencia, así sigue
                        cuadrando con sus componentes
      zonas             una parte de los viajes cambia de origen/destino
      pasajeros         una parte gana o pierde uno

    Lo que ya es raro en la semilla (importes negativos, distancia cero,
    pasajeros a cero) se conserva: son anomalías reales y la validación
    tiene que seguir viéndolas.

LA SUCIEDAD EXTRA
    Solo los casos que NO aparecen en el dataset de forma natural:
      nulo              falta la fecha de recogida
      fecha_imposible   recogida en otro año o bajada antes que subida
      esquema_roto      falta un campo, texto donde va un número o
                        versión de esquema desconocida
"""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd

from common import esquema

# Los viajes del flujo en vivo terminan en este último intervalo, para que
# no acaben todos en el mismo segundo
VENTANA_FIN_S = 60

SIGMA_DISTANCIA = 0.15
SIGMA_IMPORTE = 0.12
PROB_CAMBIO_ZONA = 0.35
PROB_CAMBIO_PASAJEROS = 0.20
ZONAS_VALIDAS = (1, 263)   # sin 264/265, que son "desconocida"

TIPOS_SUCIEDAD = ("nulo", "fecha_imposible", "esquema_roto")


def perturbar(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Devuelve una copia de `df` con cada fila variada respecto a su semilla."""
    df = df.copy()
    n = len(df)

    # ── Fechas de negocio: el viaje acaba de terminar ───────────────────
    # UTC sin zona, como el resto del contrato de datos
    duracion = df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ahora = pd.Timestamp.now(tz="UTC").tz_localize(None)
    fin = ahora - pd.to_timedelta(rng.integers(0, VENTANA_FIN_S, n), unit="s")
    fin = pd.Series(fin, index=df.index)

    # ── Distancia y duración con el mismo factor ────────────────────────
    f_dist = np.exp(rng.normal(0, SIGMA_DISTANCIA, n))
    df["trip_distance"] = (df["trip_distance"] * f_dist).round(2)
    nueva_duracion = duracion * f_dist
    # Sin bajada en la semilla: se conserva sin bajada, recogida = fin
    df["tpep_pickup_datetime"] = (fin - nueva_duracion.fillna(pd.Timedelta(0))).dt.floor("s")
    df["tpep_dropoff_datetime"] = fin.where(duracion.notna()).dt.floor("s")

    # ── Importes: tarifa y propina varían, el total absorbe la diferencia ─
    f_tarifa = np.exp(rng.normal(0, SIGMA_IMPORTE, n))
    f_propina = np.exp(rng.normal(0, SIGMA_IMPORTE * 2, n))
    tarifa = (df["fare_amount"] * f_tarifa).round(2)
    propina = (df["tip_amount"] * f_propina).round(2)
    df["total_amount"] = (df["total_amount"]
                          + (tarifa - df["fare_amount"])
                          + (propina - df["tip_amount"])).round(2)
    df["fare_amount"] = tarifa
    df["tip_amount"] = propina

    # ── Zonas ──────────────────────────────────────────────────────────
    for col in ("pu_location_id", "do_location_id"):
        cambia = rng.random(n) < PROB_CAMBIO_ZONA
        nuevas = rng.integers(ZONAS_VALIDAS[0], ZONAS_VALIDAS[1] + 1, n)
        df[col] = df[col].mask(cambia, pd.Series(nuevas, index=df.index)).astype("Int16")

    # ── Pasajeros: solo los que ya tenían, para no borrar el aviso real ─
    p = df["passenger_count"]
    cambia = (rng.random(n) < PROB_CAMBIO_PASAJEROS) & p.notna() & (p > 0)
    delta = pd.Series(rng.choice([-1, 1], n), index=df.index)
    df["passenger_count"] = p.mask(cambia, (p + delta).clip(1, 6)).astype("Int16")

    return df


def ensuciar(msg: dict, rng: np.random.Generator) -> dict:
    """Estropea un mensaje con uno de los tipos de suciedad extra."""
    tipo = TIPOS_SUCIEDAD[rng.integers(len(TIPOS_SUCIEDAD))]
    if tipo == "fecha_imposible" and msg.get("tpep_pickup_datetime") is None:
        tipo = "nulo"
    msg = dict(msg)
    msg["_suciedad"] = tipo       # ayuda a depurar; la validación la ignora

    if tipo == "nulo":
        msg["tpep_pickup_datetime"] = None

    elif tipo == "fecha_imposible":
        if rng.random() < 0.5:
            anio = int(rng.choice([1999, 2035, 2099]))
            msg["tpep_pickup_datetime"] = f"{anio}" + msg["tpep_pickup_datetime"][4:]
        else:
            # La bajada una hora antes de la subida
            subida = pd.Timestamp(msg["tpep_pickup_datetime"])
            msg["tpep_dropoff_datetime"] = (subida - timedelta(hours=1)).isoformat()

    else:  # esquema_roto
        caso = rng.integers(3)
        if caso == 0:
            campo = esquema.COLUMNAS_NEGOCIO[rng.integers(len(esquema.COLUMNAS_NEGOCIO))]
            msg.pop(campo, None)
        elif caso == 1:
            msg["total_amount"] = "N/A"
        else:
            msg["esquema_version"] = "0.9"

    return msg
