"""
PIDS Parte 2 — Contrato de datos: reglas de calidad.

Las reglas salen de perfilar el dataset real, no de imaginarlo. Sobre la
muestra de 1.000 filas falla el 3,7%, y en el dataset completo de 24,6M
hay cosas peores: viajes fechados en 2002, tarifas de 998.310 $ y unos
810.000 nulos repartidos en seis columnas.

Por eso NO inyectamos suciedad falsa para las anomalías que ya existen.
Los importes negativos son devoluciones reales y las distancias cero son
carreras canceladas o con el GPS fallando. Es mejor material para la
memoria que cualquier error inventado.

DOS SEVERIDADES, porque no todo lo raro es un error:

    RECHAZO  El registro no puede ser cierto. Va a trips_cuarentena con
             su motivo. No entra en taxi_trips.

    AVISO    El registro es sospechoso pero plausible. Entra en
             taxi_trips con una marca en la columna `avisos`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd

# ─────────────────────────────────────────────────────────────
# Umbrales. Están aquí arriba para poder importarlos sueltos desde el
# consumidor de Kafka, el simulador o la API sin arrastrar el módulo entero.
# ─────────────────────────────────────────────────────────────

# El dataset dice "2020" pero trae viajes de 2002 a 2021. Damos margen a
# ambos lados para los viajes de fin de año, y cortamos el resto.
FECHA_MIN = pd.Timestamp("2019-12-01")
FECHA_MAX = pd.Timestamp("2021-02-01")

IMPORTE_MAX = 10_000.0      # el máximo real del dataset es 998.310,03
DISTANCIA_MAX = 500.0       # millas
DURACION_MAX_MIN = 360.0    # 6 horas
DURACION_MIN_MIN = 1.0
VELOCIDAD_MAX_MPH = 100.0
ZONA_MIN, ZONA_MAX = 1, 265
ZONAS_DESCONOCIDAS = {264, 265}   # "Unknown" y "NV" en el diccionario del TLC

RECHAZO = "rechazo"
AVISO = "aviso"


@dataclass(frozen=True)
class Regla:
    nombre: str
    severidad: str
    descripcion: str
    # Devuelve una Series booleana: True = la fila INCUMPLE la regla
    predicado: Callable[[pd.DataFrame], pd.Series]


def _duracion_min(df: pd.DataFrame) -> pd.Series:
    return (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60.0


def _velocidad_mph(df: pd.DataFrame) -> pd.Series:
    horas = _duracion_min(df) / 60.0
    return df["trip_distance"] / horas.where(horas > 0)


# ─────────────────────────────────────────────────────────────
# RECHAZOS: el registro no puede ser cierto
# ─────────────────────────────────────────────────────────────
REGLAS: list[Regla] = [
    Regla(
        "fecha_fuera_de_rango", RECHAZO,
        f"tpep_pickup_datetime fuera de {FECHA_MIN.date()}..{FECHA_MAX.date()}",
        lambda d: d["tpep_pickup_datetime"].isna()
                  | (d["tpep_pickup_datetime"] < FECHA_MIN)
                  | (d["tpep_pickup_datetime"] > FECHA_MAX),
    ),
    Regla(
        "cronologia_invalida", RECHAZO,
        "La bajada ocurre antes o a la vez que la subida",
        lambda d: d["tpep_dropoff_datetime"].notna()
                  & (d["tpep_dropoff_datetime"] <= d["tpep_pickup_datetime"]),
    ),
    Regla(
        "importe_excesivo", RECHAZO,
        f"total_amount o fare_amount por encima de {IMPORTE_MAX:,.0f}",
        lambda d: (d["total_amount"].abs() > IMPORTE_MAX)
                  | (d["fare_amount"].abs() > IMPORTE_MAX),
    ),
    Regla(
        "distancia_excesiva", RECHAZO,
        f"trip_distance por encima de {DISTANCIA_MAX:,.0f} millas",
        lambda d: d["trip_distance"] > DISTANCIA_MAX,
    ),
    Regla(
        "duracion_excesiva", RECHAZO,
        f"Viaje de más de {DURACION_MAX_MIN/60:.0f} horas",
        lambda d: _duracion_min(d) > DURACION_MAX_MIN,
    ),
    Regla(
        "zona_fuera_de_rango", RECHAZO,
        f"pu/do_location_id fuera de {ZONA_MIN}..{ZONA_MAX}",
        lambda d: (~d["pu_location_id"].between(ZONA_MIN, ZONA_MAX))
                  | (~d["do_location_id"].between(ZONA_MIN, ZONA_MAX)),
    ),

    # ─────────────────────────────────────────────────────────
    # AVISOS: sospechoso pero plausible, se acepta marcado
    # ─────────────────────────────────────────────────────────
    Regla(
        "importe_negativo", AVISO,
        "total_amount negativo: suele ser una devolución o una corrección",
        lambda d: d["total_amount"] < 0,
    ),
    Regla(
        "sin_pasajeros", AVISO,
        "passenger_count nulo o cero (~2,1% del dataset)",
        lambda d: d["passenger_count"].isna() | (d["passenger_count"] == 0),
    ),
    Regla(
        "distancia_cero", AVISO,
        "trip_distance cero: carrera cancelada o fallo de GPS (~1,2%)",
        lambda d: d["trip_distance"] <= 0,
    ),
    Regla(
        "duracion_sospechosa", AVISO,
        f"Menos de {DURACION_MIN_MIN:.0f} minuto con distancia mayor que cero",
        lambda d: (_duracion_min(d) < DURACION_MIN_MIN) & (d["trip_distance"] > 0),
    ),
    Regla(
        "velocidad_imposible", AVISO,
        f"Velocidad media por encima de {VELOCIDAD_MAX_MPH:.0f} mph",
        lambda d: _velocidad_mph(d) > VELOCIDAD_MAX_MPH,
    ),
    Regla(
        "zona_desconocida", AVISO,
        "Zona 264 (Unknown) o 265 (NV) en origen o destino",
        lambda d: d["pu_location_id"].isin(ZONAS_DESCONOCIDAS)
                  | d["do_location_id"].isin(ZONAS_DESCONOCIDAS),
    ),
]

REGLAS_RECHAZO = [r for r in REGLAS if r.severidad == RECHAZO]
REGLAS_AVISO = [r for r in REGLAS if r.severidad == AVISO]


@dataclass
class Resultado:
    validos: pd.DataFrame       # con columna `avisos` (lista de str)
    rechazados: pd.DataFrame    # con columna `motivos` (lista de str)
    resumen: dict

    def __str__(self) -> str:
        t = len(self.validos) + len(self.rechazados)
        pct = 100 * len(self.rechazados) / t if t else 0
        return (f"{t:,} filas -> {len(self.validos):,} válidas, "
                f"{len(self.rechazados):,} rechazadas ({pct:.2f}%)")


def validar(df: pd.DataFrame) -> Resultado:
    """Aplica el contrato de datos y separa válidos de rechazados.

    Vectorizado: una pasada por regla, no una por fila. Con 24 millones
    de filas la diferencia es de horas.
    """
    if df.empty:
        return Resultado(df.copy(), df.copy(), {"total": 0})

    df = df.copy()
    conteos: dict[str, int] = {}

    # Evaluar todas las reglas de una vez
    violaciones: dict[str, pd.Series] = {}
    for regla in REGLAS:
        try:
            v = regla.predicado(df).fillna(False).astype(bool)
        except (KeyError, TypeError):
            # Si falta una columna, la regla no puede evaluarse: no rechaza
            v = pd.Series(False, index=df.index)
        violaciones[regla.nombre] = v
        conteos[regla.nombre] = int(v.sum())

    def etiquetas(reglas: list[Regla]) -> pd.Series:
        """Para cada fila, la lista de reglas de ese grupo que incumple."""
        acumulado = pd.Series([[] for _ in range(len(df))], index=df.index)
        for r in reglas:
            marca = violaciones[r.nombre]
            if marca.any():
                acumulado.loc[marca] = acumulado.loc[marca].apply(lambda xs, n=r.nombre: xs + [n])
        return acumulado

    motivos = etiquetas(REGLAS_RECHAZO)
    avisos = etiquetas(REGLAS_AVISO)

    es_rechazo = motivos.apply(len) > 0

    rechazados = df.loc[es_rechazo].copy()
    rechazados["motivos"] = motivos.loc[es_rechazo]

    validos = df.loc[~es_rechazo].copy()
    validos["avisos"] = avisos.loc[~es_rechazo]

    total = len(df)
    resumen = {
        "total": total,
        "validos": len(validos),
        "rechazados": len(rechazados),
        "pct_rechazo": round(100 * len(rechazados) / total, 3) if total else 0.0,
        "con_avisos": int((avisos.apply(len) > 0).sum()),
        "por_regla": conteos,
    }

    return Resultado(validos, rechazados, resumen)


def informe(resultado: Resultado) -> str:
    """Informe legible del resultado, para logs y para la memoria."""
    r = resultado.resumen
    lineas = [
        "─" * 64,
        f"CONTRATO DE DATOS — {r['total']:,} filas evaluadas",
        "─" * 64,
        f"  Válidas    : {r['validos']:,}",
        f"  Rechazadas : {r['rechazados']:,}  ({r['pct_rechazo']}%)",
        f"  Con avisos : {r['con_avisos']:,}",
        "",
        "  RECHAZOS (van a cuarentena)",
    ]
    for regla in REGLAS_RECHAZO:
        n = r["por_regla"][regla.nombre]
        if n:
            lineas.append(f"    {regla.nombre:<26} {n:>9,}  ({100*n/r['total']:.2f}%)")
    lineas.append("")
    lineas.append("  AVISOS (se aceptan marcados)")
    for regla in REGLAS_AVISO:
        n = r["por_regla"][regla.nombre]
        if n:
            lineas.append(f"    {regla.nombre:<26} {n:>9,}  ({100*n/r['total']:.2f}%)")
    lineas.append("─" * 64)
    return "\n".join(lineas)
