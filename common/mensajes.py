"""
PIDS Parte 2 — Contrato del flujo en vivo: formato de mensaje y escritura.

El simulador emite viajes como mensajes (diccionarios JSON) y hay dos
caminos para que lleguen al tier caliente:

    simulador --sumidero postgres  ─────────────────────┐
                                                        ├─▶ escribir()
    simulador --sumidero kafka ─▶ trips.raw ─▶ consumidor ┘

Los dos acaban aquí, en la misma función, para que un viaje reciba
exactamente el mismo trato venga por donde venga: comprobación de
esquema, contrato de datos (common.validacion) y escritura en
taxi_trips o en trips_cuarentena.

FORMATO DEL MENSAJE (esquema_version 1.0)
    trip_id                UUID generado por el simulador
    event_time             ISO 8601 con zona: tiempo de SISTEMA, "ahora"
    tpep_pickup_datetime   ISO 8601 sin zona: tiempo de NEGOCIO, 2020
    tpep_dropoff_datetime  ISO 8601 sin zona
    <resto de COLUMNAS_NEGOCIO>   números JSON o null
    esquema_version        "1.0"
    fichero_origen         opcional, de qué semilla sale

Las dos columnas de tiempo viajan desde el primer mensaje (§3.1).

IDEMPOTENCIA
    El trip_id lo pone el simulador, no PostgreSQL. Si el consumidor se
    cae después del commit en PostgreSQL y antes de confirmar el offset,
    Kafka le vuelve a entregar el lote; con ON CONFLICT DO NOTHING esas
    filas no se duplican. En cuarentena hace lo mismo el índice único de
    postgres/init/05_p2.sql. Resultado: al menos una vez en el transporte
    y exactamente una vez en la base de datos.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd
import psycopg2.extras

from . import esquema, validacion

CAMPOS_TIEMPO_NEGOCIO = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
CAMPOS_NUMERICOS = [c for c in esquema.COLUMNAS_NEGOCIO
                    if c not in CAMPOS_TIEMPO_NEGOCIO and c != "store_and_fwd_flag"]
CAMPOS_OBLIGATORIOS = ["trip_id", "event_time", "esquema_version", *esquema.COLUMNAS_NEGOCIO]

# Motivos de rechazo propios del transporte. Se suman a los de
# common.validacion en trips_cuarentena.motivos y salen en v_calidad.
MOTIVO_NO_ES_JSON = "mensaje_ilegible"
MOTIVO_CAMPO_AUSENTE = "campo_ausente"
MOTIVO_TIPO_INVALIDO = "tipo_invalido"
MOTIVO_VERSION = "version_esquema_desconocida"

_COLUMNAS_INSERT = (["trip_id", "event_time"] + esquema.COLUMNAS_NEGOCIO
                    + ["origen", "fichero_origen", "esquema_version", "avisos"])


# ─────────────────────────────────────────────────────────────
# COMPROBACIÓN DE ESQUEMA
# common.validacion trabaja sobre un DataFrame ya tipado. Antes hay que
# saber si el mensaje se puede convertir en una fila: si le faltan
# campos o trae texto donde va un número, no hay regla de calidad que
# aplicar. Eso son los "esquemas rotos".
# ─────────────────────────────────────────────────────────────
def _es_numero(v) -> bool:
    return v is None or (isinstance(v, (int, float)) and not isinstance(v, bool))


def _es_fecha(v, obligatoria: bool) -> bool:
    if v is None:
        return not obligatoria
    if not isinstance(v, str):
        return False
    # fromisoformat y no pd.to_datetime: se llama tres veces por mensaje
    # y pandas, elemento a elemento, se comía más de la mitad del tiempo.
    try:
        datetime.fromisoformat(v)
    except ValueError:
        return False
    return True


def motivos_esquema(msg) -> list[str]:
    """Lista de problemas de forma del mensaje. Vacía = se puede tipar."""
    if not isinstance(msg, dict):
        return [MOTIVO_NO_ES_JSON]

    motivos = []
    if any(c not in msg for c in CAMPOS_OBLIGATORIOS):
        motivos.append(MOTIVO_CAMPO_AUSENTE)

    if "esquema_version" in msg and msg["esquema_version"] != esquema.ESQUEMA_VERSION:
        motivos.append(MOTIVO_VERSION)

    tipos_ok = (
        _es_fecha(msg.get("event_time"), obligatoria="event_time" in msg)
        and all(_es_fecha(msg.get(c), obligatoria=False) for c in CAMPOS_TIEMPO_NEGOCIO)
        and all(_es_numero(msg.get(c)) for c in CAMPOS_NUMERICOS)
        and (msg.get("store_and_fwd_flag") is None
             or isinstance(msg.get("store_and_fwd_flag"), str))
    )
    try:
        uuid.UUID(str(msg.get("trip_id")))
    except ValueError:
        tipos_ok = False
    if not tipos_ok:
        motivos.append(MOTIVO_TIPO_INVALIDO)

    return motivos


def a_dataframe(mensajes: list[dict]) -> pd.DataFrame:
    """Convierte mensajes con el esquema correcto a un DataFrame tipado.

    event_time queda con zona (UTC). Las fechas de negocio quedan SIN zona,
    igual que las deja common.esquema.leer_csv: así las comparaciones con
    los umbrales de common.validacion funcionan igual en los dos caminos.
    """
    df = pd.DataFrame(mensajes)
    df["event_time"] = pd.to_datetime(df["event_time"], format="ISO8601", utc=True)
    for c in CAMPOS_TIEMPO_NEGOCIO:
        s = pd.to_datetime(df[c], format="ISO8601", errors="coerce")
        df[c] = s.dt.tz_localize(None) if getattr(s.dt, "tz", None) is not None else s
    return esquema.aplicar_tipos(df)


# ─────────────────────────────────────────────────────────────
# ESCRITURA
# ─────────────────────────────────────────────────────────────
@dataclass
class ResultadoLote:
    recibidos: int = 0
    insertados: int = 0
    duplicados: int = 0          # ya estaban: reentrega de Kafka
    cuarentena: int = 0
    por_motivo: dict = field(default_factory=dict)

    def sumar(self, otro: "ResultadoLote") -> None:
        self.recibidos += otro.recibidos
        self.insertados += otro.insertados
        self.duplicados += otro.duplicados
        self.cuarentena += otro.cuarentena
        for k, v in otro.por_motivo.items():
            self.por_motivo[k] = self.por_motivo.get(k, 0) + v


def _nativo(v):
    """numpy/pandas -> tipos de Python que psycopg2 sabe adaptar."""
    if v is None or v is pd.NaT:
        return None
    if isinstance(v, (list, tuple)):
        return list(v)
    if pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v.item() if hasattr(v, "item") else v


def _payload(msg) -> str:
    if isinstance(msg, dict):
        return json.dumps(msg, default=str, ensure_ascii=False)
    return json.dumps({"crudo": msg if isinstance(msg, str) else repr(msg)}, ensure_ascii=False)


def asegurar_particiones(conn, dias: int = 1) -> None:
    """Crea las particiones de hoy y de los próximos `dias` si faltan.

    Es el DAG mantener_particiones quien las mantiene; esto es solo una red
    de seguridad al arrancar, para que el primer lote de una base recién
    creada no acabe entero en taxi_trips_default. Idempotente.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT crear_particiones_adelanto(%s)", (dias,))
    conn.commit()


def escribir(conn, mensajes: list, origen: str = "stream",
             page_size: int = 500) -> ResultadoLote:
    """Valida un lote de mensajes y lo escribe en una sola transacción.

    Hace commit al final. Quien llama (el consumidor de Kafka) solo debe
    confirmar sus offsets DESPUÉS de que esto vuelva sin excepción.
    """
    res = ResultadoLote(recibidos=len(mensajes))
    cuarentena: list[tuple] = []       # (fichero_origen, motivos, payload)

    def a_cuarentena(msg, motivos):
        fichero = msg.get("fichero_origen") if isinstance(msg, dict) else None
        cuarentena.append((fichero, motivos, _payload(msg)))
        for m in motivos:
            res.por_motivo[m] = res.por_motivo.get(m, 0) + 1

    # 1. Esquema
    bien_formados = []
    for msg in mensajes:
        motivos = motivos_esquema(msg)
        if motivos:
            a_cuarentena(msg, motivos)
        else:
            bien_formados.append(msg)

    # 2. Contrato de datos
    filas = []
    if bien_formados:
        df = a_dataframe(bien_formados)
        r = validacion.validar(df)

        for i, fila in r.rechazados.iterrows():
            a_cuarentena(bien_formados[i], list(fila["motivos"]))

        for _, fila in r.validos.iterrows():
            filas.append(tuple(
                [str(fila["trip_id"]), fila["event_time"].to_pydatetime()]
                + [_nativo(fila[c]) for c in esquema.COLUMNAS_NEGOCIO]
                + [origen, _nativo(fila.get("fichero_origen")),
                   esquema.ESQUEMA_VERSION, list(fila["avisos"])]
            ))

    # 3. Escritura, todo o nada
    with conn.cursor() as cur:
        if filas:
            devueltas = psycopg2.extras.execute_values(
                cur,
                f"""INSERT INTO taxi_trips ({', '.join(_COLUMNAS_INSERT)}) VALUES %s
                    ON CONFLICT (trip_id, event_time) DO NOTHING
                    RETURNING 1""",
                filas, page_size=page_size, fetch=True,
            )
            res.insertados = len(devueltas)
            res.duplicados = len(filas) - res.insertados

        if cuarentena:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO trips_cuarentena (origen, fichero_origen, motivos, payload)
                   VALUES %s
                   ON CONFLICT ((payload->>'trip_id')) WHERE payload ? 'trip_id'
                   DO NOTHING""",
                [(origen, f, m, p) for f, m, p in cuarentena],
                template="(%s, %s, %s, %s::jsonb)", page_size=page_size,
            )
            res.cuarentena = len(cuarentena)
    conn.commit()
    return res
