"""
PIDS Parte 2 — Transiciones de la máquina de estados del archivado.

Todo el SQL que toca `archival_jobs` vive aquí, para que el job no tenga
UPDATE sueltos por en medio y para que la purga (T1.2) reutilice la
conexión y la lectura de la política sin duplicarlas.

    PENDIENTE ──▶ ESCRIBIENDO ──▶ ESCRITO ──▶ VERIFICADO ──▶ DESALOJADO
                       │              │
                       └──────▶ ERROR ◀┘

Cada transición comprueba el estado de partida en el propio UPDATE
(`WHERE estado IN (...)`). Si otra ejecución ha movido la partición por
debajo, el UPDATE no toca ninguna fila y se lanza TransicionInvalida en
vez de pisar el estado en silencio.

Cada transición hace su propio COMMIT: el estado tiene que sobrevivir a
que el proceso muera justo después, que es precisamente lo que permite
retomar.
"""

from __future__ import annotations

from datetime import date

import psycopg2
import psycopg2.extras

from common.config import PG

PENDIENTE = "PENDIENTE"
ESCRIBIENDO = "ESCRIBIENDO"
ESCRITO = "ESCRITO"
VERIFICADO = "VERIFICADO"
DESALOJADO = "DESALOJADO"
ERROR = "ERROR"


class TransicionInvalida(RuntimeError):
    """La partición no estaba en el estado del que se quería salir."""


def conectar(dsn: str | None = None):
    """Conexión con RealDictCursor y la sesión en UTC.

    Las fronteras de las particiones diarias se calculan a medianoche UTC.
    Fijar la zona aquí evita que un cliente con otra TZ local lea un día
    desplazado unas horas y la verificación no cuadre nunca.
    """
    conn = psycopg2.connect(dsn or PG.dsn,
                            cursor_factory=psycopg2.extras.RealDictCursor)
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    conn.commit()
    return conn


def politica(conn, accion: str, dataset: str = "trips") -> dict | None:
    """Política activa para una acción ('ARCHIVE' | 'DELETE'), con su umbral
    ya convertido a INTERVAL por Postgres."""
    with conn.cursor() as cur:
        cur.execute("""SELECT *, umbral_intervalo(dataset, accion) AS intervalo
                         FROM retention_policy
                        WHERE dataset = %s AND accion = %s AND activa""",
                    (dataset, accion))
        fila = cur.fetchone()
    conn.commit()
    return fila


def obtener(conn, dia: date) -> dict | None:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM archival_jobs WHERE particion = %s", (dia,))
        fila = cur.fetchone()
    conn.commit()
    return fila


def _transicion(conn, dia: date, desde: tuple[str, ...], sql_set: str,
                params: tuple = ()) -> None:
    with conn.cursor() as cur:
        cur.execute(f"""UPDATE archival_jobs SET {sql_set}
                         WHERE particion = %s AND estado = ANY(%s)""",
                    params + (dia, list(desde)))
        if cur.rowcount != 1:
            conn.rollback()
            actual = obtener(conn, dia)
            raise TransicionInvalida(
                f"{dia}: se esperaba estado en {desde} y está en "
                f"{actual['estado'] if actual else 'SIN FILA'}")
    conn.commit()


# ─────────────────────────────────────────────────────────────
# Transiciones
# ─────────────────────────────────────────────────────────────
def marcar_escribiendo(conn, dia: date, filas_origen: int) -> None:
    """Entrada a la máquina: crea la fila si no existe y suma un intento.

    Se admite desde ESCRIBIENDO (el job murió escribiendo) y desde ERROR
    (reintento): la escritura en Iceberg es un overwrite del día, así que
    repetirla no duplica.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO archival_jobs (particion, estado, filas_origen,
                                       intentos, iniciado_en)
            VALUES (%s, 'ESCRIBIENDO', %s, 1, NOW())
            ON CONFLICT (particion) DO UPDATE
               SET estado         = 'ESCRIBIENDO',
                   filas_origen   = EXCLUDED.filas_origen,
                   filas_escritas = NULL,
                   bytes_escritos = NULL,
                   snapshot_id    = NULL,
                   error          = NULL,
                   intentos       = archival_jobs.intentos + 1,
                   iniciado_en    = NOW(),
                   terminado_en   = NULL
             WHERE archival_jobs.estado IN ('PENDIENTE','ESCRIBIENDO','ERROR')
        """, (dia, filas_origen))
        if cur.rowcount != 1:
            conn.rollback()
            actual = obtener(conn, dia)
            raise TransicionInvalida(
                f"{dia}: no se puede volver a ESCRIBIENDO desde {actual['estado']}")
    conn.commit()


def marcar_escrito(conn, dia: date, filas_escritas: int, snapshot_id: int | None,
                   bytes_escritos: int | None) -> None:
    _transicion(conn, dia, (ESCRIBIENDO,),
                "estado='ESCRITO', filas_escritas=%s, snapshot_id=%s, bytes_escritos=%s",
                (filas_escritas, snapshot_id, bytes_escritos))


def marcar_verificado(conn, dia: date) -> None:
    _transicion(conn, dia, (ESCRITO,), "estado='VERIFICADO'")


def marcar_error(conn, dia: date, mensaje: str) -> None:
    """Cualquier estado previo al desalojo puede acabar en ERROR.

    Desde VERIFICADO no: si el DROP falla, la partición sigue verificada y
    basta con reintentar el desalojo.
    """
    conn.rollback()
    _transicion(conn, dia, (PENDIENTE, ESCRIBIENDO, ESCRITO),
                "estado='ERROR', error=%s, terminado_en=NOW()", (mensaje[:2000],))


def desalojar(conn, dia: date) -> str:
    """Delegado en desalojar_particion(), que es quien hace cumplir la regla
    de oro en la propia base de datos: sin VERIFICADO, no hay DROP."""
    with conn.cursor() as cur:
        cur.execute("SELECT desalojar_particion(%s) AS r", (dia,))
        r = cur.fetchone()["r"]
    conn.commit()
    return r
