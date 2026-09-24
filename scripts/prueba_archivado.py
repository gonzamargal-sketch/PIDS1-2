#!/usr/bin/env python3
"""
PIDS Parte 2 — Prueba end-to-end del ciclo de vida (T1.3).

Convierte el «hecho cuando» de T1.1 en algo que se repite sin acordarse de
los pasos. Con el job de archivado DE VERDAD (como subproceso, igual que lo
lanzará Airflow) y PyIceberg de verdad:

    1. Siembra filas en tres días viejos y en el de hoy
    2. Con la política en 30 días no hay nada que archivar
    3. Desalojar sin VERIFICADO se rechaza (la regla de oro)
    4. Baja la política a 5 minutos: los tres días viejos pasan a candidatos
    5. Mata el job justo después de escribir en Iceberg y lo relanza:
       retoma y NO duplica
    6. Mata el job después de marcar ESCRITO y lo relanza: retoma verificando
    7. Lanza el job completo: filas fuera del caliente, dentro del frío,
       conteos cuadrando a ambos lados y el día de hoy intacto
    8. Relanzarlo es idempotente: no hay candidatas y el frío no cambia
    9. Si la verificación no cuadra: ERROR y no se borra nada

Como prueba_humo.py, parte de una base limpia: BORRA las particiones
diarias, taxi_trips y archival_jobs. No la lancéis en la máquina donde
tengáis los datos del vídeo. En Iceberg solo toca sus propias filas
(fichero_origen = 'prueba_archivado') y los cinco días que usa, así que
el resto del histórico no se pierde. Al terminar restaura la política y
las particiones de adelanto.

Pausad el DAG `archivar` de Airflow mientras corre: con la política
bajada a 5 minutos, archivaría a la vez las particiones de la prueba.

Uso:
    python scripts/prueba_archivado.py
    python scripts/prueba_archivado.py --conservar   # deja los datos para mirarlos
"""

from __future__ import annotations

import sys
import logging
import argparse
import subprocess
from pathlib import Path
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import pandas as pd
from pyiceberg.expressions import EqualTo, Or

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import esquema, validacion, lakehouse   # noqa: E402
from common.config import PG                         # noqa: E402
from archivado.job_archivado import rango_dia, filtro_dia   # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(message)s")

if sys.version_info < (3, 11):
    raise SystemExit("Este proyecto necesita Python 3.11 o superior.")

RAIZ = Path(__file__).resolve().parents[1]
CSV = RAIZ / "datos" / "muestra_1000.csv"
JOB = RAIZ / "archivado" / "job_archivado.py"
MARCA = "prueba_archivado"          # fichero_origen de todo lo que siembra

OK, FALLO = "  [OK]  ", "  [FALLO]"
errores: list[str] = []


def comprobar(condicion: bool, descripcion: str) -> None:
    print(f"{OK if condicion else FALLO} {descripcion}")
    if not condicion:
        errores.append(descripcion)


def seccion(titulo: str) -> None:
    print(f"\n{'─'*66}\n{titulo}\n{'─'*66}")


def uno(cur, sql: str, params: tuple = ()):
    cur.execute(sql, params)
    fila = cur.fetchone()
    return None if fila is None else next(iter(fila.values()))


def lanzar_job(dsn: str, *extra: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(JOB), "--dsn", dsn, *extra],
                          cwd=RAIZ, capture_output=True, text=True)


def politica(cur, conn, valor: int, unidad: str) -> None:
    cur.execute("""UPDATE retention_policy SET umbral_valor=%s, umbral_unidad=%s
                    WHERE dataset='trips' AND accion='ARCHIVE'""", (valor, unidad))
    conn.commit()


def en_iceberg(tabla, dia) -> int:
    tabla.refresh()
    return lakehouse.contar_particion(tabla, *rango_dia(dia))


def job(cur, dia) -> dict:
    cur.execute("SELECT * FROM archival_jobs WHERE particion=%s", (dia,))
    return cur.fetchone() or {}


def limpiar(cur, conn, tabla, dias) -> None:
    """Base como recién creada: mismo criterio que prueba_humo.limpiar().

    En Iceberg se borran, además de las filas de pruebas anteriores, los
    días que usa la prueba: si alguien archivó antes esos días (el DAG
    archivar con la política bajada), el conteo del frío no partiría de 0.
    """
    cur.execute("""SELECT c.relname FROM pg_class c
                   JOIN pg_inherits i ON i.inhrelid = c.oid
                   JOIN pg_class pa   ON pa.oid = i.inhparent
                   WHERE pa.relname = 'taxi_trips'
                     AND c.relname ~ '^taxi_trips_\\d{4}_\\d{2}_\\d{2}$'""")
    for r in cur.fetchall():
        cur.execute(f'DROP TABLE IF EXISTS "{r["relname"]}"')
    cur.execute("TRUNCATE taxi_trips, archival_jobs")
    conn.commit()
    borrar_de_iceberg(tabla, dias)


def borrar_de_iceberg(tabla, dias=()) -> None:
    import warnings
    filtro = EqualTo("fichero_origen", MARCA)
    for d in dias:
        filtro = Or(filtro, filtro_dia(d))
    tabla.refresh()
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", "Delete operation did not match")
        tabla.delete(filtro)


def sembrar(cur, conn, validos: pd.DataFrame, reparto: dict) -> dict:
    """Inserta en cada día las filas pedidas, con event_time dentro del día."""
    cols = ["event_time"] + esquema.COLUMNAS_NEGOCIO
    sembradas, ini = {}, 0
    for dia, n in reparto.items():
        cur.execute("SELECT crear_particion_dia(%s)", (dia,))
        trozo = validos.iloc[ini:ini + n]
        ini += n
        desde, _ = rango_dia(dia)
        ahora = datetime.now(timezone.utc)
        filas = []
        for i, (_, r) in enumerate(trozo.iterrows()):
            et = desde + timedelta(seconds=(86_399 * i) // max(n, 1))
            if et > ahora:           # el día de hoy: nada en el futuro
                et = ahora - timedelta(seconds=i)
            filas.append(tuple(
                [et] + [None if pd.isna(r[c]) else r[c] for c in esquema.COLUMNAS_NEGOCIO]
            ) + ("stream", MARCA, esquema.ESQUEMA_VERSION, list(r["avisos"])))
        psycopg2.extras.execute_values(
            cur,
            f"""INSERT INTO taxi_trips ({','.join(cols)}, origen, fichero_origen,
                                        esquema_version, avisos) VALUES %s""",
            filas, page_size=500)
        sembradas[dia] = len(filas)
    conn.commit()
    return sembradas


# ══════════════════════════════════════════════════════════════
def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=PG.dsn)
    p.add_argument("--conservar", action="store_true",
                   help="No limpiar al terminar: deja las filas en el frío y la "
                        "política en 5 minutos, para mirarlo en Grafana o la API")
    args = p.parse_args()

    conn = psycopg2.connect(args.dsn, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute("SET TIME ZONE 'UTC'")
    cur.execute("""SELECT umbral_valor, umbral_unidad FROM retention_policy
                    WHERE dataset='trips' AND accion='ARCHIVE'""")
    politica_original = cur.fetchone()
    conn.commit()

    try:
        return _prueba(args, conn, cur)
    finally:
        if not args.conservar:
            politica(cur, conn, politica_original["umbral_valor"],
                     politica_original["umbral_unidad"])
        cur.execute("SELECT crear_particiones_adelanto(7)")
        conn.commit()
        conn.close()


def _prueba(args, conn, cur) -> int:
    hoy = datetime.now(timezone.utc).date()
    d_normal, d_caida1, d_caida2 = (hoy - timedelta(days=k) for k in (4, 3, 2))
    d_error = hoy - timedelta(days=5)
    viejos = [d_normal, d_caida1, d_caida2]

    # ── 1. Siembra ────────────────────────────────────────────
    seccion("1. SIEMBRA")
    tabla = lakehouse.obtener_tabla()
    limpiar(cur, conn, tabla, viejos + [d_error, hoy])
    comprobar(True, "Base limpia: particiones, taxi_trips, archival_jobs y, en "
                    "Iceberg, los días que usa la prueba")

    validos = validacion.validar(esquema.leer_csv(CSV)).validos
    reparto = {d_normal: 250, d_caida1: 200, d_caida2: 150, hoy: 100}
    sembradas = sembrar(cur, conn, validos, reparto)
    total = sum(sembradas.values())
    for d, n in sembradas.items():
        print(f"     {d}  {n} filas{'  (hoy)' if d == hoy else ''}")
    comprobar(uno(cur, "SELECT count(*) FROM taxi_trips") == total,
              f"{total} filas en el caliente")
    comprobar(uno(cur, "SELECT count(*) FROM taxi_trips_default") == 0,
              "Ninguna fila en la partición por defecto")
    frio_inicial = {d: en_iceberg(tabla, d) for d in sembradas}
    comprobar(all(v == 0 for v in frio_inicial.values()),
              "Ninguno de esos días está en el frío todavía")

    # ── 2. Política en 30 días ────────────────────────────────
    seccion("2. CON LA POLÍTICA EN 30 DÍAS NO SE MUEVE NADA")
    politica(cur, conn, 30, "days")
    cur.execute("SELECT dia FROM particiones_a_archivar()")
    candidatas = {r["dia"] for r in cur.fetchall()}
    conn.commit()
    comprobar(not candidatas & set(sembradas), "Ningún día sembrado es candidato")
    r = lanzar_job(args.dsn)
    comprobar(r.returncode == 0 and "0 partición(es) candidata(s)" in r.stderr,
              "El job arranca y no encuentra nada que hacer")

    # ── 3. Regla de oro ───────────────────────────────────────
    seccion("3. REGLA DE ORO: NO SE DESALOJA SIN VERIFICAR")
    try:
        cur.execute("SELECT desalojar_particion(%s)", (d_normal,))
        conn.commit()
        comprobar(False, "El desalojo sin VERIFICADO debería haber fallado")
    except psycopg2.errors.RaiseException:
        conn.rollback()
        comprobar(True, "desalojar_particion() sin VERIFICADO se rechaza")
    comprobar(uno(cur, "SELECT contar_particion(%s)", (d_normal,)) == sembradas[d_normal],
              "Y la partición sigue intacta")
    conn.commit()

    # ── 4. Bajar la política ──────────────────────────────────
    seccion("4. BAJAR LA POLÍTICA A 5 MINUTOS (UN UPDATE, SIN REDESPLEGAR)")
    politica(cur, conn, 5, "minutes")
    cur.execute("SELECT dia FROM particiones_a_archivar()")
    candidatas = {r["dia"] for r in cur.fetchall()}
    conn.commit()
    comprobar(set(viejos) <= candidatas, "Los tres días viejos pasan a ser candidatos")
    comprobar(hoy not in candidatas, "El día de hoy no (aún no supera el umbral)")

    # ── 5. Caída justo después de escribir en Iceberg ─────────
    seccion("5. CAÍDA ENTRE ESCRIBIR EN ICEBERG Y MARCAR ESCRITO")
    r = lanzar_job(args.dsn, "--dia", str(d_caida1), "--simular-caida", "tras-escribir")
    comprobar(r.returncode == 137, "El job muere a mitad (código 137)")
    j = job(cur, d_caida1)
    conn.commit()
    comprobar(j.get("estado") == "ESCRIBIENDO", f"Queda en {j.get('estado')}")
    comprobar(en_iceberg(tabla, d_caida1) == sembradas[d_caida1],
              "Las filas ya están en Iceberg: el caso peligroso")
    comprobar(uno(cur, "SELECT contar_particion(%s)", (d_caida1,)) == sembradas[d_caida1],
              "Y siguen en Postgres: no se ha borrado nada")
    conn.commit()

    r = lanzar_job(args.dsn, "--dia", str(d_caida1))
    j = job(cur, d_caida1)
    conn.commit()
    comprobar(r.returncode == 0 and j.get("estado") == "DESALOJADO",
              f"Al relanzar retoma y termina en {j.get('estado')}")
    n = en_iceberg(tabla, d_caida1)
    comprobar(n == sembradas[d_caida1],
              f"Sin duplicados en Iceberg: {n} = {sembradas[d_caida1]}")
    comprobar(j.get("intentos") == 2, f"intentos = {j.get('intentos')}")

    # ── 6. Caída después de marcar ESCRITO ────────────────────
    seccion("6. CAÍDA ENTRE ESCRITO Y VERIFICADO")
    r = lanzar_job(args.dsn, "--dia", str(d_caida2), "--simular-caida", "tras-escrito")
    j = job(cur, d_caida2)
    conn.commit()
    comprobar(r.returncode == 137 and j.get("estado") == "ESCRITO",
              f"El job muere con la partición en {j.get('estado')}")
    r = lanzar_job(args.dsn, "--dia", str(d_caida2))
    j = job(cur, d_caida2)
    conn.commit()
    comprobar(r.returncode == 0 and j.get("estado") == "DESALOJADO",
              f"Al relanzar verifica y desaloja: {j.get('estado')}")
    comprobar(j.get("intentos") == 1,
              "Sin reescribir: retoma desde ESCRITO (intentos = 1)")
    comprobar(en_iceberg(tabla, d_caida2) == sembradas[d_caida2],
              "Conteo exacto en Iceberg")

    # ── 7. Job completo ───────────────────────────────────────
    seccion("7. JOB COMPLETO")
    r = lanzar_job(args.dsn)
    comprobar(r.returncode == 0, "El job termina sin errores")
    print("     " + (r.stderr.strip().splitlines() or ["?"])[-1].split("] ", 1)[-1])

    cur.execute("""SELECT particion, estado, filas_origen, filas_escritas, snapshot_id
                     FROM archival_jobs WHERE particion = ANY(%s) ORDER BY 1""",
                (viejos,))
    jobs = cur.fetchall()
    conn.commit()
    for j in jobs:
        print(f"     {j['particion']}  {j['estado']:<11} origen={j['filas_origen']}"
              f"  escritas={j['filas_escritas']}  snapshot={j['snapshot_id']}")
    comprobar(len(jobs) == 3 and all(j["estado"] == "DESALOJADO" for j in jobs),
              "Los tres días viejos, DESALOJADOS")
    comprobar(all(j["filas_origen"] == j["filas_escritas"] for j in jobs),
              "filas_origen = filas_escritas en todos")
    comprobar(all(j["snapshot_id"] for j in jobs), "Todos con su snapshot de Iceberg")

    for d in viejos:
        existe = uno(cur, "SELECT to_regclass(%s) IS NOT NULL",
                     (f"taxi_trips_{d:%Y_%m_%d}",))
        n = en_iceberg(tabla, d)
        comprobar(not existe and n == sembradas[d],
                  f"{d}: fuera del caliente, {n} filas en el frío")
    conn.commit()
    caliente = uno(cur, "SELECT count(*) FROM taxi_trips")
    conn.commit()
    comprobar(caliente == sembradas[hoy],
              f"En el caliente solo queda hoy: {caliente} = {sembradas[hoy]}")
    comprobar(en_iceberg(tabla, hoy) == 0, "Y hoy no se ha copiado al frío")
    comprobar(caliente + sum(en_iceberg(tabla, d) for d in viejos) == total,
              f"Caliente + frío = {total}: no se pierde ni se duplica ninguna fila")

    # ── 8. Idempotencia ───────────────────────────────────────
    seccion("8. RELANZAR ES IDEMPOTENTE")
    tabla.refresh()
    snap_antes = tabla.current_snapshot().snapshot_id
    r = lanzar_job(args.dsn)
    tabla.refresh()
    comprobar(r.returncode == 0 and "0 partición(es) candidata(s)" in r.stderr,
              "La segunda ejecución no encuentra nada que hacer")
    comprobar(tabla.current_snapshot().snapshot_id == snap_antes,
              "Iceberg no cambia: mismo snapshot")

    # ── 9. Verificación que no cuadra ─────────────────────────
    seccion("9. SI LA VERIFICACIÓN NO CUADRA, NO SE BORRA NADA")
    extra = sembrar(cur, conn, validos.iloc[700:], {d_error: 50})
    # Se fuerza el caso: la partición figura como ESCRITA pero en Iceberg no
    # hay nada (como si alguien hubiera borrado los ficheros por debajo)
    cur.execute("""INSERT INTO archival_jobs (particion, estado, filas_origen,
                                              filas_escritas, intentos)
                   VALUES (%s, 'ESCRITO', 50, 50, 1)""", (d_error,))
    conn.commit()
    r = lanzar_job(args.dsn, "--dia", str(d_error))
    j = job(cur, d_error)
    conn.commit()
    comprobar(r.returncode == 1, "El job acaba con código 1 (Airflow lo verá rojo)")
    comprobar(j.get("estado") == "ERROR" and "Verificación fallida" in (j.get("error") or ""),
              f"Estado {j.get('estado')}: {j.get('error')}")
    comprobar(uno(cur, "SELECT contar_particion(%s)", (d_error,)) == extra[d_error],
              "La partición sigue entera en Postgres")
    conn.commit()

    r = lanzar_job(args.dsn, "--dia", str(d_error))
    j = job(cur, d_error)
    conn.commit()
    comprobar(r.returncode == 0 and j.get("estado") == "DESALOJADO"
              and en_iceberg(tabla, d_error) == extra[d_error],
              "El reintento reescribe el día desde ERROR y lo desaloja")

    # ── Limpieza ──────────────────────────────────────────────
    if not args.conservar:
        borrar_de_iceberg(tabla)
        cur.execute("DELETE FROM taxi_trips WHERE fichero_origen = %s", (MARCA,))
        cur.execute("DELETE FROM archival_jobs WHERE particion = ANY(%s)",
                    (viejos + [d_error],))
        conn.commit()
        print("\n  Limpieza: filas de la prueba fuera del caliente y del frío, "
              "política restaurada")

    print(f"\n{'═'*66}")
    if errores:
        print(f"  {len(errores)} COMPROBACIONES FALLIDAS:")
        for e in errores:
            print(f"    - {e}")
        print("═" * 66)
        return 1
    print("  TODO CORRECTO. El ciclo de vida caliente -> frío es seguro.")
    print("═" * 66)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
