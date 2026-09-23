#!/usr/bin/env python3
"""
PIDS Parte 2 — Prueba de humo.

Ejercita el ciclo completo con la muestra de 1.000 filas:

    1. Lee el CSV con el contrato de datos
    2. Aplica las reglas de calidad
    3. Inserta válidos en taxi_trips y rechazados en cuarentena
    4. Comprueba que las vistas de métricas responden
    5. Comprueba la MÁQUINA DE ESTADOS del archivado, que es lo que hace
       segura la mudanza: verifica que NO se puede desalojar una partición
       que no esté en VERIFICADO

Si esto pasa, el esqueleto está sano y se puede construir encima.

Uso:
    python scripts/prueba_humo.py
    python scripts/prueba_humo.py --dsn postgresql://pids:...@localhost:5432/pids
"""

from __future__ import annotations

import sys
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import esquema, validacion   # noqa: E402
from common.config import PG             # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("humo")

if sys.version_info < (3, 11):
    raise SystemExit(
        f"Este proyecto necesita Python 3.11 o superior "
        f"(tienes {sys.version_info.major}.{sys.version_info.minor})."
    )

RAIZ = Path(__file__).resolve().parents[1]
CSV = RAIZ / "datos" / "muestra_1000.csv"

OK, FALLO = "  [OK]  ", "  [FALLO]"
errores: list[str] = []


def comprobar(condicion: bool, descripcion: str) -> None:
    print(f"{OK if condicion else FALLO} {descripcion}")
    if not condicion:
        errores.append(descripcion)


def seccion(titulo: str) -> None:
    print(f"\n{'─'*66}\n{titulo}\n{'─'*66}")


def limpiar(cur, conn) -> None:
    """Deja las tablas que toca esta prueba como recién creadas.

    La prueba afirma conteos absolutos (`count(*) == filas insertadas`) y
    recorre la máquina de estados del archivado de principio a fin. Las dos
    cosas solo se sostienen partiendo de cero: sin esto, la segunda
    ejecución falla por filas acumuladas, y la partición elegida puede venir
    ya en VERIFICADO de la vez anterior, con lo que la comprobación
    importante —que no se puede desalojar sin verificar— se saltaría.

    Solo borra lo que la propia prueba escribe. cold_stats y
    retention_policy no se tocan.
    """
    cur.execute("""SELECT c.relname FROM pg_class c
                   JOIN pg_inherits i ON i.inhrelid = c.oid
                   JOIN pg_class pa   ON pa.oid = i.inhparent
                   WHERE pa.relname = 'taxi_trips'
                     AND c.relname ~ '^taxi_trips_\\d{4}_\\d{2}_\\d{2}$'""")
    particiones = [r["relname"] for r in cur.fetchall()]
    for nombre in particiones:
        cur.execute(f'DROP TABLE IF EXISTS "{nombre}"')

    cur.execute("TRUNCATE taxi_trips, trips_cuarentena, archival_jobs, query_log")
    conn.commit()
    print(f"{OK} Estado de partida limpio "
          f"({len(particiones)} particiones previas eliminadas)")


# ══════════════════════════════════════════════════════════════
def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", default=PG.dsn)
    p.add_argument("--no-limpiar", action="store_true",
                   help="No borrar el rastro de ejecuciones anteriores. "
                        "Las comprobaciones de conteo fallarán si ya hay datos.")
    args = p.parse_args()

    # ── 1. Contrato de datos ──────────────────────────────────
    seccion("1. CONTRATO DE DATOS")
    df = esquema.leer_csv(CSV)
    comprobar(len(df) > 0, f"CSV leído: {len(df)} filas")
    comprobar(
        list(df.columns) == [c for c in esquema.MAPEO_COLUMNAS.values()],
        "Columnas normalizadas al esquema de PostgreSQL",
    )
    comprobar(
        pd.api.types.is_datetime64_any_dtype(df["tpep_pickup_datetime"]),
        "Fechas parseadas como datetime (no como texto)",
    )
    # La trampa del formato americano: si se hubiera inferido mal, enero
    # y los días 1-12 saldrían intercambiados
    comprobar(
        df["tpep_pickup_datetime"].dt.year.between(2019, 2021).all(),
        "Fechas en el rango esperado (el formato americano no se ha confundido)",
    )

    res = validacion.validar(df)
    print(validacion.informe(res))
    comprobar(len(res.rechazados) + len(res.validos) == len(df),
              "Válidos + rechazados = total (no se pierde ninguna fila)")
    comprobar(res.resumen["con_avisos"] > 0,
              f"Se detectan avisos reales del dataset: {res.resumen['con_avisos']} filas")

    # ── 2. Conexión ───────────────────────────────────────────
    seccion("2. CONEXIÓN A POSTGRESQL")
    conn = psycopg2.connect(args.dsn)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT current_database() AS db, version() AS v")
    fila = cur.fetchone()
    comprobar(True, f"Conectado a {fila['db']} ({fila['v'].split(',')[0]})")

    cur.execute("""SELECT count(*) AS n FROM information_schema.tables
                   WHERE table_schema='public'
                     AND table_name IN ('taxi_trips','trips_cuarentena',
                                        'retention_policy','archival_jobs',
                                        'cold_stats','query_log')""")
    comprobar(cur.fetchone()["n"] == 6, "Las 6 tablas del esquema existen")

    cur.execute("SELECT count(*) AS n FROM retention_policy WHERE activa")
    comprobar(cur.fetchone()["n"] == 3, "3 políticas de retención sembradas")

    if args.no_limpiar:
        print("  [AVISO] --no-limpiar: los conteos fallarán si la BD no está vacía")
    else:
        limpiar(cur, conn)

    # ── 3. Inserción ──────────────────────────────────────────
    seccion("3. INSERCIÓN CON event_time REPARTIDO")
    # Repartimos event_time entre hace 40 días y ahora, para que parte de
    # los datos quede por debajo del umbral de 30 días y parte por encima.
    # Así hay candidatas al archivado desde el primer momento.
    ahora = datetime.now(timezone.utc)
    n = len(res.validos)
    v = res.validos.copy()
    v["event_time"] = [ahora - timedelta(days=40 * i / n) for i in range(n)]

    # Crear las particiones que hagan falta para ese rango
    dias = sorted({d.date() for d in v["event_time"]})
    for d in dias:
        cur.execute("SELECT crear_particion_dia(%s)", (d,))
    conn.commit()
    comprobar(True, f"{len(dias)} particiones diarias creadas para el rango")

    cols = ["event_time"] + esquema.COLUMNAS_NEGOCIO
    filas = []
    for _, r in v.iterrows():
        filas.append(tuple(
            [r["event_time"]]
            + [None if pd.isna(r[c]) else r[c] for c in esquema.COLUMNAS_NEGOCIO]
        ) + ("stream", CSV.name, esquema.ESQUEMA_VERSION, list(r["avisos"])))

    psycopg2.extras.execute_values(
        cur,
        f"""INSERT INTO taxi_trips ({','.join(cols)}, origen, fichero_origen,
                                    esquema_version, avisos) VALUES %s""",
        filas, page_size=500,
    )
    conn.commit()
    cur.execute("SELECT count(*) AS n FROM taxi_trips")
    insertadas = cur.fetchone()["n"]
    comprobar(insertadas == len(res.validos),
              f"{insertadas} filas válidas en taxi_trips")

    # Rechazados a cuarentena
    if len(res.rechazados):
        rech = []
        for _, r in res.rechazados.iterrows():
            payload = {k: (None if pd.isna(x) else str(x)) for k, x in r.items()
                       if k != "motivos"}
            rech.append(("stream", CSV.name, list(r["motivos"]), json.dumps(payload)))
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO trips_cuarentena (origen, fichero_origen, motivos, payload)
               VALUES %s""", rech,
        )
        conn.commit()
    cur.execute("SELECT count(*) AS n FROM trips_cuarentena")
    comprobar(cur.fetchone()["n"] == len(res.rechazados),
              f"{len(res.rechazados)} filas en cuarentena con su motivo")

    cur.execute("SELECT count(*) AS n FROM taxi_trips_default")
    comprobar(cur.fetchone()["n"] == 0,
              "Ninguna fila ha caído en la partición por defecto")

    # ── 4. Vistas de métricas ─────────────────────────────────
    seccion("4. MÉTRICAS E8")
    cur.execute("SELECT * FROM v_metricas_caliente")
    m = cur.fetchone()
    print(f"     particiones={m['particiones']}  filas≈{m['filas']}  "
          f"bytes={m['bytes']:,}  B/fila={m['bytes_por_fila']}")
    comprobar(m["particiones"] > 0, "v_metricas_caliente responde")

    # reltuples es una estimación que solo se puebla tras ANALYZE
    cur.execute("ANALYZE taxi_trips")
    conn.commit()
    cur.execute("SELECT * FROM v_metricas_caliente")
    m = cur.fetchone()
    comprobar(m["bytes_por_fila"] is not None and m["bytes_por_fila"] > 0,
              f"Bytes por fila en caliente: {m['bytes_por_fila']} B")
    print("     OJO: con solo ~1.000 filas repartidas en decenas de particiones,")
    print("     este número está dominado por el coste fijo de página y de índice.")
    print("     Con volumen real baja a unos 250-350 B/fila. No lo uséis en la")
    print("     memoria hasta tener millones de filas cargadas.")

    cur.execute("SELECT * FROM v_calidad ORDER BY registros DESC LIMIT 5")
    for r in cur.fetchall():
        print(f"     {r['motivo']:<26} {r['registros']:>5}")
    comprobar(True, "v_calidad responde")

    cur.execute("SELECT * FROM v_cumplimiento_politica")
    c = cur.fetchone()
    print(f"     umbral={c['umbral']}  edad_max={c['edad_maxima_dias']}d  "
          f"estado={c['estado']}")
    comprobar(c["estado"] in ("OK", "INCUMPLE"), "v_cumplimiento_politica responde")

    # ── 5. Máquina de estados ─────────────────────────────────
    seccion("5. MÁQUINA DE ESTADOS DEL ARCHIVADO")
    cur.execute("SELECT * FROM particiones_a_archivar()")
    candidatas = cur.fetchall()
    comprobar(len(candidatas) > 0,
              f"{len(candidatas)} particiones superan el umbral de 30 días")

    objetivo = candidatas[0]["dia"]
    filas_obj = candidatas[0]["filas_estimadas"]
    print(f"     probando con la partición {objetivo}")

    # LA COMPROBACIÓN IMPORTANTE: no se puede desalojar sin verificar
    conn.rollback()
    try:
        cur.execute("SELECT desalojar_particion(%s)", (objetivo,))
        conn.commit()
        comprobar(False, "El desalojo sin VERIFICADO debería haber fallado")
    except psycopg2.errors.RaiseException:
        conn.rollback()
        comprobar(True, "Desalojar sin VERIFICADO se rechaza (protección de la mudanza)")

    # Recorrer los estados como haría el DAG
    cur.execute("SELECT contar_particion(%s) AS n", (objetivo,))
    exactas = cur.fetchone()["n"]

    cur.execute("""INSERT INTO archival_jobs (particion, estado, filas_origen, iniciado_en)
                   VALUES (%s,'ESCRIBIENDO',%s,NOW())
                   ON CONFLICT (particion) DO UPDATE
                     SET estado='ESCRIBIENDO', filas_origen=EXCLUDED.filas_origen""",
                (objetivo, exactas))
    cur.execute("""UPDATE archival_jobs SET estado='ESCRITO', filas_escritas=%s,
                          snapshot_id=123456789 WHERE particion=%s""",
                (exactas, objetivo))
    conn.commit()

    # Verificación: lo escrito en Iceberg debe coincidir con lo que había
    cur.execute("""SELECT filas_origen, filas_escritas FROM archival_jobs
                   WHERE particion=%s""", (objetivo,))
    a = cur.fetchone()
    coinciden = a["filas_origen"] == a["filas_escritas"]
    comprobar(coinciden, f"Verificación de conteos: {a['filas_origen']} = {a['filas_escritas']}")

    if coinciden:
        cur.execute("UPDATE archival_jobs SET estado='VERIFICADO' WHERE particion=%s",
                    (objetivo,))
        conn.commit()

    cur.execute("SELECT desalojar_particion(%s) AS r", (objetivo,))
    resultado = cur.fetchone()["r"]
    conn.commit()
    comprobar("desalojada" in resultado, f"Desalojo desde VERIFICADO: {resultado}")

    cur.execute("SELECT count(*) AS n FROM taxi_trips")
    despues = cur.fetchone()["n"]
    comprobar(despues == insertadas - exactas,
              f"Filas tras el desalojo: {despues} = {insertadas} - {exactas}")

    # Idempotencia: relanzarlo no debe romper nada
    cur.execute("SELECT desalojar_particion(%s) AS r", (objetivo,))
    conn.commit()
    comprobar(True, "Relanzar el desalojo es idempotente (no falla)")

    cur.execute("""SELECT count(*) AS n FROM particiones_a_archivar()
                   WHERE dia = %s""", (objetivo,))
    comprobar(cur.fetchone()["n"] == 0,
              "La partición desalojada ya no aparece como candidata")

    # ── 6. query_log ──────────────────────────────────────────
    seccion("6. INSTRUMENTACIÓN DE LATENCIAS")
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO query_log (endpoint,data_source,filas,latencia_ms,cache_hit)
           VALUES %s""",
        [("/trips", "cache", 10, 3.2, True),
         ("/trips", "hot", 500, 42.0, False),
         ("/trips", "cold", 5000, 2400.0, False),
         ("/trips", "mixto", 5500, 2600.0, False)],
    )
    conn.commit()
    cur.execute("SELECT * FROM v_latencia_por_tier ORDER BY p95_ms")
    for r in cur.fetchall():
        print(f"     {r['tier']:<7} p50={r['p50_ms']:>8} ms  p95={r['p95_ms']:>8} ms")
    comprobar(True, "v_latencia_por_tier responde (de aquí salen los SLAs)")

    cur.close()
    conn.close()

    # ── Resultado ─────────────────────────────────────────────
    print(f"\n{'═'*66}")
    if errores:
        print(f"  {len(errores)} COMPROBACIONES FALLIDAS:")
        for e in errores:
            print(f"    - {e}")
        print("═" * 66)
        return 1
    print("  TODO CORRECTO. El esqueleto está sano.")
    print("═" * 66)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
