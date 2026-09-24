"""
PIDS Parte 2 — DAG estadisticas_frio (P4, T4.2).

Grafana no sabe leer Iceberg. Este DAG lee los manifiestos de la tabla
del tier frío (sin tocar los datos, así que es barato) y deja las cifras
en cold_stats, que Grafana pinta como cualquier otra tabla:

    una fila con particion_mes = NULL   total de la tabla
    una fila por partición mensual      filas, bytes y ficheros de ese mes

Sin esto, v_coste_por_tier solo devuelve la mitad caliente, y esa vista
es la métrica estrella de E8.

En la misma ejecución hace una foto del caliente en hot_stats
(postgres/init/07_p4.sql). Así las dos series de "filas por tier en el
tiempo" se miden a la vez y el área apilada de Grafana cuadra. Antes de
la foto pasa un ANALYZE: las cifras del caliente salen de reltuples, que
solo se actualiza al analizar, y sin él v_coste_por_tier y
v_cumplimiento_politica irían con retraso.

Corre cada cinco minutos para que en el vídeo se vea el frío subir casi
en directo; el DAG archivar lo lanza además al terminar cada mudanza.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task

PYTHON_PIDS = os.getenv("PIDS_PYTHON", "/opt/pids-venv/bin/python")


@dag(
    dag_id="estadisticas_frio",
    description="Vuelca filas, bytes y ficheros de Iceberg a cold_stats (y foto del caliente)",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["pids", "e8", "frio", "metricas"],
)
def estadisticas_frio():

    @task.external_python(python=PYTHON_PIDS, expect_airflow=False)
    def volcar_frio() -> dict:
        from datetime import date

        import psycopg2
        import psycopg2.extras
        from pyiceberg.exceptions import NoSuchTableError

        from common import lakehouse
        from common.config import PG

        try:
            tabla = lakehouse.obtener_tabla(crear=False)
        except NoSuchTableError:
            # Base recién creada, antes de la carga inicial: se deja
            # constancia de un frío vacío en vez de fallar.
            tabla = None

        total = (lakehouse.estadisticas(tabla) if tabla is not None
                 else {"filas": 0, "bytes": 0, "ficheros": 0, "snapshot_id": None})

        # Desglose mensual. inspect.partitions() también sale de los
        # manifiestos; la partición es el número de meses desde 1970
        # (MonthTransform sobre event_time).
        por_mes = []
        if tabla is not None and total["snapshot_id"] is not None:
            for p in tabla.inspect.partitions().to_pylist():
                meses = (p["partition"] or {}).get("event_month")
                if meses is None:
                    continue
                por_mes.append((
                    date(1970 + meses // 12, meses % 12 + 1, 1),
                    p["record_count"],
                    p["total_data_file_size_in_bytes"],
                    p["file_count"],
                    total["snapshot_id"],
                ))

        # Todo en una transacción: el total y los meses comparten medido_en
        # (NOW() es la hora de inicio de la transacción).
        conn = psycopg2.connect(**PG.kwargs)
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO cold_stats (particion_mes, filas, bytes, ficheros, snapshot_id)
                       VALUES (NULL, %s, %s, %s, %s)""",
                    (total["filas"], total["bytes"], total["ficheros"], total["snapshot_id"]),
                )
                if por_mes:
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO cold_stats (particion_mes, filas, bytes, ficheros, snapshot_id)
                           VALUES %s""",
                        por_mes,
                    )
        finally:
            conn.close()

        print(f"frío: {total['filas']} filas, {total['bytes']} B, "
              f"{total['ficheros']} ficheros, {len(por_mes)} meses")
        return {"filas": total["filas"], "bytes": total["bytes"], "meses": len(por_mes)}

    @task.external_python(python=PYTHON_PIDS, expect_airflow=False)
    def foto_caliente() -> dict:
        import psycopg2
        from common.config import PG

        conn = psycopg2.connect(**PG.kwargs)
        try:
            with conn, conn.cursor() as cur:
                cur.execute("ANALYZE taxi_trips")
            with conn, conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO hot_stats (particiones, filas, bytes, bytes_indices)
                       SELECT particiones, filas, bytes, bytes_indices
                       FROM v_metricas_caliente
                       RETURNING particiones, filas, bytes"""
                )
                particiones, filas, bytes_ = cur.fetchone()
        finally:
            conn.close()

        print(f"caliente: {filas} filas, {bytes_} B, {particiones} particiones")
        return {"filas": filas, "bytes": bytes_, "particiones": particiones}

    [volcar_frio(), foto_caliente()]


estadisticas_frio()
