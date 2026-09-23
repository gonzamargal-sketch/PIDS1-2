"""
PIDS Parte 2 — DAG mantener_particiones (P4, T4.1).

Crea por adelantado las particiones diarias de taxi_trips llamando a
crear_particiones_adelanto(N), que ya existe en PostgreSQL y es
idempotente: relanzarlo no rompe nada.

Si nadie crea las particiones de mañana, las filas nuevas caen en
taxi_trips_default. No se pierden, pero esa partición no tiene día y el
archivado no la ve nunca. Por eso el DAG comprueba al final que siga
vacía y falla si no: es la alarma de que algo va atrasado.

Como todos los DAGs del proyecto, el código corre en /opt/pids-venv (ver
airflow/Dockerfile), que es donde están psycopg2 y common/. La función
de la tarea se ejecuta en otro intérprete, así que tiene que ser
autocontenida: los imports van dentro.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task

PYTHON_PIDS = os.getenv("PIDS_PYTHON", "/opt/pids-venv/bin/python")
DIAS_ADELANTO = int(os.getenv("PARTICIONES_ADELANTO_DIAS", "7"))


@dag(
    dag_id="mantener_particiones",
    description="Crea las particiones diarias de taxi_trips de los próximos días",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["pids", "e8", "caliente"],
)
def mantener_particiones():

    @task.external_python(python=PYTHON_PIDS, expect_airflow=False)
    def crear_particiones(dias: int) -> list[str]:
        import psycopg2
        from common.config import PG

        with psycopg2.connect(**PG.kwargs) as conn, conn.cursor() as cur:
            cur.execute("SELECT resultado FROM crear_particiones_adelanto(%s)", (dias,))
            resultados = [fila[0] for fila in cur.fetchall()]
        conn.close()

        for r in resultados:
            print(r)
        return resultados

    @task.external_python(python=PYTHON_PIDS, expect_airflow=False)
    def comprobar_default() -> int:
        import psycopg2
        from common.config import PG

        with psycopg2.connect(**PG.kwargs) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM taxi_trips_default")
            n = cur.fetchone()[0]
        conn.close()

        if n:
            raise RuntimeError(
                f"taxi_trips_default tiene {n} filas: hay event_time sin "
                "partición propia y el archivado no las verá nunca."
            )
        print("taxi_trips_default vacía")
        return n

    crear_particiones(DIAS_ADELANTO) >> comprobar_default()


mantener_particiones()
