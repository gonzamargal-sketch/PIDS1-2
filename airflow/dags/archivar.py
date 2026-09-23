"""
PIDS Parte 2 — DAG archivar (P4, T4.3).

Envuelve el job de archivado de P1 (archivado/job_archivado.py, T1.1).
Es fino a propósito: la máquina de estados, la escritura en Iceberg, la
verificación y el desalojo viven en archivado/. El DAG solo lo programa,
le pone reintentos y lo hace visible.

La política se lee de retention_policy en cada ejecución, así que bajar
el umbral con PUT /lifecycle/policy basta para que la siguiente pasada
mueva particiones: sin redesplegar ni tocar este fichero. Corre cada
cinco minutos para que en el vídeo el cambio se vea enseguida. Si no hay
candidatas, el job no hace nada; y como es idempotente, un reintento a
mitad de mudanza retoma desde el estado guardado en archival_jobs.

CONTRATO CON P1: el job se ejecuta como `python -m archivado.job_archivado`
y termina con código distinto de 0 si algo falla. Mientras el fichero no
exista, la tarea sale con 99 y Airflow la marca como skipped, no en rojo.

Al terminar lanza estadisticas_frio para que Grafana vea la mudanza sin
esperar a su siguiente pasada.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag

PYTHON_PIDS = os.getenv("PIDS_PYTHON", "/opt/pids-venv/bin/python")
RAIZ_PIDS = os.getenv("PIDS_RAIZ", "/opt/pids")


@dag(
    dag_id="archivar",
    description="Mueve las particiones caducadas de PostgreSQL a Iceberg (job de P1)",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    # Dos pasadas a la vez se pisarían la máquina de estados
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=1)},
    tags=["pids", "e8", "ciclo-de-vida"],
)
def archivar():

    job = BashOperator(
        task_id="job_archivado",
        bash_command=(
            f"test -f {RAIZ_PIDS}/archivado/job_archivado.py "
            f"|| {{ echo 'archivado/job_archivado.py aún no existe (T1.1)'; exit 99; }}; "
            f"cd {RAIZ_PIDS} && {PYTHON_PIDS} -m archivado.job_archivado"
        ),
        # Una partición grande tarda, pero nunca tanto
        execution_timeout=timedelta(minutes=30),
    )

    refrescar = TriggerDagRunOperator(
        task_id="refrescar_estadisticas",
        trigger_dag_id="estadisticas_frio",
    )

    job >> refrescar


archivar()
