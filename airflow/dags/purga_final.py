"""
PIDS Parte 2 — DAG purga_final (P4, T4.3).

Envuelve la purga del tier frío de P1 (archivado/purga.py, T1.2), que
cierra el ciclo de vida: borra de Iceberg las particiones mensuales que
superan la custodia de la política ('trips', 'cold', 'DELETE') y ejecuta
expire_snapshots, que es lo que hace que los ficheros dejen de ocupar en
MinIO. El umbral se lee de retention_policy en cada ejecución.

Es diario: la custodia se mide en años y no hay prisa. Para la demo se
lanza a mano desde Airflow después de bajar el umbral.

CONTRATO CON P1: se ejecuta como `python -m archivado.purga` y termina
con código distinto de 0 si algo falla. Mientras el fichero no exista, la
tarea sale con 99 y Airflow la marca como skipped.
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
    dag_id="purga_final",
    description="Borrado definitivo del frío pasada la custodia + expire_snapshots (job de P1)",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["pids", "e8", "ciclo-de-vida"],
)
def purga_final():

    purga = BashOperator(
        task_id="purga",
        bash_command=(
            f"test -f {RAIZ_PIDS}/archivado/purga.py "
            f"|| {{ echo 'archivado/purga.py aún no existe (T1.2)'; exit 99; }}; "
            f"cd {RAIZ_PIDS} && {PYTHON_PIDS} -m archivado.purga"
        ),
        execution_timeout=timedelta(hours=1),
    )

    refrescar = TriggerDagRunOperator(
        task_id="refrescar_estadisticas",
        trigger_dag_id="estadisticas_frio",
    )

    purga >> refrescar


purga_final()
