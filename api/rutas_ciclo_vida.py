"""
PIDS Parte 2 — API · Rutas de /lifecycle (P3, T3.3).

/lifecycle/status  — la máquina de estados de §3.2 vista desde fuera.
/lifecycle/policy  — GET y PUT sobre retention_policy.

El PUT es el momento clave de la demo: baja el umbral a
5 minutos en directo y el archivado se dispara sin redesplegar nada. Por
eso la respuesta del PUT incluye cuántas particiones pasan a ser
candidatas: se ve el efecto en la misma pantalla, sin cambiar de ventana.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg2.extensions import connection as PGConnection

from api.dependencias import DATASET, dict_cursor, get_conn
from api.instrumentacion import ahora_utc, cronometrar, registrar_consulta
from api.modelos import PoliticaUpdate

router = APIRouter(prefix="/lifecycle", tags=["lifecycle"])


def _serializable(valor: Any) -> Any:
    if isinstance(valor, (datetime, date)):
        return valor.isoformat()
    if isinstance(valor, Decimal):
        return float(valor)
    return valor


def _filas(cur) -> list[dict[str, Any]]:
    return [{k: _serializable(v) for k, v in dict(f).items()} for f in cur.fetchall()]


@router.get("/status")
def status(conn: PGConnection = Depends(get_conn)) -> dict:
    """Estado del ciclo de vida: los jobs de archivado y el cumplimiento.

    `jobs` es la máquina de estados de §3.2 partición a partición
    (PENDIENTE → ESCRIBIENDO → ESCRITO → VERIFICADO → DESALOJADO), que es
    lo que se enseña mientras corre el DAG de P4.

    `cumplimiento` responde a la pregunta de E8 "¿va el archivado al
    día?": compara la edad del registro más antiguo del caliente con el
    umbral vigente.
    """
    with cronometrar() as crono:
        with dict_cursor(conn) as cur:
            cur.execute(
                """
                SELECT particion, estado, filas_origen, filas_escritas,
                       bytes_escritos, snapshot_id, intentos, error,
                       iniciado_en, terminado_en, actualizado_en
                FROM archival_jobs
                ORDER BY particion DESC
                LIMIT 200
                """
            )
            jobs = _filas(cur)

            cur.execute("SELECT * FROM v_cumplimiento_politica")
            cumplimiento = _filas(cur)

            cur.execute(
                """
                SELECT estado, count(*) AS particiones
                FROM archival_jobs
                GROUP BY estado
                ORDER BY estado
                """
            )
            resumen = _filas(cur)

            # Candidatas según la política vigente: lo que el DAG de P4
            # se llevaría en su próxima ejecución.
            cur.execute("SELECT * FROM particiones_a_archivar()")
            candidatas = _filas(cur)

        latencia_ms = crono.transcurrido_ms

        registrar_consulta(
            conn,
            endpoint="/lifecycle/status",
            data_source="hot",
            rango_desde=None,
            rango_hasta=None,
            filas=len(jobs),
            latencia_ms=latencia_ms,
        )

    return {
        "data": {
            "jobs": jobs,
            "resumen_por_estado": resumen,
            "candidatas_a_archivar": candidatas,
            "cumplimiento": cumplimiento[0] if cumplimiento else None,
        },
        "meta": {
            "data_source": "hot",
            "coverage": [],
            "as_of": ahora_utc().isoformat(),
            "latency_ms": latencia_ms,
            "rows": len(jobs),
        },
    }


@router.get("/policy")
def policy_get(conn: PGConnection = Depends(get_conn)) -> dict:
    """La política de retención vigente. Es un dato, no código (§3.3)."""
    with cronometrar() as crono:
        with dict_cursor(conn) as cur:
            cur.execute(
                """
                SELECT id, dataset, tier_origen, tier_destino,
                       umbral_valor, umbral_unidad, accion, activa,
                       descripcion, actualizado_en
                FROM retention_policy
                ORDER BY id
                """
            )
            politicas = _filas(cur)

        latencia_ms = crono.transcurrido_ms

        registrar_consulta(
            conn,
            endpoint="/lifecycle/policy",
            data_source="hot",
            rango_desde=None,
            rango_hasta=None,
            filas=len(politicas),
            latencia_ms=latencia_ms,
        )

    return {
        "data": politicas,
        "meta": {
            "data_source": "hot",
            "coverage": [],
            "as_of": ahora_utc().isoformat(),
            "latency_ms": latencia_ms,
            "rows": len(politicas),
        },
    }


@router.put("/policy")
def policy_put(
    cambio: PoliticaUpdate,
    accion: str = Query("ARCHIVE", pattern="^(ARCHIVE|DELETE)$"),
    dataset: str = Query(DATASET),
    conn: PGConnection = Depends(get_conn),
) -> dict:
    """Cambia el umbral de una política. El momento clave de la demo.

    La unidad y el valor los valida ya el modelo (api/modelos.py), que
    refleja los CHECK de la tabla: umbral_valor > 0 y umbral_unidad en
    ('minutes','hours','days','years'). Así el 422 de FastAPI explica qué
    está mal en vez de dejar que reviente la base con un error de
    restricción.
    """
    with cronometrar() as crono:
        with dict_cursor(conn) as cur:
            cur.execute(
                """
                UPDATE retention_policy
                   SET umbral_valor = %s,
                       umbral_unidad = %s,
                       actualizado_en = NOW()
                 WHERE dataset = %s AND accion = %s AND activa
                RETURNING id, dataset, tier_origen, tier_destino,
                          umbral_valor, umbral_unidad, accion, activa,
                          descripcion, actualizado_en
                """,
                (cambio.umbral_valor, cambio.umbral_unidad, dataset, accion),
            )
            actualizada = cur.fetchone()

            if actualizada is None:
                conn.rollback()
                raise HTTPException(
                    404,
                    f"No hay política activa para dataset='{dataset}' y "
                    f"accion='{accion}'.",
                )

            # Efecto inmediato de la política recién guardada: lo que
            # empieza a ser candidato acto seguido. Es el criterio de
            # "hecho cuando" de T3.3, y en la demo evita tener que
            # cambiar de ventana para demostrar que ha surtido efecto.
            cur.execute("SELECT count(*) AS candidatas FROM particiones_a_archivar()")
            candidatas = cur.fetchone()["candidatas"]

        conn.commit()
        latencia_ms = crono.transcurrido_ms

        registrar_consulta(
            conn,
            endpoint="/lifecycle/policy",
            data_source="hot",
            rango_desde=None,
            rango_hasta=None,
            filas=1,
            latencia_ms=latencia_ms,
            parametros={
                "metodo": "PUT",
                "dataset": dataset,
                "accion": accion,
                "umbral_valor": cambio.umbral_valor,
                "umbral_unidad": cambio.umbral_unidad,
            },
        )

    return {
        "data": {
            "politica": {k: _serializable(v) for k, v in dict(actualizada).items()},
            "particiones_candidatas_ahora": candidatas,
        },
        "meta": {
            "data_source": "hot",
            "coverage": [],
            "as_of": ahora_utc().isoformat(),
            "latency_ms": latencia_ms,
            "rows": 1,
        },
    }
