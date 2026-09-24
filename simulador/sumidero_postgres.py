"""
PIDS Parte 2 — Sumidero PostgreSQL del simulador (P2, T2.1).

Escritura directa al tier caliente, sin Kafka. Es el camino rápido para
llenar el sistema con millones de filas realistas y desbloquear las
métricas de compresión y latencia de todo el grupo.

No valida ni inserta por su cuenta: llama a common.mensajes.escribir(),
la misma función que usa el consumidor de Kafka. Así un viaje recibe el
mismo trato (esquema, contrato de datos, cuarentena) venga por donde venga.
"""

from __future__ import annotations

import logging

import psycopg2

from common import mensajes
from common.config import PG

log = logging.getLogger("simulador.postgres")


class Sumidero:
    def __init__(self):
        self.conn = psycopg2.connect(**PG.kwargs)
        mensajes.asegurar_particiones(self.conn)
        self.total = mensajes.ResultadoLote()
        log.info("Sumidero PostgreSQL conectado a %s:%s/%s", PG.host, PG.port, PG.db)

    def enviar(self, lote: list[dict]) -> None:
        try:
            self.total.sumar(mensajes.escribir(self.conn, lote))
        except Exception:
            self.conn.rollback()
            raise

    def cerrar(self) -> None:
        t = self.total
        log.info("PostgreSQL: %d recibidos → %d en taxi_trips, %d en cuarentena, "
                 "%d duplicados", t.recibidos, t.insertados, t.cuarentena, t.duplicados)
        if t.por_motivo:
            log.info("  motivos de cuarentena: %s",
                     ", ".join(f"{k}={v}" for k, v in sorted(t.por_motivo.items())))
        self.conn.close()
