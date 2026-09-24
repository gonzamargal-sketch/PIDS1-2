"""
PIDS Parte 2 — Sumidero Kafka del simulador (P2, T2.3).

Publica cada viaje en trips.raw como JSON, con las dos columnas de tiempo
desde el primer mensaje (§3.1): event_time "ahora" y la fecha de negocio
en 2020. Lo recoge ingesta/consumidor_kafka.py.

Implementa la interfaz de sumidero que fija simulador.py; no lo toca.

Cada lote se vacía (flush) antes de devolver el control, y un mensaje que
Kafka no confirma hace fallar el envío: el recuento de emitidos del
simulador es, por tanto, lo que de verdad llegó al broker. Así se puede
cuadrar con lo que acaba en PostgreSQL.
"""

from __future__ import annotations

import json
import logging

from confluent_kafka import Producer

from common.config import KAFKA

log = logging.getLogger("simulador.kafka")


class Sumidero:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": KAFKA.bootstrap,
            # Sin duplicados por reintento del productor y confirmación del broker
            "enable.idempotence": True,
            "acks": "all",
            # Agrupa lo que llega en 20 ms: el lote entero sale en pocas peticiones
            "linger.ms": 20,
            "compression.type": "zstd",
        })
        self.enviados = 0
        self.fallos: list[str] = []
        log.info("Sumidero Kafka → %s (topic %s)", KAFKA.bootstrap, KAFKA.topic_trips)

    def _entregado(self, err, _msg) -> None:
        if err is not None:
            self.fallos.append(str(err))

    def enviar(self, lote: list[dict]) -> None:
        for msg in lote:
            valor = json.dumps(msg, ensure_ascii=False).encode("utf-8")
            clave = str(msg.get("trip_id", "")).encode("utf-8")
            while True:
                try:
                    self.producer.produce(KAFKA.topic_trips, value=valor, key=clave,
                                          on_delivery=self._entregado)
                    break
                except BufferError:
                    # Cola local llena: dejar que salga algo y reintentar
                    self.producer.poll(0.5)
            self.producer.poll(0)

        pendientes = self.producer.flush(30)
        if pendientes or self.fallos:
            raise RuntimeError(f"Kafka no confirmó {pendientes} mensajes; "
                               f"errores: {self.fallos[:3]}")
        self.enviados += len(lote)

    def cerrar(self) -> None:
        self.producer.flush(30)
        log.info("Kafka: %d mensajes confirmados en %s", self.enviados, KAFKA.topic_trips)
