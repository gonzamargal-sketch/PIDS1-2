"""
PIDS Parte 2 — Consumidor de Kafka (P2, T2.2).

    trips.raw ─▶ lote ─▶ esquema + contrato de datos ─┬─▶ taxi_trips
                                                      └─▶ trips_cuarentena

La validación y la escritura son common.mensajes.escribir(), la misma
función que usa el sumidero PostgreSQL del simulador.

LAS DOS REGLAS QUE NO SE PUEDEN SALTAR
    1. Insertar por lotes (execute_values). Fila a fila no aguanta el
       ritmo del simulador.
    2. Confirmar el offset de Kafka DESPUÉS del commit en PostgreSQL,
       nunca antes. El autocommit de Kafka está apagado: si el proceso cae
       entre medias, al volver se relee el lote en vez de perderlo.

    Releer un lote no duplica nada: el trip_id viene en el mensaje y la
    escritura hace ON CONFLICT DO NOTHING (ver common/mensajes.py y
    postgres/init/05_p2.sql). Al menos una vez en Kafka, exactamente una
    vez en la base de datos.

Uso:
    python -m ingesta.consumidor_kafka
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from common import mensajes
from common.config import KAFKA, PG

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("consumidor")

LOG_CADA_S = 10.0


def asegurar_topic() -> None:
    """Crea trips.raw si no existe, para no depender del auto-create."""
    admin = AdminClient({"bootstrap.servers": KAFKA.bootstrap})
    futuro = admin.create_topics([NewTopic(KAFKA.topic_trips, num_partitions=1,
                                           replication_factor=1)])[KAFKA.topic_trips]
    try:
        futuro.result(timeout=15)
        log.info("Topic %s creado", KAFKA.topic_trips)
    except KafkaException as e:
        if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise


def decodificar(valor: bytes | None):
    """JSON -> dict. Si no se puede leer, se devuelve el texto tal cual y
    common.mensajes lo manda a cuarentena como mensaje_ilegible."""
    if valor is None:
        return None
    texto = valor.decode("utf-8", errors="replace")
    try:
        return json.loads(texto)
    except json.JSONDecodeError:
        return texto


def main() -> int:
    asegurar_topic()
    consumer = Consumer({
        "bootstrap.servers": KAFKA.bootstrap,
        "group.id": KAFKA.grupo_consumidor,
        "auto.offset.reset": "earliest",
        # Los offsets se confirman a mano, y solo tras el commit en PostgreSQL
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA.topic_trips])

    conn = psycopg2.connect(**PG.kwargs)
    mensajes.asegurar_particiones(conn)

    parar = False

    def _parar(signum, _frame):
        nonlocal parar
        log.info("Señal %s: cerrando tras el lote en curso", signum)
        parar = True

    signal.signal(signal.SIGTERM, _parar)
    signal.signal(signal.SIGINT, _parar)

    log.info("Consumiendo %s (grupo %s) → %s:%s/%s, lotes de %d",
             KAFKA.topic_trips, KAFKA.grupo_consumidor, PG.host, PG.port, PG.db,
             KAFKA.lote_consumidor)

    total = mensajes.ResultadoLote()
    ultimo_log = time.monotonic()
    try:
        while not parar:
            lote = consumer.consume(num_messages=KAFKA.lote_consumidor, timeout=1.0)
            valores = []
            for m in lote:
                if m.error():
                    if m.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(m.error())
                valores.append(decodificar(m.value()))

            if valores:
                try:
                    res = mensajes.escribir(conn, valores)     # commit en PostgreSQL
                except Exception:
                    conn.rollback()
                    raise
                # Solo ahora: lo que está en PostgreSQL ya no se puede perder
                consumer.commit(asynchronous=False)
                total.sumar(res)

            if time.monotonic() - ultimo_log >= LOG_CADA_S and total.recibidos:
                log.info("%d recibidos → %d en caliente, %d en cuarentena, %d duplicados",
                         total.recibidos, total.insertados, total.cuarentena, total.duplicados)
                ultimo_log = time.monotonic()
    finally:
        consumer.close()
        conn.close()
        log.info("FIN: %d recibidos → %d en caliente, %d en cuarentena, %d duplicados",
                 total.recibidos, total.insertados, total.cuarentena, total.duplicados)
    return 0


if __name__ == "__main__":
    sys.exit(main())
