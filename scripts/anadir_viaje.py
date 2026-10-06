#!/usr/bin/env python3
"""
PIDS Parte 2 — Añadir un viaje a mano.

Construye UN viaje con los datos que se le pasan y lo mete en el sistema
por el mismo camino que el flujo en vivo: common.mensajes.escribir(), que
es la función que usan el consumidor de Kafka y el simulador. Así el viaje
pasa por el esquema y el contrato de datos, y acaba en taxi_trips o en
trips_cuarentena con sus motivos, igual que cualquier otro.

El viaje "acaba de terminar": event_time es ahora y la bajada también, así
que siempre cae en el tier caliente. No se pueden meter viajes con
event_time antiguo: esos días ya están en Iceberg y romperían la mudanza.

Uso:
    python scripts/anadir_viaje.py                           # un viaje normal
    python scripts/anadir_viaje.py --distancia 12 --minutos 35 --importe 48.5 --propina 9
    python scripts/anadir_viaje.py --distancia 600           # → cuarentena (distancia_excesiva)
    python scripts/anadir_viaje.py --importe -20             # → caliente con aviso importe_negativo

Las reglas de common/validacion.py son de dos tipos: RECHAZO (el viaje va a
cuarentena) y AVISO (se acepta, marcado en la columna avisos).
    python scripts/anadir_viaje.py --json | docker compose exec -T kafka \\
        /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic trips.raw
"""

from __future__ import annotations

import sys
import json
import argparse
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import mensajes              # noqa: E402
from common.config import PG             # noqa: E402


def construir(a: argparse.Namespace) -> dict:
    return mensajes.viaje_manual(
        distancia=a.distancia, minutos=a.minutos, importe=a.importe,
        propina=a.propina, pasajeros=a.pasajeros, pago=a.pago,
        zona_origen=a.zona_origen, zona_destino=a.zona_destino,
    )


def main() -> int:
    p = argparse.ArgumentParser(description="Añade un viaje a mano al tier caliente")
    p.add_argument("--distancia", type=float, default=2.5, help="millas (defecto 2.5)")
    p.add_argument("--minutos", type=float, default=12, help="duración (defecto 12)")
    p.add_argument("--importe", type=float, default=14.0, help="fare_amount en $ (defecto 14)")
    p.add_argument("--propina", type=float, default=3.0, help="tip_amount en $ (defecto 3)")
    p.add_argument("--pasajeros", type=int, default=1)
    p.add_argument("--pago", type=int, default=1, help="1 tarjeta, 2 efectivo…")
    p.add_argument("--zona-origen", type=int, default=161, help="pu_location_id (161 = Midtown)")
    p.add_argument("--zona-destino", type=int, default=236, help="do_location_id (236 = Upper East Side)")
    p.add_argument("--json", action="store_true",
                   help="No escribe nada: saca el mensaje en una línea, para mandarlo por Kafka")
    a = p.parse_args()

    msg = construir(a)
    if a.json:
        print(json.dumps(msg, ensure_ascii=False))
        return 0

    conn = psycopg2.connect(**PG.kwargs)
    try:
        mensajes.asegurar_particiones(conn)
        res = mensajes.escribir(conn, [msg], origen="manual")
        with conn.cursor() as cur:
            cur.execute("SELECT avisos FROM taxi_trips WHERE trip_id = %s",
                        (msg["trip_id"],))
            fila = cur.fetchone()
    finally:
        conn.close()

    if res.insertados:
        print(f"Viaje {msg['trip_id']} → CALIENTE (taxi_trips)")
        if fila and fila[0]:
            print(f"  avisos (aceptado, pero marcado): {', '.join(fila[0])}")
        print(f"  event_time {msg['event_time']}")
        print(f"  {a.distancia} millas en {a.minutos:g} min, total {msg['total_amount']} $")
        print("  Verlo: pg \"SELECT * FROM taxi_trips WHERE trip_id = "
              f"'{msg['trip_id']}';\"")
    else:
        print(f"Viaje {msg['trip_id']} → CUARENTENA")
        print(f"  motivos: {', '.join(sorted(res.por_motivo)) or '?'}")
        print("  Verlo: pg \"SELECT motivos, payload FROM trips_cuarentena "
              "ORDER BY id DESC LIMIT 1;\"")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
