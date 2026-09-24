#!/usr/bin/env python3
"""Genera viajes sintéticos de 2026 con el mismo formato que la muestra.

Parte de los viajes reales de datos/muestra_1000.csv y los reparte entre
una fecha de inicio (por defecto 2026-01-01) y AHORA, con las horas del día
siguiendo el perfil típico de los taxis de Nueva York. Nunca genera un viaje
en el futuro: el tope se mueve solo con los días, así que relanzarlo mañana
da un día más de datos. Todo en UTC, como PostgreSQL y el contrato de datos.

Cada fila sale distinta de su plantilla (distancia, duración, importes,
zonas, pasajeros), pero conservando lo que ya era raro en la muestra
(importes negativos, distancia cero, pasajeros a cero): son anomalías reales
y la validación tiene que seguir viéndolas.

Ejemplos:

    python scripts/generar_datos_sinteticos.py 1000000 --seed 42
    python scripts/generar_datos_sinteticos.py 50000 --today
    python scripts/generar_datos_sinteticos.py 10000 --desde 2026-06-01 -o datos/sinteticos/x.csv
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

RAIZ = Path(__file__).resolve().parents[1]
DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p"
INICIO_DEFECTO = date(2026, 1, 1)
EXPECTED_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
]

# Viajes por hora del día (0-23), proporcionales al perfil típico de los
# taxis amarillos de Nueva York: mínimo a las 4-5 y pico a las 18-19. La
# muestra no sirve para esto: sus mil viajes son todos de la madrugada del
# 1 de enero, y copiar su hora amontonaría el año entero entre las 0 y las 5.
PESOS_HORA = [3.0, 2.2, 1.6, 1.2, 0.9, 1.0, 2.0, 3.6, 4.6, 4.7, 4.5, 4.6,
              4.9, 5.0, 5.2, 5.2, 4.9, 5.5, 6.3, 6.4, 5.8, 5.6, 5.3, 4.2]
HORAS = list(range(24))

# Parte de los viajes que cambia de zona respecto a su plantilla. El resto
# conserva las zonas reales, para que los mapas por zona tengan sentido.
PROB_CAMBIO_ZONA = 0.35


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera un CSV sintético de 2026 con el esquema del portal."
    )
    parser.add_argument(
        "rows",
        nargs="?",
        type=positive_int,
        help="Número de filas que se generarán.",
    )
    parser.add_argument(
        "-n",
        "--rows",
        dest="rows_option",
        type=positive_int,
        help="Número de filas que se generarán.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Ruta del CSV de salida. Por defecto: datos/sinteticos/sintetico_<filas>.csv.",
    )
    parser.add_argument(
        "-s",
        "--source",
        type=Path,
        default=RAIZ / "datos" / "muestra_1000.csv",
        help="CSV de referencia. Por defecto: datos/muestra_1000.csv.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Semilla aleatoria para reproducir la generación.",
    )
    parser.add_argument(
        "--desde",
        type=parse_date,
        default=None,
        help=f"Primer día (YYYY-MM-DD). Por defecto: {INICIO_DEFECTO}.",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=None,
        help="Último día (YYYY-MM-DD). Por defecto y como máximo: hoy.",
    )
    parser.add_argument(
        "--days-back",
        type=positive_int,
        default=None,
        help="Alternativa a --desde: número de días hacia atrás desde end-date.",
    )
    parser.add_argument(
        "--today",
        action="store_true",
        help="Genera todas las filas con la fecha de hoy (hasta ahora).",
    )

    args = parser.parse_args()

    if args.rows is not None and args.rows_option is not None:
        parser.error("indica el número de filas una sola vez")
    args.rows = args.rows if args.rows is not None else args.rows_option
    if args.rows is None:
        parser.error("falta indicar el número de filas, por ejemplo: 10000")
    if args.desde is not None and args.days_back is not None:
        parser.error("usa --desde o --days-back, no los dos")
    return args


def positive_int(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("debe ser un entero positivo")
    return number


def parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "la fecha debe tener formato YYYY-MM-DD"
        ) from error


def load_reference(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"no existe el CSV de referencia: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != EXPECTED_COLUMNS:
            raise ValueError(
                "el CSV de referencia no tiene el esquema esperado. "
                f"Columnas encontradas: {reader.fieldnames}"
            )
        rows = list(reader)

    if not rows:
        raise ValueError("el CSV de referencia no contiene datos")

    # Lo que es igual para todas las copias de una plantilla se calcula una
    # sola vez: con millones de filas, parsear fechas en cada una se nota.
    for row in rows:
        pickup = datetime.strptime(row["tpep_pickup_datetime"], DATE_FORMAT)
        dropoff = (datetime.strptime(row["tpep_dropoff_datetime"], DATE_FORMAT)
                   if row["tpep_dropoff_datetime"] else None)
        row["_duracion"] = (dropoff - pickup).total_seconds() if dropoff else None
    return rows


def num(value: str) -> float | None:
    return float(value) if value not in ("", None) else None


def money(value: float | None) -> str:
    return "" if value is None else f"{value:.2f}"


def varia(value: float | None, rng: random.Random, lo: float, hi: float) -> float | None:
    """Varía un importe conservando el signo: un negativo es una devolución
    real de la muestra y tiene que seguir siéndolo."""
    return None if value is None else value * rng.uniform(lo, hi)


def make_row(
    template: Dict[str, str],
    rng: random.Random,
    start: datetime,
    now: datetime,
    zonas: List[str],
) -> List[str]:
    # ── Fechas: un día del rango, a una hora según el perfil diario ─────
    days = (now.date() - start.date()).days
    day = start + timedelta(days=rng.randint(0, days))
    hora = rng.choices(HORAS, weights=PESOS_HORA)[0]
    pickup = day + timedelta(hours=hora, seconds=rng.randint(0, 3599))

    factor = rng.uniform(0.75, 1.25)
    base = template["_duracion"]
    duration = None if base is None else max(0.0, base * factor)

    # Nunca en el futuro: si el viaje no habría terminado aún, se lleva a
    # un momento que ya ha pasado
    fin = pickup + timedelta(seconds=duration or 0)
    if fin > now:
        pickup -= fin - now + timedelta(seconds=rng.randint(60, 3 * 3600))
    if pickup < start:
        # Cabe entero entre el inicio y ahora (con --today a primera hora
        # el margen puede ser de minutos)
        hueco = (now - start).total_seconds() - (duration or 0)
        pickup = start + timedelta(seconds=rng.randint(0, max(0, int(min(3600, hueco)))))
    dropoff = None if duration is None else pickup + timedelta(seconds=duration)

    # ── Distancia con el mismo factor que la duración: velocidad creíble ─
    distance = num(template["trip_distance"])
    distance = None if distance is None else distance * factor

    # ── Importes: el total cuadra con la suma de sus partes ─────────────
    fare = varia(num(template["fare_amount"]), rng, 0.85, 1.20)
    tip = varia(num(template["tip_amount"]), rng, 0.7, 1.3)
    extra = num(template["extra"])
    mta_tax = num(template["mta_tax"])
    tolls = num(template["tolls_amount"])
    improvement = num(template["improvement_surcharge"])
    congestion = num(template["congestion_surcharge"])
    total = sum(v for v in (fare, extra, mta_tax, tip, tolls, improvement, congestion)
                if v is not None)

    # ── Zonas reales de la muestra; una parte cambia ────────────────────
    pu = template["PULocationID"]
    do = template["DOLocationID"]
    if rng.random() < PROB_CAMBIO_ZONA:
        pu = rng.choice(zonas)
    if rng.random() < PROB_CAMBIO_ZONA:
        do = rng.choice(zonas)

    # ── Pasajeros: solo cambia quien ya tenía, el cero es un aviso real ──
    passengers = template["passenger_count"]
    if passengers not in ("", "0") and rng.random() < 0.2:
        passengers = str(min(6, max(1, int(float(passengers)) + rng.choice((-1, 1)))))

    return [
        template["VendorID"],
        pickup.strftime(DATE_FORMAT),
        "" if dropoff is None else dropoff.strftime(DATE_FORMAT),
        passengers,
        "" if distance is None else f"{distance:.2f}",
        template["RatecodeID"],
        template["store_and_fwd_flag"],
        pu,
        do,
        template["payment_type"],
        money(fare),
        template["extra"],
        template["mta_tax"],
        money(tip),
        template["tolls_amount"],
        template["improvement_surcharge"],
        money(total),
        template["congestion_surcharge"],
    ]


def rango(args: argparse.Namespace, now: datetime) -> datetime:
    """Primer instante del rango, ya validado contra hoy."""
    end = min(args.end_date or now.date(), now.date())
    if args.today:
        first = now.date()
    elif args.days_back is not None:
        first = end - timedelta(days=args.days_back)
    else:
        first = args.desde or INICIO_DEFECTO
    if first > now.date():
        raise ValueError(f"la fecha de inicio {first} es posterior a hoy")
    return datetime.combine(first, datetime.min.time())


def main() -> int:
    args = parse_args()
    output = args.output or RAIZ / "datos" / "sinteticos" / f"sintetico_{args.rows}.csv"
    rng = random.Random(args.seed)

    # "Ahora" en UTC sin zona. Con --end-date anterior a hoy, el tope es el
    # final de ese día.
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    if args.end_date and args.end_date < now.date():
        now = datetime.combine(args.end_date, datetime.max.time()).replace(microsecond=0)

    try:
        start = rango(args, now)
        reference = load_reference(args.source)
        zonas = sorted({r["PULocationID"] for r in reference}
                       | {r["DOLocationID"] for r in reference})

        output.parent.mkdir(parents=True, exist_ok=True)
        t0 = time.monotonic()
        with output.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(EXPECTED_COLUMNS)
            for i in range(args.rows):
                writer.writerow(make_row(rng.choice(reference), rng, start, now, zonas))
                if (i + 1) % 1_000_000 == 0:
                    print(f"  {i + 1:,} filas ({time.monotonic() - t0:.0f} s)", flush=True)
    except (OSError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    print(f"Generadas {args.rows:,} filas en {output}")
    print(f"Rango temporal (UTC): {start} a {now}")
    if args.seed is not None:
        print(f"Semilla utilizada: {args.seed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
