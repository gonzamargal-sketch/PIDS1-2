#!/usr/bin/env python3
"""Genera datos sintéticos compatibles con el mismo formato que rows.csv.

Ejemplos:

    python generar_datos_sinteticos.py 10000
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List


DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p"
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera un CSV sintético con el esquema de rows.csv."
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
        help="Ruta del CSV de salida. Por defecto: synthetic_<filas>.csv.",
    )
    parser.add_argument(
        "-s",
        "--source",
        type=Path,
        default=Path(__file__).with_name("rows.csv"),
        help="CSV de referencia. Por defecto: rows.csv junto al script.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Semilla aleatoria para reproducir la generación.",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=date.today(),
        help="Fecha final en formato YYYY-MM-DD. Por defecto: hoy.",
    )
    parser.add_argument(
        "--days-back",
        type=positive_int,
        default=365,
        help="Número de días hacia atrás desde end-date. Por defecto: 365.",
    )
    parser.add_argument(
        "--today",
        action="store_true",
        help="Genera todas las filas con la fecha de hoy.",
    )

    args = parser.parse_args()

    if args.rows is not None and args.rows_option is not None:
        parser.error("indica el número de filas una sola vez")
    args.rows = args.rows if args.rows is not None else args.rows_option
    if args.rows is None:
        parser.error("falta indicar el número de filas, por ejemplo: 10000")
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
    return rows


def money(value: float) -> str:
    return f"{max(0.0, value):.2f}"


def integer(value: float) -> str:
    return str(max(0, int(round(value))))


def make_row(
    template: Dict[str, str],
    rng: random.Random,
    end_date: date,
    days_back: int,
) -> Dict[str, str]:
    template_pickup = datetime.strptime(
        template["tpep_pickup_datetime"], DATE_FORMAT
    )

    generated_day = end_date - timedelta(days=rng.randint(0, days_back))
    pickup = datetime.combine(generated_day, template_pickup.time())

    duration = rng.randint(2 * 60, 75 * 60)
    distance = max(
        0.1,
        float(template["trip_distance"]) * rng.uniform(0.75, 1.25),
    )
    duration = max(
        60,
        int(duration * max(0.6, min(2.0, distance / 2.0))),
    )
    dropoff = pickup + timedelta(seconds=duration)

    passengers = max(
        1,
        min(6, int(round(float(template["passenger_count"])))),
    )
    if rng.random() < 0.08:
        passengers = 1

    fare = max(
        2.5,
        float(template["fare_amount"]) * rng.uniform(0.85, 1.20),
    )
    extra = max(
        0.0,
        float(template["extra"]) * rng.uniform(0.8, 1.2),
    )
    mta_tax = 0.5
    tip = max(
        0.0,
        float(template["tip_amount"]) * rng.uniform(0.7, 1.3),
    )
    tolls = max(
        0.0,
        float(template["tolls_amount"]) * rng.uniform(0.8, 1.2),
    )
    improvement = 0.3
    congestion = max(
        0.0,
        float(template["congestion_surcharge"]) * rng.uniform(0.8, 1.2),
    )
    total = fare + extra + mta_tax + tip + tolls + improvement + congestion

    # Evita que todos los registros sintéticos tengan exactamente los mismos
    # identificadores cuando se genera un volumen grande.
    pickup_zone = rng.randint(1, 265)
    dropoff_zone = rng.randint(1, 265)

    return {
        "VendorID": template["VendorID"],
        "tpep_pickup_datetime": pickup.strftime(DATE_FORMAT),
        "tpep_dropoff_datetime": dropoff.strftime(DATE_FORMAT),
        "passenger_count": str(passengers),
        "trip_distance": f"{distance:.1f}",
        "RatecodeID": template["RatecodeID"],
        "store_and_fwd_flag": template["store_and_fwd_flag"],
        "PULocationID": str(pickup_zone),
        "DOLocationID": str(dropoff_zone),
        "payment_type": template["payment_type"],
        "fare_amount": money(fare),
        "extra": money(extra),
        "mta_tax": money(mta_tax),
        "tip_amount": money(tip),
        "tolls_amount": money(tolls),
        "improvement_surcharge": money(improvement),
        "total_amount": money(total),
        "congestion_surcharge": money(congestion),
    }


def generate(
    reference: List[Dict[str, str]],
    count: int,
    rng: random.Random,
    end_date: date,
    days_back: int,
) -> List[Dict[str, str]]:
    return [
        make_row(rng.choice(reference), rng, end_date, days_back)
        for _ in range(count)
    ]


def main() -> int:
    args = parse_args()
    output = args.output or Path(f"synthetic_{args.rows}.csv")
    rng = random.Random(args.seed)

    try:
        reference = load_reference(args.source)
        days_back = 0 if args.today else args.days_back
        rows = generate(
            reference,
            args.rows,
            rng,
            args.end_date,
            days_back,
        )

        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=EXPECTED_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
    except (OSError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    print(f"Generadas {args.rows} filas en {output}")
    print(
        f"Rango temporal: {args.end_date - timedelta(days=days_back)} "
        f"a {args.end_date}"
    )
    if args.seed is not None:
        print(f"Semilla utilizada: {args.seed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())