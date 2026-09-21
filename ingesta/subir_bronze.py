#!/usr/bin/env python3
"""
PIDS Parte 2 — Paso 4: subida a la zona BRONZE.

Sube los ficheros del dataset a MinIO TAL CUAL, sin tocarlos. Bronze es
la copia cruda e inmutable: si más adelante descubrimos que el contrato
de datos estaba mal, se reprocesa desde aquí sin volver a descargar nada.

    datos/*.csv  ->  s3://bronze/nyc-taxi/2020/<fichero>

Uso:
    python ingesta/subir_bronze.py                    # sube datos/*.csv
    python ingesta/subir_bronze.py --origen ./bronze  # otra carpeta
    python ingesta/subir_bronze.py --listar           # solo ver qué hay
"""

from __future__ import annotations

import sys
import hashlib
import logging
import argparse
from pathlib import Path

if sys.stdout.encoding is not None and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.config import MINIO   # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("bronze")

PREFIJO = "nyc-taxi/2020"


def cliente_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO.url,
        aws_access_key_id=MINIO.access_key,
        aws_secret_access_key=MINIO.secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def md5(ruta: Path, trozo: int = 1 << 20) -> str:
    h = hashlib.md5()
    with open(ruta, "rb") as f:
        for b in iter(lambda: f.read(trozo), b""):
            h.update(b)
    return h.hexdigest()


def ya_esta(s3, clave: str, tam: int, digest: str) -> bool:
    """Evita resubir lo que ya está igual. La subida es idempotente."""
    try:
        cab = s3.head_object(Bucket=MINIO.bucket_bronze, Key=clave)
    except ClientError:
        return False
    if cab["ContentLength"] != tam:
        return False
    return cab.get("Metadata", {}).get("md5", "") == digest


def listar(s3) -> None:
    resp = s3.list_objects_v2(Bucket=MINIO.bucket_bronze, Prefix=PREFIJO)
    objetos = resp.get("Contents", [])
    if not objetos:
        log.info("Bronze vacío en s3://%s/%s", MINIO.bucket_bronze, PREFIJO)
        return
    total = 0
    for o in sorted(objetos, key=lambda x: x["Key"]):
        log.info("  %-48s %9.1f MB", o["Key"], o["Size"] / 1e6)
        total += o["Size"]
    log.info("  %d objetos, %.2f GB", len(objetos), total / 1e9)


def subir(origen: Path, patron: str) -> int:
    ficheros = sorted(origen.glob(patron))
    if not ficheros:
        log.error("No hay ficheros que casen con %s en %s", patron, origen)
        return 1

    s3 = cliente_s3()
    log.info("Subiendo %d fichero(s) a s3://%s/%s",
             len(ficheros), MINIO.bucket_bronze, PREFIJO)

    subidos = omitidos = 0
    for f in ficheros:
        clave = f"{PREFIJO}/{f.name}"
        tam = f.stat().st_size
        digest = md5(f)

        if ya_esta(s3, clave, tam, digest):
            log.info("  = %-40s ya estaba (%.1f MB)", f.name, tam / 1e6)
            omitidos += 1
            continue

        s3.upload_file(
            str(f), MINIO.bucket_bronze, clave,
            ExtraArgs={"Metadata": {"md5": digest, "origen": "nyc-open-data"}},
        )
        log.info("  + %-40s subido (%.1f MB)", f.name, tam / 1e6)
        subidos += 1

    log.info("Hecho: %d subidos, %d ya estaban", subidos, omitidos)
    listar(s3)
    return 0


def main() -> int:
    raiz = Path(__file__).resolve().parents[1]
    p = argparse.ArgumentParser(description="Sube el dataset crudo a bronze")
    p.add_argument("--origen", type=Path, default=raiz / "datos")
    p.add_argument("--patron", default="*.csv")
    p.add_argument("--listar", action="store_true",
                   help="Solo listar lo que ya hay en bronze")
    a = p.parse_args()

    if a.listar:
        listar(cliente_s3())
        return 0
    return subir(a.origen, a.patron)


if __name__ == "__main__":
    raise SystemExit(main())
