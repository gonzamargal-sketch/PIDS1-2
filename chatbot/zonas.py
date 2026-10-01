"""
PIDS Parte 3 — Chatbot · Nombres de las zonas de taxi.

La API solo conoce números (pu_location_id 1..265). La tabla oficial de
la TLC de Nueva York (datos/zonas_taxi.csv, taxi_zone_lookup.csv) da el
nombre y el distrito de cada una, para que el usuario pueda preguntar por
«JFK» o «Midtown» y el chatbot lo traduzca a un id.
"""

from __future__ import annotations

import csv
import unicodedata
from functools import lru_cache
from pathlib import Path

FICHERO = Path(__file__).with_name("datos") / "zonas_taxi.csv"

# Formas habituales de nombrar algunas zonas que no coinciden con el nombre oficial
ALIAS = {
    "jfk": 132, "aeropuerto jfk": 132, "kennedy": 132,
    "laguardia": 138, "la guardia": 138,
    "newark": 1, "aeropuerto de newark": 1,
    "times square": 230, "central park": 43, "wall street": 87,
}
# Nombres que corresponden a varias zonas (Harlem, Upper East Side…) no van
# aquí: buscar() devuelve todas y el chatbot pregunta cuál.


def normalizar(texto: str) -> str:
    sin_tildes = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode()
    return " ".join(sin_tildes.lower().replace("/", " ").replace("-", " ").split())


@lru_cache(maxsize=1)
def zonas() -> dict[int, dict]:
    with FICHERO.open(newline="", encoding="utf-8") as f:
        return {
            int(r["LocationID"]): {"id": int(r["LocationID"]), "nombre": r["Zone"], "distrito": r["Borough"]}
            for r in csv.DictReader(f)
        }


def nombre(zona_id: int | None) -> str | None:
    if zona_id is None:
        return None
    z = zonas().get(int(zona_id))
    return f"{z['nombre']} ({z['distrito']})" if z else None


def buscar(texto: str, maximo: int = 5) -> list[dict]:
    """Zonas cuyo nombre o distrito contiene el texto (o un alias conocido)."""
    q = normalizar(texto)
    encontradas: list[dict] = []
    if q in ALIAS:
        encontradas.append(zonas()[ALIAS[q]])
    for z in zonas().values():
        if z in encontradas:
            continue
        if q and (q in normalizar(z["nombre"]) or q == normalizar(z["distrito"])):
            encontradas.append(z)
    # Exactas primero, luego las de nombre más corto (más específicas)
    encontradas.sort(key=lambda z: (normalizar(z["nombre"]) != q and ALIAS.get(q) != z["id"], len(z["nombre"])))
    return encontradas[:maximo]


def mencionada_en(texto: str) -> str | None:
    """Para el modo simulado: el trozo del texto que nombra una zona, si lo hay."""
    t = f" {normalizar(texto)} "
    candidatos = list(ALIAS) + [normalizar(z["nombre"]) for z in zonas().values() if len(z["nombre"]) > 4]
    for c in sorted(candidatos, key=len, reverse=True):
        if f" {c} " in t:
            return c
    return None
