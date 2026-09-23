"""
PIDS Parte 2 — API de acceso a los tiers.

Esqueleto del paso 0 (A1): solo /health, para que el perfil core levante
verde desde el primer día. A partir de aquí el fichero es de P3 (T3.1),
que monta las dependencias, la instrumentación de query_log y registra
los routers.
"""

from fastapi import FastAPI

app = FastAPI(title="PIDS Parte 2 · E8", version="0.1")


@app.get("/health")
def health() -> dict:
    return {"estado": "ok"}
