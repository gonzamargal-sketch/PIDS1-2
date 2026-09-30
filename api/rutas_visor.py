"""
PIDS Parte 2 — API · Visor web de /trips (P3).

Una página HTML para consultar viajes por rango de fechas sin usar curl
ni Swagger. No añade lógica: llama a /trips desde el navegador y pinta
data + meta (tabla de viajes, tier de cada uno y tramos del coverage).
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse, RedirectResponse

router = APIRouter(include_in_schema=False)

_HTML = Path(__file__).with_name("visor.html")


@router.get("/")
def raiz() -> RedirectResponse:
    return RedirectResponse("/visor")


@router.get("/visor")
def visor() -> FileResponse:
    return FileResponse(_HTML, media_type="text/html")
