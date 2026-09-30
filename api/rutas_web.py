"""
PIDS Parte 2 — API · Frontend web (P3).

El frontend es estático (api/web/: HTML, CSS y módulos JS sin compilar) y
se sirve desde la propia API en /app, así que llama a /trips, /stats,
/lifecycle y /metrics en el mismo origen: sin CORS ni contenedor aparte.
No añade lógica: todo lo que enseña sale de las rutas de siempre.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

DIRECTORIO = Path(__file__).with_name("web")

router = APIRouter(include_in_schema=False)

# Se monta en app.py: un APIRouter no propaga los mount al incluirse
estaticos = StaticFiles(directory=DIRECTORIO, html=True)


@router.get("/")
def raiz() -> RedirectResponse:
    return RedirectResponse("/app/")


@router.get("/visor")
def visor(request: Request) -> RedirectResponse:
    """El visor de antes vive ahora en la pestaña Viajes (conserva el rango)."""
    query = request.url.query
    return RedirectResponse("/app/#/viajes" + (f"?{query}" if query else ""))
