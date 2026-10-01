"""
PIDS Parte 3 — Chatbot · Herramientas (tool calling).

El modelo PROPONE una llamada (nombre + argumentos en JSON); este módulo
la VALIDA con Pydantic y con las reglas del escenario, la EJECUTA contra
la API de la Parte 2 por HTTP y devuelve valor, periodo y origen.

Todas son de solo lectura: el chatbot no puede cambiar la política ni
borrar nada. Eso se hace desde el frontend (pestaña Ciclo de vida).

Reglas del escenario que se comprueban antes de llamar a la API:
  - Solo hay datos de 2026, del 1 de enero hasta ahora: un periodo
    anterior o futuro se rechaza; un fin en el futuro se recorta a ahora.
  - inicio < fin, y fin es exclusivo.
  - Zonas 1..265 (tabla de la TLC). Como mucho 20 viajes de ejemplo.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Callable, Literal

import httpx
from pydantic import BaseModel, Field, ValidationError, model_validator

from chatbot import config, zonas

INICIO_DATOS = datetime(2026, 1, 1, tzinfo=timezone.utc)
TIER = {"hot": "caliente (PostgreSQL)", "cold": "frío (Iceberg)", "mixto": "mixto (caliente + frío)"}
UNIDADES = {"minutes": "minutos", "hours": "horas", "days": "días", "years": "años"}


# ── Argumentos: el contrato que ve el modelo ─────────────────────

class Periodo(BaseModel):
    inicio: datetime = Field(description="Inicio del periodo, ISO 8601 (p. ej. 2026-03-01). Inclusivo. UTC.")
    fin: datetime = Field(description="Fin del periodo, ISO 8601. EXCLUSIVO: para todo marzo, fin=2026-04-01. UTC.")

    @model_validator(mode="after")
    def reglas_del_periodo(self):
        ahora = datetime.now(timezone.utc)
        self.inicio = self.inicio if self.inicio.tzinfo else self.inicio.replace(tzinfo=timezone.utc)
        self.fin = self.fin if self.fin.tzinfo else self.fin.replace(tzinfo=timezone.utc)
        if self.inicio >= self.fin:
            raise ValueError("inicio tiene que ser anterior a fin")
        if self.inicio >= ahora:
            raise ValueError(f"el periodo empieza en el futuro; solo hay datos hasta ahora ({ahora:%Y-%m-%d %H:%M} UTC)")
        if self.fin <= INICIO_DATOS:
            raise ValueError("solo hay datos desde el 2026-01-01")
        self.inicio = max(self.inicio, INICIO_DATOS)
        self.fin = min(self.fin, ahora)  # no hay datos del futuro: se recorta
        return self


class ConsultarIngresos(Periodo):
    zona: int | None = Field(None, ge=1, le=265, description="Id de la zona de recogida (1-265). Omitir para todas. Si el usuario da un nombre, usar antes buscar_zona.")


class DesgloseViajes(Periodo):
    por: Literal["dia", "zona"] = Field(description="'dia' = un total por día; 'zona' = ranking de zonas de recogida por ingresos.")
    zona: int | None = Field(None, ge=1, le=265, description="Solo con por='dia': limitar a una zona de recogida.")
    top: int = Field(10, ge=1, le=20, description="Con por='zona': cuántas zonas devolver.")


class BuscarZona(BaseModel):
    texto: str = Field(min_length=2, max_length=60, description="Nombre o parte del nombre de la zona, barrio o aeropuerto (p. ej. 'JFK', 'Midtown', 'Brooklyn').")


class VerViajes(Periodo):
    limite: int = Field(5, ge=1, le=20, description="Cuántos viajes de ejemplo devolver (máximo 20).")


class SinArgumentos(BaseModel):
    pass


class Metricas(BaseModel):
    nombre: Literal["coste", "latencia", "calidad", "caliente"] = Field(
        description="coste = bytes por fila en cada tier; latencia = tiempos de respuesta por tier; "
                    "calidad = registros rechazados por motivo; caliente = estado de PostgreSQL.")


# ── Llamadas a la API de la Parte 2 ──────────────────────────────

class ErrorHerramienta(Exception):
    pass


def _get(ruta: str, params: dict | None = None) -> dict:
    try:
        r = httpx.get(f"{config.API_URL}{ruta}", params=params, timeout=config.TIMEOUT_API_S)
    except httpx.HTTPError as e:
        raise ErrorHerramienta(f"La API de datos no responde ({type(e).__name__}).") from e
    if r.status_code >= 400:
        try:
            detalle = r.json().get("detail")
        except ValueError:
            detalle = r.text[:200]
        raise ErrorHerramienta(f"La API respondió {r.status_code}: {detalle}")
    return r.json()


def _origen(meta: dict) -> dict:
    """El «de dónde sale» que el modelo tiene que contar al usuario."""
    return {
        "tier": TIER.get(meta.get("data_source"), meta.get("data_source")),
        "tramos": [{"tier": TIER.get(t["tier"], t["tier"]), "desde": t["desde"], "hasta": t["hasta"]} for t in meta.get("coverage", [])],
        "datos_a": meta.get("as_of"),
        "latencia_api_ms": meta.get("latency_ms"),
    }


def _periodo(a: Periodo) -> dict:
    return {"inicio": a.inicio.isoformat(), "fin_exclusivo": a.fin.isoformat()}


def _con_nombre(fila: dict, clave: str = "zona") -> dict:
    return {**fila, f"{clave}_nombre": zonas.nombre(fila.get(clave))}


# ── Ejecutores ───────────────────────────────────────────────────

def consultar_ingresos(a: ConsultarIngresos) -> dict:
    r = _get("/trips/resumen", {"desde": a.inicio.isoformat(), "hasta": a.fin.isoformat(), **({"zona": a.zona} if a.zona else {})})
    d = r["data"]
    return {
        "valor": d["totales"],
        "por_tier": d.get("por_tier", {}),
        "zona": {"id": a.zona, "nombre": zonas.nombre(a.zona)} if a.zona else "todas",
        "periodo": _periodo(a),
        "origen": _origen(r["meta"]),
        "unidades": "importes en dólares (USD); distancias en millas",
    }


def desglose_viajes(a: DesgloseViajes) -> dict:
    params: dict[str, Any] = {"desde": a.inicio.isoformat(), "hasta": a.fin.isoformat(), "agrupar": a.por}
    if a.por == "zona":
        params["top"] = a.top
    elif a.zona:
        params["zona"] = a.zona
    r = _get("/trips/resumen", params)
    d = r["data"]
    filas = d.get("por_dia") if a.por == "dia" else [_con_nombre(z) for z in d.get("por_zona", [])]
    return {
        "valor": filas,
        "totales": d["totales"],
        **({"zonas_con_viajes": d.get("zonas_con_viajes")} if a.por == "zona" else {}),
        "periodo": _periodo(a),
        "origen": _origen(r["meta"]),
        "unidades": "importes en dólares (USD); distancias en millas",
    }


def buscar_zona(a: BuscarZona) -> dict:
    encontradas = zonas.buscar(a.texto)
    return {
        "valor": encontradas,
        "nota": "Sin coincidencias: pide al usuario otro nombre." if not encontradas
        else ("Varias coincidencias: si no está claro cuál quiere el usuario, pregúntale." if len(encontradas) > 1 else "Coincidencia única."),
        "origen": {"tier": "tabla de zonas de la TLC de Nueva York (local, no es la API)"},
    }


def ver_viajes(a: VerViajes) -> dict:
    r = _get("/trips", {"desde": a.inicio.isoformat(), "hasta": a.fin.isoformat(), "limite": a.limite})
    campos = ("event_time", "pu_location_id", "do_location_id", "trip_distance", "passenger_count", "total_amount", "tip_amount", "origen")
    filas = [{k: v[k] for k in campos} for v in r["data"]]
    for f in filas:
        f["origen_nombre"] = zonas.nombre(f["pu_location_id"])
        f["destino_nombre"] = zonas.nombre(f["do_location_id"])
    return {"valor": filas, "periodo": _periodo(a), "origen": _origen(r["meta"])}


def estado_ciclo_vida(_: SinArgumentos) -> dict:
    estado = _get("/lifecycle/status")
    politica = _get("/lifecycle/policy")
    d = estado["data"]
    return {
        "valor": {
            "politica": [
                {"accion": p["accion"], "umbral": f"{p['umbral_valor']} {UNIDADES.get(p['umbral_unidad'], p['umbral_unidad'])}", "descripcion": p["descripcion"]}
                for p in politica["data"]
            ],
            "cumplimiento": d["cumplimiento"],
            "particiones_por_estado": d["resumen_por_estado"],
            "pendientes_de_archivar": len(d["candidatas_a_archivar"]),
            "ultimas_archivadas": [
                {k: j[k] for k in ("particion", "estado", "filas_origen", "filas_escritas", "terminado_en")}
                for j in d["jobs"][:5]
            ],
        },
        "origen": _origen(estado["meta"]),
    }


def metricas(a: Metricas) -> dict:
    r = _get(f"/metrics/{a.nombre}")
    return {"valor": r["data"], "origen": _origen(r["meta"])}


# ── Registro: lo que se manda al modelo y cómo se ejecuta ────────

class Herramienta(BaseModel):
    nombre: str
    descripcion: str
    argumentos: type[BaseModel]
    ejecutar: Callable[[Any], dict]

    model_config = {"arbitrary_types_allowed": True}

    def como_tool(self) -> dict:
        """El esquema JSON de los argumentos, simplificado para el modelo.

        Pydantic escribe un campo opcional como anyOf[tipo, null]; algunos
        proveedores (Gemini, p. ej.) no aceptan anyOf en las tools. Se deja
        el tipo solo: que sea opcional ya lo dice no estar en "required".
        """
        esquema = self.argumentos.model_json_schema()
        esquema.pop("title", None)
        for nombre, p in list(esquema.get("properties", {}).items()):
            p.pop("title", None)
            variantes = [v for v in p.pop("anyOf", []) if v.get("type") != "null"]
            if variantes:
                p.update(variantes[0])
            if p.get("default", "") is None:
                p.pop("default")
        return {"type": "function", "function": {"name": self.nombre, "description": self.descripcion, "parameters": esquema}}


HERRAMIENTAS = {h.nombre: h for h in [
    Herramienta(nombre="consultar_ingresos", argumentos=ConsultarIngresos, ejecutar=consultar_ingresos,
                descripcion="Totales de un periodo: número de viajes, ingresos, tarifas, propinas, importe medio, distancia y pasajeros medios. Opcionalmente de una sola zona de recogida."),
    Herramienta(nombre="desglose_viajes", argumentos=DesgloseViajes, ejecutar=desglose_viajes,
                descripcion="Desglose de un periodo: por día (evolución) o ranking de zonas de recogida por ingresos."),
    Herramienta(nombre="buscar_zona", argumentos=BuscarZona, ejecutar=buscar_zona,
                descripcion="Traduce el nombre de una zona, barrio o aeropuerto de Nueva York a su id (1-265). Usar siempre que el usuario nombre una zona."),
    Herramienta(nombre="ver_viajes", argumentos=VerViajes, ejecutar=ver_viajes,
                descripcion="Unos pocos viajes de ejemplo de un periodo (máximo 20), con hora, zonas, distancia e importe."),
    Herramienta(nombre="estado_ciclo_vida", argumentos=SinArgumentos, ejecutar=estado_ciclo_vida,
                descripcion="Política de retención vigente (cuándo pasa un dato del caliente al frío y cuándo se borra), si el archivado va al día y qué particiones se han archivado."),
    Herramienta(nombre="metricas", argumentos=Metricas, ejecutar=metricas,
                descripcion="Métricas del sistema: coste de almacenamiento por tier, latencia por tier, calidad de los datos o estado del tier caliente."),
]}

TOOLS = [h.como_tool() for h in HERRAMIENTAS.values()]


def ejecutar(nombre: str, argumentos_json: str) -> dict:
    """Valida y ejecuta una llamada propuesta por el modelo.

    Nunca lanza: los errores vuelven al modelo como resultado, para que
    pueda corregir los argumentos o explicárselo al usuario.
    """
    t0 = time.perf_counter()
    paso: dict[str, Any] = {"herramienta": nombre, "argumentos": argumentos_json}
    try:
        h = HERRAMIENTAS.get(nombre)
        if h is None:
            raise ErrorHerramienta(f"No existe la herramienta '{nombre}'. Disponibles: {', '.join(HERRAMIENTAS)}")
        try:
            crudos = json.loads(argumentos_json or "{}")
        except json.JSONDecodeError as e:
            raise ErrorHerramienta(f"Los argumentos no son JSON válido: {e}") from e
        args = h.argumentos.model_validate(crudos)
        paso["argumentos"] = args.model_dump(mode="json")
        paso["ok"] = True
        paso["resultado"] = h.ejecutar(args)
    except ValidationError as e:
        paso["ok"] = False
        paso["error"] = "Argumentos no válidos: " + "; ".join(
            f"{'.'.join(map(str, x['loc'])) or 'periodo'}: {x['msg'].removeprefix('Value error, ')}" for x in e.errors())
    except ErrorHerramienta as e:
        paso["ok"] = False
        paso["error"] = str(e)
    paso["ms"] = round((time.perf_counter() - t0) * 1000, 1)
    return paso
