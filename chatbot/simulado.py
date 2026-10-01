"""
PIDS Parte 3 — Chatbot · Modelo simulado (sin clave de OpenRouter).

Sustituye al LLM con reglas, para poder probar todo el circuito
(Streamlit → FastAPI → herramientas → API de la Parte 2) sin clave ni
coste. Hace a mano las tres fases de la teoría:

  NLU        intención (ingresos, ranking de zonas, ciclo de vida…),
             zona mencionada y periodo («ayer», «agosto», «últimos 30 días»)
  Acción     propone las mismas tool calls que propondría el modelo,
             incluida la cadena buscar_zona → consultar_ingresos
  Respuesta  redacta el texto a partir de lo que devolvieron

Devuelve objetos del SDK de openai, así agente.py no distingue los dos
modos. Entiende frases sencillas; para lo demás está el modelo real.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timedelta, timezone

from openai.types.chat import ChatCompletion

from chatbot import zonas
from chatbot.zonas import normalizar

MESES = {m: i for i, m in enumerate(
    ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
     "septiembre", "octubre", "noviembre", "diciembre"], start=1)}
NUMEROS = {"un": 1, "una": 1, "dos": 2, "tres": 3, "cuatro": 4, "cinco": 5, "seis": 6,
           "siete": 7, "ocho": 8, "nueve": 9, "diez": 10, "quince": 15, "veinte": 20}


# ── Formato ──────────────────────────────────────────────────────

def _num(v, dec=0) -> str:
    if v is None:
        return "—"
    s = f"{v:,.{dec}f}"
    return s.replace(",", "·").replace(".", ",").replace("·", ".")


def _usd(v) -> str:
    return "—" if v is None else f"{_num(v, 2)} $"


def _fecha(iso: str) -> str:
    return iso[:16].replace("T", " ")


# ── NLU: periodo ─────────────────────────────────────────────────

def _inicio_dia(d: datetime) -> datetime:
    return d.replace(hour=0, minute=0, second=0, microsecond=0)


def _mes(anio: int, mes: int) -> datetime:
    return datetime(anio + (mes - 1) // 12, (mes - 1) % 12 + 1, 1, tzinfo=timezone.utc)


def periodo(texto: str, ahora: datetime) -> tuple[datetime, datetime, str]:
    t = normalizar(texto)
    hoy = _inicio_dia(ahora)

    fechas = re.findall(r"\b(2026-\d{2}-\d{2})\b", texto)
    if fechas:
        ini = datetime.fromisoformat(fechas[0]).replace(tzinfo=timezone.utc)
        fin = datetime.fromisoformat(fechas[-1]).replace(tzinfo=timezone.utc) + timedelta(days=1)
        return ini, fin, f"del {fechas[0]} al {fechas[-1]}"

    meses = [MESES[p] for p in re.findall(r"[a-z]+", t) if p in MESES]
    if meses:
        nombres = [k for k, v in MESES.items() if v in (meses[0], meses[-1])]
        desc = f"en {nombres[0]}" if meses[0] == meses[-1] else f"de {nombres[0]} a {nombres[-1]}"
        return _mes(2026, meses[0]), _mes(2026, meses[-1] + 1), desc

    m = re.search(r"ultim[oa]s? (\d+|[a-z]+) (dias|semanas|horas)", t)
    if m:
        n = int(m.group(1)) if m.group(1).isdigit() else NUMEROS.get(m.group(1), 7)
        paso = {"dias": timedelta(days=1), "semanas": timedelta(weeks=1), "horas": timedelta(hours=1)}[m.group(2)]
        return ahora - n * paso, ahora, f"en las últimas {n} {m.group(2).replace('dias', 'días')}" if m.group(2) != "dias" else f"en los últimos {n} días"

    lunes = hoy - timedelta(days=hoy.weekday())
    reglas = [
        ("anteayer", hoy - timedelta(days=2), hoy - timedelta(days=1), "anteayer"),
        ("ayer", hoy - timedelta(days=1), hoy, "ayer"),
        ("hoy", hoy, ahora, "hoy"),
        ("semana pasada", lunes - timedelta(weeks=1), lunes, "la semana pasada"),
        ("esta semana", lunes, ahora, "esta semana"),
        ("ultima semana", ahora - timedelta(days=7), ahora, "en la última semana"),
        ("mes pasado", _mes(hoy.year, hoy.month - 1), _mes(hoy.year, hoy.month), "el mes pasado"),
        ("este mes", _mes(hoy.year, hoy.month), ahora, "este mes"),
        ("ultimo mes", ahora - timedelta(days=30), ahora, "en los últimos 30 días"),
        ("ultima hora", ahora - timedelta(hours=1), ahora, "en la última hora"),
    ]
    for clave, ini, fin, desc in reglas:
        if clave in t:
            return ini, fin, desc
    if any(k in t for k in ("este ano", "todo el ano", "en total", "desde enero", "2026", "historico", "todo")):
        return datetime(2026, 1, 1, tzinfo=timezone.utc), ahora, "en lo que va de 2026"
    return ahora - timedelta(days=7), ahora, "en los últimos 7 días (no indicaste periodo)"


def _cantidad(t: str, defecto: int) -> int:
    m = re.search(r"\b(\d{1,2})\b(?! de)", t) or re.search(r"\b(" + "|".join(NUMEROS) + r")\b (viajes|zonas)", t)
    if not m:
        return defecto
    v = m.group(1)
    return int(v) if v.isdigit() else NUMEROS[v]


# ── NLU: intención → plan de herramientas ────────────────────────

def plan(texto: str, ahora: datetime) -> tuple[list[tuple[str, dict]], str]:
    """Las llamadas que haría el modelo, en orden. Un valor "$zona" se
    rellena con el id que devuelva buscar_zona."""
    t = normalizar(texto)
    ini, fin, desc = periodo(texto, ahora)
    rango = {"inicio": ini.isoformat(), "fin": fin.isoformat()}
    zona_txt = zonas.mencionada_en(texto)
    tiene = lambda *ks: any(k in t for k in ks)  # noqa: E731

    if tiene("archiv", "politica", "ciclo de vida", "retencion", "cumpl", "pendiente", "desaloj", "particion", "al dia"):
        return [("estado_ciclo_vida", {})], desc
    if tiene("coste", "ocupa", "espacio", "bytes", "almacen", "comprim"):
        return [("metricas", {"nombre": "coste"})], desc
    if tiene("latencia", "rapid", "lent", "tarda", "velocidad de la api"):
        return [("metricas", {"nombre": "latencia"})], desc
    if tiene("calidad", "cuarentena", "rechaz", "sucio", "invalid"):
        return [("metricas", {"nombre": "calidad"})], desc
    if tiene("que zona es", "id de", "codigo de", "como se llama la zona", "busca la zona"):
        return [("buscar_zona", {"texto": zona_txt or t.split()[-1]})], desc
    if tiene("ejemplo", "muestra", "ensena", "lista", "algunos viajes", "ultimos viajes", "dame viajes"):
        return [("ver_viajes", {**rango, "limite": min(_cantidad(t, 5), 20)})], desc
    if tiene("zonas", "ranking", "barrios") or (tiene("zona") and tiene("mas", "mejor", "top", "cual", "que zona")):
        return [("desglose_viajes", {**rango, "por": "zona", "top": min(_cantidad(t, 5), 20)})], desc

    previo = [("buscar_zona", {"texto": zona_txt})] if zona_txt else []
    con_zona = {"zona": "$zona"} if zona_txt else {}
    if tiene("por dia", "cada dia", "diario", "evolucion", "dia a dia", "dia por dia"):
        return previo + [("desglose_viajes", {**rango, "por": "dia", **con_zona})], desc
    if tiene("ingres", "factur", "dinero", "recaud", "gan", "cuanto", "cuantos", "viajes", "propina", "importe", "media", "total"):
        return previo + [("consultar_ingresos", {**rango, **con_zona})], desc
    return [], desc


# ── Respuesta ────────────────────────────────────────────────────

def _origen(o: dict) -> str:
    if not o or "tramos" not in o:
        return ""
    tramos = "; ".join(f"{t['tier']} del {_fecha(t['desde'])} al {_fecha(t['hasta'])}" for t in o["tramos"])
    return f"\n\n**Origen:** {o['tier']}" + (f" — {tramos}" if len(o["tramos"]) > 1 else "") + \
           f". Consultado a las {_fecha(o.get('datos_a') or '')} UTC."


def redactar(pasos: list[tuple[str, dict]], desc: str) -> str:
    if not pasos:
        return ("Puedo responder preguntas sobre los viajes de taxi de 2026 y sobre el ciclo de vida de los datos. Por ejemplo:\n"
                "- ¿Cuánto se facturó en JFK en agosto?\n- ¿Qué zonas generaron más ingresos la última semana?\n"
                "- ¿Cuántos viajes hubo cada día de esta semana?\n- ¿Va el archivado al día?\n"
                "- ¿Cuánto ocupa un viaje en cada tier?\n- Enséñame 5 viajes de ayer")
    nombre, r = pasos[-1]
    if "error" in r:
        return f"No he podido consultarlo: {r['error']}"
    v = r.get("valor")

    if nombre == "buscar_zona":
        if not v:
            return "No encuentro ninguna zona con ese nombre."
        return "Zonas encontradas:\n" + "\n".join(f"- **{z['nombre']}** ({z['distrito']}) → id {z['id']}" for z in v)
    if nombre == "consultar_ingresos":
        zona = "" if r["zona"] == "todas" else f" con recogida en **{r['zona']['nombre']}**"
        texto = (f"{desc[0].upper() + desc[1:]}{zona} hubo **{_num(v['viajes'])} viajes**, que facturaron **{_usd(v['ingresos'])}** "
                 f"(importe medio {_usd(v['importe_medio'])}, propina media {_usd(v['propina_media'])}, "
                 f"{_num(v['distancia_media_mi'], 2)} millas de media).")
        if len(r.get("por_tier", {})) > 1:
            pt = r["por_tier"]
            texto += f" De ellos, {_num(pt['cold']['viajes'])} salen del frío y {_num(pt['hot']['viajes'])} del caliente."
        return texto + _origen(r["origen"])
    if nombre == "desglose_viajes" and r["valor"] and "dia" in r["valor"][0]:
        filas = r["valor"]
        lineas = [f"- {f['dia']}: {_num(f['viajes'])} viajes · {_usd(f['ingresos'])}" for f in filas[-14:]]
        extra = f"\n(se muestran los últimos 14 de {len(filas)} días)" if len(filas) > 14 else ""
        return (f"Viajes por día {desc}:\n" + "\n".join(lineas) + extra +
                f"\n\n**Total:** {_num(r['totales']['viajes'])} viajes · {_usd(r['totales']['ingresos'])}" + _origen(r["origen"]))
    if nombre == "desglose_viajes":
        if not r["valor"]:
            return f"No hay viajes {desc}." + _origen(r["origen"])
        lineas = [f"{i}. **{z['zona_nombre']}**: {_usd(z['ingresos'])} en {_num(z['viajes'])} viajes" for i, z in enumerate(r["valor"], 1)]
        return (f"Zonas de recogida con más ingresos {desc}:\n" + "\n".join(lineas) +
                f"\n\nHubo viajes en {r.get('zonas_con_viajes')} zonas distintas." + _origen(r["origen"]))
    if nombre == "ver_viajes":
        if not v:
            return f"No hay viajes {desc}." + _origen(r["origen"])
        lineas = [f"- {_fecha(f['event_time'])} · {f['origen_nombre']} → {f['destino_nombre']} · "
                  f"{_num(f['trip_distance'], 1)} mi · {_usd(f['total_amount'])}" for f in v]
        return f"{len(v)} viajes {desc}:\n" + "\n".join(lineas) + _origen(r["origen"])
    if nombre == "estado_ciclo_vida":
        pol = {p["accion"]: p["umbral"] for p in v["politica"]}
        c = v["cumplimiento"] or {}
        estados = ", ".join(f"{e['particiones']} {e['estado']}" for e in v["particiones_por_estado"]) or "ninguna"
        return (f"**Política:** un viaje pasa al frío a los {pol.get('ARCHIVE', '—')} y se borra a los {pol.get('DELETE', '—')}.\n\n"
                f"**¿Va al día?** {'Sí' if c.get('estado') == 'OK' else 'No'} ({c.get('estado', '—')}): el dato más viejo del caliente "
                f"tiene {c.get('edad_maxima_dias', '—')} días y el límite es {c.get('umbral_dias', '—')}.\n\n"
                f"**Particiones:** {estados}; {v['pendientes_de_archivar']} pendientes de archivar." + _origen(r["origen"]))
    if nombre == "metricas":
        filas = v or []
        if filas and "bytes_por_fila" in filas[0] and "tier" in filas[0]:
            d = {f["tier"]: f for f in filas}
            factor = d["hot"]["bytes_por_fila"] / d["cold"]["bytes_por_fila"] if "hot" in d and "cold" in d else None
            return ("Bytes por viaje: " + ", ".join(f"{f['motor']} {_num(f['bytes_por_fila'], 0)} B" for f in filas) +
                    (f". Iceberg ocupa **{_num(factor, 1)} veces menos**." if factor else ".") + _origen(r["origen"]))
        if filas and "p95_ms" in filas[0]:
            return "Latencia por tier:\n" + "\n".join(
                f"- {f['tier']}: p50 {_num(f['p50_ms'])} ms, p95 {_num(f['p95_ms'])} ms ({f['consultas']} consultas)" for f in filas) + _origen(r["origen"])
        if filas and "motivo" in filas[0]:
            return (f"{_num(sum(f['registros'] for f in filas))} registros en cuarentena. Motivos:\n" +
                    "\n".join(f"- {f['motivo']}: {_num(f['registros'])}" for f in filas) + _origen(r["origen"]))
        if filas:
            f = filas[0]
            return (f"El caliente tiene {_num(f['filas'])} viajes en {f['particiones']} particiones diarias; "
                    f"el día más antiguo es {f['dia_mas_antiguo']}." + _origen(r["origen"]))
    return json.dumps(v, ensure_ascii=False)[:1500]


# ── El «cliente» con la misma forma que el SDK ───────────────────

class _Completions:
    def create(self, *, messages: list[dict], tool_choice: str = "auto", **_) -> ChatCompletion:
        ultimo = max(i for i, m in enumerate(messages) if m["role"] == "user")
        texto = messages[ultimo]["content"]
        llamadas = {}
        hechos: list[tuple[str, dict]] = []
        for m in messages[ultimo + 1:]:
            if m["role"] == "assistant":
                llamadas.update({tc["id"]: tc["function"]["name"] for tc in m.get("tool_calls", [])})
            elif m["role"] == "tool":
                hechos.append((llamadas[m["tool_call_id"]], json.loads(m["content"])))

        pasos, desc = plan(texto, datetime.now(timezone.utc))
        siguiente = pasos[len(hechos)] if tool_choice != "none" and len(hechos) < len(pasos) else None

        if siguiente:
            nombre, args = siguiente
            if args.get("zona") == "$zona":
                encontradas = (hechos[-1][1].get("valor") or []) if hechos else []
                if not encontradas:  # buscar_zona no encontró nada: se responde ya
                    siguiente = None
                else:
                    args = {**args, "zona": encontradas[0]["id"]}
        mensaje = (
            {"role": "assistant", "content": None, "tool_calls": [{
                "id": f"sim_{uuid.uuid4().hex[:8]}", "type": "function",
                "function": {"name": siguiente[0], "arguments": json.dumps(args, ensure_ascii=False)}}]}
            if siguiente else
            {"role": "assistant", "content": redactar(hechos, desc) +
             "\n\n_Modo simulado: respuesta por reglas, sin modelo de lenguaje._"}
        )
        return ChatCompletion.model_validate({
            "id": "sim", "object": "chat.completion", "created": 0, "model": "simulado",
            "choices": [{"index": 0, "finish_reason": "tool_calls" if siguiente else "stop", "message": mensaje}],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        })


class _Chat:
    completions = _Completions()


class ClienteSimulado:
    chat = _Chat()
