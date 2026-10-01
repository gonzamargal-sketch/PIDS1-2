"""
PIDS Parte 3 — Chatbot · Interfaz (Streamlit).

Solo habla con el backend del chatbot (CHATBOT_URL). No conoce la clave
ni llama a OpenRouter ni a la API de datos: le manda la conversación y
pinta lo que vuelve, incluido cómo lo ha resuelto (herramientas,
argumentos, resultado, tokens y coste).
"""

from __future__ import annotations

import json
import os

import httpx
import streamlit as st

CHATBOT_URL = os.environ.get("CHATBOT_URL", "http://localhost:8001").rstrip("/")
FRONTEND_URL = os.environ.get("FRONTEND_URL", "http://localhost:8000/app/")

EJEMPLOS = [
    "¿Cuánto se facturó en JFK en agosto?",
    "¿Qué zonas generaron más ingresos la última semana?",
    "¿Cuántos viajes hubo cada día de esta semana?",
    "¿Va el archivado al día?",
    "¿Cuánto ocupa un viaje en cada tier?",
    "Enséñame 5 viajes de ayer",
]

st.set_page_config(page_title="PIDS · Asistente", page_icon="🚕", layout="centered")


@st.cache_data(ttl=15, show_spinner=False)
def estado_backend() -> dict | None:
    try:
        return httpx.get(f"{CHATBOT_URL}/health", timeout=5).json()
    except (httpx.HTTPError, ValueError):
        return None


def preguntar(mensajes: list[dict]) -> dict:
    cuerpo = {"mensajes": [{"rol": m["rol"], "contenido": m["contenido"]} for m in mensajes]}
    try:
        r = httpx.post(f"{CHATBOT_URL}/chat", json=cuerpo, timeout=150)
    except httpx.HTTPError as e:
        return {"error": f"El backend del chatbot no responde ({type(e).__name__}). ¿Está levantado el servicio chatbot?"}
    if r.status_code != 200:
        try:
            detalle = r.json().get("detail")
        except ValueError:
            detalle = r.text[:300]
        return {"error": f"Error {r.status_code}: {detalle}"}
    return r.json()


def detalle(r: dict) -> None:
    """El desplegable «Cómo lo he resuelto»: lo que pasó entre bambalinas."""
    pasos, uso = r.get("pasos", []), r.get("uso", {})
    coste = uso.get("coste_usd")
    resumen = (f"{len(pasos)} herramienta(s) · {uso.get('llamadas_modelo', 0)} llamada(s) al modelo · "
               f"{uso.get('tokens_entrada', 0) + uso.get('tokens_salida', 0):,} tokens · "
               + (f"{coste:.5f} $" if coste is not None else "coste 0 $") + f" · {r.get('ms', 0) / 1000:.1f} s")
    with st.expander("Cómo lo he resuelto"):
        st.caption(resumen.replace(",", "."))
        if r.get("aviso"):
            st.warning(r["aviso"])
        if not pasos:
            st.write("Sin herramientas: respuesta directa del modelo.")
        for i, p in enumerate(pasos, 1):
            icono = "✅" if p.get("ok") else "⚠️"
            st.markdown(f"**{i}. {icono} `{p['herramienta']}`** · {p.get('ms', 0):.0f} ms")
            st.code(json.dumps(p.get("argumentos"), ensure_ascii=False, indent=2), language="json")
            if p.get("ok"):
                origen = (p.get("resultado") or {}).get("origen", {})
                if origen.get("tier"):
                    st.caption(f"Origen: {origen['tier']}" + (f" · datos a {origen['datos_a'][:19].replace('T', ' ')} UTC" if origen.get("datos_a") else ""))
                st.json(p.get("resultado"), expanded=False)
            else:
                st.error(p.get("error"))


# ── Barra lateral ────────────────────────────────────────────────
with st.sidebar:
    st.header("🚕 Asistente PIDS")
    salud = estado_backend()
    if salud is None:
        st.error("Backend del chatbot sin respuesta")
    elif salud["modo"] == "openrouter":
        st.success(f"Modelo: `{salud['modelo']}` vía OpenRouter")
    else:
        st.info("**Modo simulado**: sin clave de OpenRouter responde un analizador por reglas. "
                "Poned `OPENROUTER_API_KEY` en `.env` y recread el servicio `chatbot` para usar el modelo real.")
    st.caption("Solo lectura: consulta los datos de la Parte 2 pero no cambia nada. "
               f"La política se cambia en el [frontend]({FRONTEND_URL}#/ciclo).")
    st.subheader("Prueba con")
    for e in EJEMPLOS:
        if st.button(e, width="stretch"):
            st.session_state.pendiente = e
    st.divider()
    if st.button("Nueva conversación", width="stretch"):
        st.session_state.mensajes = []
        st.rerun()

# ── Conversación ─────────────────────────────────────────────────
st.title("Asistente de datos")
st.caption("Viajes de taxi de Nueva York de 2026 y su ciclo de vida: caliente (PostgreSQL) → frío (Iceberg). Horas en UTC.")

if "mensajes" not in st.session_state:
    st.session_state.mensajes = []

for m in st.session_state.mensajes:
    with st.chat_message(m["rol"]):
        st.markdown(m["contenido"])
        if m.get("detalle"):
            detalle(m["detalle"])

pregunta = st.chat_input("Pregunta por ingresos, zonas, viajes o el ciclo de vida…") or st.session_state.pop("pendiente", None)
if pregunta:
    st.session_state.mensajes.append({"rol": "user", "contenido": pregunta})
    with st.chat_message("user"):
        st.markdown(pregunta)
    with st.chat_message("assistant"):
        with st.spinner("Consultando…"):
            r = preguntar(st.session_state.mensajes)
        if "error" in r:
            st.error(r["error"])
            st.session_state.mensajes.pop()  # que se pueda reintentar sin arrastrar la pregunta fallida
        else:
            st.markdown(r["respuesta"])
            detalle(r)
            st.session_state.mensajes.append({"rol": "assistant", "contenido": r["respuesta"], "detalle": r})
