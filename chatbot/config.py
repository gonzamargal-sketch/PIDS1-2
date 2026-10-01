"""
PIDS Parte 3 — Chatbot · Configuración.

La clave de OpenRouter solo existe en este proceso (servicio chatbot):
llega por variable de entorno desde .env y nunca se manda al navegador
ni a la interfaz de Streamlit. Sin clave, el chatbot arranca en modo
simulado para poder probar todo el circuito igualmente.
"""

from __future__ import annotations

import os

OPENROUTER_URL = "https://openrouter.ai/api/v1"
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "").strip()

# Cualquier modelo de OpenRouter que admita tools. Por defecto, gratuitos
# (sufijo :free): no cuestan nada pero tienen límite de peticiones y a
# veces están saturados, así que se pasa una lista de respaldo y OpenRouter
# salta solo al siguiente si el primero falla o está limitado.
MODELO = os.environ.get("CHATBOT_MODELO", "").strip() or "qwen/qwen3.8-27b:free"
# (`or`, no default de get: docker compose pasa la variable vacía si no está en .env)
RESPALDO = [m.strip() for m in (
    os.environ.get("CHATBOT_MODELOS_RESPALDO", "").strip()
    or "google/gemma-4-31b-it:free,nvidia/nemotron-3-super-120b-a12b:free"
).split(",") if m.strip()]

MODO = "openrouter" if OPENROUTER_API_KEY else "simulado"

# La API de la Parte 2, por la red interna de Docker
API_URL = os.environ.get("CHATBOT_API_URL", "http://api:8000").rstrip("/")

# Límites: pasos del bucle de herramientas, tiempos y tokens
MAX_PASOS = int(os.environ.get("CHATBOT_MAX_PASOS", "5"))
PRESUPUESTO_S = float(os.environ.get("CHATBOT_PRESUPUESTO_S", "90"))   # por pregunta, en total
TIMEOUT_MODELO_S = float(os.environ.get("CHATBOT_TIMEOUT_MODELO_S", "30"))
TIMEOUT_API_S = float(os.environ.get("CHATBOT_TIMEOUT_API_S", "30"))
# Por respuesta del modelo. Holgado porque algunos modelos gratuitos
# «razonan» antes de contestar y eso también gasta tokens.
MAX_TOKENS = int(os.environ.get("CHATBOT_MAX_TOKENS", "1500"))
MAX_HISTORIAL = int(os.environ.get("CHATBOT_MAX_HISTORIAL", "12"))      # mensajes previos que se reenvían
