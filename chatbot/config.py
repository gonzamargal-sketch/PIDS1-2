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

# Cualquier modelo de OpenRouter que admita tools. El de por defecto es
# barato (~0,10 $ / 1M tokens de entrada) y sigue bien las herramientas.
MODELO = os.environ.get("CHATBOT_MODELO", "").strip() or "google/gemini-2.5-flash-lite"

MODO = "openrouter" if OPENROUTER_API_KEY else "simulado"

# La API de la Parte 2, por la red interna de Docker
API_URL = os.environ.get("CHATBOT_API_URL", "http://api:8000").rstrip("/")

# Límites: pasos del bucle de herramientas, tiempos y tokens
MAX_PASOS = int(os.environ.get("CHATBOT_MAX_PASOS", "5"))
PRESUPUESTO_S = float(os.environ.get("CHATBOT_PRESUPUESTO_S", "90"))   # por pregunta, en total
TIMEOUT_MODELO_S = float(os.environ.get("CHATBOT_TIMEOUT_MODELO_S", "30"))
TIMEOUT_API_S = float(os.environ.get("CHATBOT_TIMEOUT_API_S", "30"))
MAX_TOKENS = int(os.environ.get("CHATBOT_MAX_TOKENS", "700"))           # por respuesta del modelo
MAX_HISTORIAL = int(os.environ.get("CHATBOT_MAX_HISTORIAL", "12"))      # mensajes previos que se reenvían
