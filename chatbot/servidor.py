"""
PIDS Parte 3 — Chatbot · Backend FastAPI.

    Streamlit → [este servicio] → SDK openai → OpenRouter → modelo
                       └→ herramientas → API de la Parte 2 (HTTP)

La interfaz solo habla con este servicio y nunca ve la clave. Es sin
estado: cada POST /chat trae la conversación entera.
"""

from __future__ import annotations

from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

from chatbot import config
from chatbot.agente import ErrorModelo, responder
from chatbot.herramientas import HERRAMIENTAS

app = FastAPI(title="PIDS · Chatbot (Parte 3)", version="1.0")


class Mensaje(BaseModel):
    rol: Literal["user", "assistant"]
    contenido: str = Field(min_length=1, max_length=4000)


class Peticion(BaseModel):
    mensajes: list[Mensaje] = Field(min_length=1, max_length=40)

    @field_validator("mensajes")
    @classmethod
    def acaba_en_usuario(cls, v: list[Mensaje]) -> list[Mensaje]:
        if v[-1].rol != "user":
            raise ValueError("el último mensaje tiene que ser del usuario")
        return v


@app.get("/health")
def health() -> dict:
    real = config.MODO == "openrouter"
    return {"estado": "ok", "modo": config.MODO, "modelo": config.MODELO if real else "simulado",
            "respaldo": config.RESPALDO if real else [], "api_datos": config.API_URL}


@app.get("/herramientas")
def herramientas() -> list[dict]:
    """El contrato que se manda al modelo, para poder enseñarlo."""
    return [h.como_tool()["function"] for h in HERRAMIENTAS.values()]


@app.post("/chat")
def chat(peticion: Peticion) -> dict:
    historial = [{"role": m.rol, "content": m.contenido} for m in peticion.mensajes]
    try:
        return responder(historial).como_dict()
    except ErrorModelo as e:
        raise HTTPException(502, str(e)) from e
