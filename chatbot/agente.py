"""
PIDS Parte 3 — Chatbot · El agente (bucle de tool calling).

Los cuatro pasos de la diapositiva:
  1. Enviar al modelo la pregunta, el contexto y el contrato de las
     herramientas (TOOLS).
  2. Recibir la llamada que propone; validarla (Pydantic + reglas del
     escenario, en herramientas.ejecutar).
  3. Consultar la API de la Parte 2 por HTTP; devolver valor, periodo y
     origen.
  4. Mandar el resultado al modelo para que redacte la respuesta.
     Limitando pasos (MAX_PASOS), tiempo (PRESUPUESTO_S, timeouts) y
     tokens (MAX_TOKENS), y midiendo tokens y coste de cada llamada.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import openai
from openai import OpenAI

from chatbot import config
from chatbot.herramientas import TOOLS, ejecutar


def instrucciones() -> str:
    ahora = datetime.now(timezone.utc)
    return f"""Eres el asistente de datos del proyecto PIDS (escenario E8: retención y ciclo de vida de los datos).
Respondes en español, breve y claro.

Contexto:
- Ahora son las {ahora:%Y-%m-%d %H:%M} UTC ({ahora:%A}). Todas las fechas y horas son UTC.
- Los datos son viajes de taxi amarillo de Nueva York de 2026, desde el 1 de enero hasta ahora.
- Los viajes recientes están en el tier CALIENTE (PostgreSQL: rápido, caro) y los antiguos en el FRÍO
  (Iceberg sobre MinIO: barato, más lento). Un dato pasa al frío al cumplir la política de retención.

Reglas:
- Cualquier número sale de una herramienta. Nunca inventes cifras ni las estimes.
- Si el usuario nombra una zona (JFK, Midtown, Brooklyn…), llama antes a buscar_zona para obtener su id.
  Si hay varias coincidencias y no está claro cuál, pregunta.
- Las fechas van en ISO y 'fin' es EXCLUSIVO: todo marzo es inicio=2026-03-01, fin=2026-04-01;
  «ayer» es inicio=ayer 00:00, fin=hoy 00:00. Si no dice periodo, usa los últimos 7 días y dilo.
- En cada respuesta con datos indica el PERIODO y el ORIGEN: de qué tier sale (caliente, frío o mixto)
  y a qué hora se consultó. Si es mixto, explica qué parte es de cada tier.
- Eres de solo lectura: no puedes cambiar la política ni borrar o mover datos. Si te lo piden, explica
  que se hace desde el frontend (http://localhost:8000/app/, pestaña «Ciclo de vida»).
- Si una herramienta devuelve un error, corrige los argumentos si puedes o explícaselo al usuario.
- Importes en dólares con dos decimales; números con separador de miles."""


@dataclass
class Uso:
    llamadas_modelo: int = 0
    tokens_entrada: int = 0
    tokens_salida: int = 0
    coste_usd: float | None = None

    def sumar(self, usage: Any) -> None:
        self.llamadas_modelo += 1
        if usage is None:
            return
        self.tokens_entrada += getattr(usage, "prompt_tokens", 0) or 0
        self.tokens_salida += getattr(usage, "completion_tokens", 0) or 0
        coste = getattr(usage, "cost", None)  # OpenRouter lo añade con usage.include
        if coste is not None:
            self.coste_usd = round((self.coste_usd or 0) + float(coste), 6)


@dataclass
class Resultado:
    respuesta: str
    pasos: list[dict] = field(default_factory=list)
    uso: Uso = field(default_factory=Uso)
    modo: str = config.MODO
    modelo: str = config.MODELO
    ms: float = 0.0
    aviso: str | None = None

    def como_dict(self) -> dict:
        return asdict(self)


class ErrorModelo(Exception):
    """Fallo hablando con el modelo (clave, cuota, red…), con un mensaje para el usuario."""


def cliente():
    if config.MODO == "openrouter":
        return OpenAI(base_url=config.OPENROUTER_URL, api_key=config.OPENROUTER_API_KEY,
                      timeout=config.TIMEOUT_MODELO_S, max_retries=1)
    from chatbot.simulado import ClienteSimulado
    return ClienteSimulado()


def _llamar_modelo(c, mensajes: list[dict], con_herramientas: bool):
    try:
        return c.chat.completions.create(
            model=config.MODELO,
            messages=mensajes,
            tools=TOOLS,
            tool_choice="auto" if con_herramientas else "none",
            max_tokens=config.MAX_TOKENS,
            temperature=0.2,
            extra_body={"usage": {"include": True}},  # OpenRouter: devuelve el coste
        )
    except openai.AuthenticationError as e:
        raise ErrorModelo("OpenRouter rechaza la clave (OPENROUTER_API_KEY). Revisad el .env.") from e
    except openai.PermissionDeniedError as e:
        raise ErrorModelo("OpenRouter deniega el acceso: suele ser saldo insuficiente o límite de la clave.") from e
    except openai.RateLimitError as e:
        raise ErrorModelo("Límite de peticiones de OpenRouter alcanzado. Esperad unos segundos.") from e
    except openai.NotFoundError as e:
        raise ErrorModelo(f"El modelo '{config.MODELO}' no existe en OpenRouter (CHATBOT_MODELO).") from e
    except openai.APITimeoutError as e:
        raise ErrorModelo(f"El modelo no respondió en {config.TIMEOUT_MODELO_S:.0f} s.") from e
    except openai.APIConnectionError as e:
        raise ErrorModelo("No se puede conectar con OpenRouter (¿hay internet en el contenedor?).") from e
    except openai.APIStatusError as e:
        raise ErrorModelo(f"OpenRouter devolvió {e.status_code}: {e.message}") from e


def responder(historial: list[dict]) -> Resultado:
    """historial: [{"role": "user"|"assistant", "content": str}, …], el último del usuario."""
    t0 = time.monotonic()
    c = cliente()
    resultado = Resultado(respuesta="")
    mensajes: list[dict] = [{"role": "system", "content": instrucciones()}] + historial[-config.MAX_HISTORIAL:]

    for paso in range(config.MAX_PASOS + 1):
        agotado = paso == config.MAX_PASOS or time.monotonic() - t0 > config.PRESUPUESTO_S
        if agotado:
            # Última vuelta sin herramientas: que responda con lo que tiene
            resultado.aviso = "Se alcanzó el límite de pasos o de tiempo; la respuesta usa lo obtenido hasta entonces."
        r = _llamar_modelo(c, mensajes, con_herramientas=not agotado)
        resultado.uso.sumar(r.usage)
        msg = r.choices[0].message

        if not msg.tool_calls or agotado:
            resultado.respuesta = (msg.content or "").strip() or "No he podido generar una respuesta."
            break

        mensajes.append({
            "role": "assistant",
            "content": msg.content or "",
            "tool_calls": [{"id": tc.id, "type": "function",
                            "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
                           for tc in msg.tool_calls],
        })
        for tc in msg.tool_calls:
            p = ejecutar(tc.function.name, tc.function.arguments)
            resultado.pasos.append(p)
            contenido = p["resultado"] if p.get("ok") else {"error": p["error"]}
            mensajes.append({"role": "tool", "tool_call_id": tc.id,
                             "content": json.dumps(contenido, ensure_ascii=False, default=str)})

    resultado.ms = round((time.monotonic() - t0) * 1000, 1)
    return resultado
