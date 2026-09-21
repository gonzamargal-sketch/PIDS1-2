"""
PIDS Parte 2 — Código común a todos los componentes.

    config      Variables de entorno en un único sitio
    esquema     Lectura y normalización del dataset (contrato de datos)
    validacion  Reglas de calidad con severidad rechazo/aviso

Lo importan el cargador inicial, el simulador, el consumidor de Spark y
la API. Si algo del formato de origen cambia, se arregla aquí una vez.
"""

from . import config, esquema, validacion   # noqa: F401

__all__ = ["config", "esquema", "validacion"]
__version__ = "1.0"
