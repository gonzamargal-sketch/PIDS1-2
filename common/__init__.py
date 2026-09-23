"""
PIDS Parte 2 — Código común a todos los componentes.

    config      Variables de entorno en un único sitio
    esquema     Lectura y normalización del dataset (contrato de datos)
    validacion  Reglas de calidad con severidad rechazo/aviso
    lakehouse   Tabla Iceberg: esquema, partición y compresión

`lakehouse` no se importa aquí a propósito: arrastra PyIceberg, y los
componentes que solo leen el CSV no tienen por qué pagar ese import.

Lo importan el cargador inicial, el simulador, el consumidor de Kafka
y la API. Si algo del formato de origen cambia, se arregla aquí una vez.
"""

from . import config, esquema, validacion   # noqa: F401

__all__ = ["config", "esquema", "validacion"]
__version__ = "1.0"
