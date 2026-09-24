"""
PIDS Parte 2 — Código común a todos los componentes.

    config      Variables de entorno en un único sitio
    esquema     Lectura y normalización del dataset (contrato de datos)
    validacion  Reglas de calidad con severidad rechazo/aviso
    lakehouse   Tabla Iceberg: esquema, partición y compresión
    mensajes    Flujo en vivo: formato de mensaje y escritura en caliente/cuarentena

`lakehouse` y `mensajes` no se importan aquí a propósito: el primero
arrastra PyIceberg y el segundo psycopg2, y los
componentes que solo leen el CSV no tienen por qué pagar ese import.

Lo importan el cargador inicial, el simulador, el consumidor de Kafka
y la API. Si algo del formato de origen cambia, se arregla aquí una vez.
"""

from . import config, esquema, validacion   # noqa: F401

__all__ = ["config", "esquema", "validacion"]
__version__ = "1.0"
