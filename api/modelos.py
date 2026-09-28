"""
PIDS Parte 2 — API · Modelos de datos (P3, T3.1).

El modelo de respuesta de §6 de ARQUITECTURA.md: toda respuesta que toca
datos lleva `data` + `meta`, con `data_source`, `coverage`, `as_of`,
`latency_ms` y `rows`. Es el contrato que exige E8 (de dónde viene cada
dato) y en el que se apoya /metrics y /stats también.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

TierNombre = Literal["hot", "cold"]
DataSource = Literal["hot", "cold", "mixto"]


class TramoCoverage(BaseModel):
    """Un tramo temporal servido desde un tier concreto."""
    desde: datetime
    hasta: datetime
    tier: TierNombre


class Meta(BaseModel):
    """Metadatos que acompañan a toda respuesta que toca datos (§6)."""
    data_source: DataSource
    coverage: list[TramoCoverage] = Field(default_factory=list)
    as_of: datetime
    latency_ms: float
    rows: int


class RespuestaDatos(BaseModel):
    """Envoltorio genérico {data, meta} de §6."""
    data: Any
    meta: Meta


class PoliticaRetencion(BaseModel):
    """Fila de retention_policy, tal cual la expone /lifecycle/policy."""
    id: int
    dataset: str
    tier_origen: str
    tier_destino: str | None
    umbral_valor: int
    umbral_unidad: Literal["minutes", "hours", "days", "years"]
    accion: Literal["ARCHIVE", "DELETE"]
    activa: bool
    descripcion: str | None
    actualizado_en: datetime


class PoliticaUpdate(BaseModel):
    """Body del PUT /lifecycle/policy.

    Solo se toca el umbral: es lo que se cambia en directo en el vídeo
    (§10, minuto 2:30) para disparar el archivado sin redesplegar nada.
    La validación de la unidad refleja el CHECK de la tabla en
    postgres/init/01_esquema.sql.
    """
    umbral_valor: int = Field(gt=0)
    umbral_unidad: Literal["minutes", "hours", "days", "years"]
