"""
Modelos de respuesta genéricos reutilizables.
"""
from __future__ import annotations

from typing import Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    """Wrapper paginado para cualquier colección de datos."""

    data: list[T]
    total: int
    limit: int
    offset: int
    has_more: bool

    @classmethod
    def build(cls, data: list[T], total: int, limit: int, offset: int) -> "PaginatedResponse[T]":
        return cls(
            data=data,
            total=total,
            limit=limit,
            offset=offset,
            has_more=(offset + limit) < total,
        )


class MessageResponse(BaseModel):
    """Respuesta simple de mensaje para operaciones sin payload."""

    message: str
    success: bool = True
