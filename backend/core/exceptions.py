"""
Excepciones de dominio del backend.
Los exception handlers en main.py las mapean a respuestas HTTP.
"""


class NotFoundError(Exception):
    """El recurso solicitado no existe (→ HTTP 404)."""


class ConflictError(Exception):
    """El recurso ya existe o hay una violación de unicidad (→ HTTP 409)."""


class BusinessValidationError(Exception):
    """La operación viola una regla de negocio (→ HTTP 422)."""


class DatabaseError(Exception):
    """Error de base de datos no recuperable (→ HTTP 500)."""
