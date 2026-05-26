"""
Parámetros de paginación reutilizables como Depends.
"""
from dataclasses import dataclass

from fastapi import Query


@dataclass
class PaginationParams:
    """
    Inyectable via Depends(PaginationParams).

    Uso en router:
        @router.get("")
        def listar(pagination: PaginationParams = Depends(PaginationParams)):
            ...
    """

    limit: int = Query(100, ge=1, le=1000, description="Máximo de registros a devolver")
    offset: int = Query(0, ge=0, description="Cantidad de registros a saltear")
