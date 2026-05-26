"""
Lógica de negocio para dim_centro_costo.
Delega SQL a centro_costo_repository.
Lanza excepciones de core/exceptions.py (no HTTPException).
"""
import logging

from backend.core.exceptions import ConflictError, DatabaseError
from backend.repositories import centro_costo_repository
from backend.schemas.centro_costo import CentroCostoCreate

log = logging.getLogger(__name__)


def listar_centros(conn) -> list[dict]:
    """Todos los centros de costo con empresa asociada."""
    return centro_costo_repository.find_all(conn)


def crear_centro(conn, body: CentroCostoCreate) -> dict:
    """
    Alta de centro de costo.
    Lanza ConflictError si el código ya existe.
    """
    if centro_costo_repository.exists(conn, body.codigo):
        raise ConflictError(f"El centro de costo '{body.codigo}' ya existe")

    data = body.model_dump()
    try:
        return centro_costo_repository.create(conn, data)
    except Exception as e:
        conn.rollback()
        raise DatabaseError(f"Error al crear centro de costo: {e}")
