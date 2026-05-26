from fastapi import APIRouter, Depends

from backend.database import get_conn
from backend.schemas.centro_costo import CentroCostoCreate, CentroCostoResponse
from backend.services import centro_costo_service

router = APIRouter(prefix="/centros-costo")


@router.get("", response_model=list[CentroCostoResponse])
def listar_centros(conn=Depends(get_conn)):
    """Lista todos los centros de costo con el nombre de empresa asociada."""
    return centro_costo_service.listar_centros(conn)


@router.post("", response_model=CentroCostoResponse, status_code=201)
def crear_centro(body: CentroCostoCreate, conn=Depends(get_conn)):
    """Crea un nuevo centro de costo. Rechaza si el código ya existe."""
    return centro_costo_service.crear_centro(conn, body)
