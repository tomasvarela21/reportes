from fastapi import APIRouter, Depends, Form, UploadFile, File

from backend.database import get_conn
from backend.schemas.diario import ValidateResponse, UploadResponse, PeriodoResumen
from backend.services import diario_service
from backend.services.file_parser import EMPRESAS

router = APIRouter(prefix="/diario")

_EMPRESAS_VALIDAS = list(EMPRESAS.keys())


@router.post("/validate", response_model=ValidateResponse)
async def validate_diario(
    file: UploadFile = File(..., description="CSV del libro diario"),
    empresa_nombre: str = Form(..., description=f"Nombre de empresa: {list(EMPRESAS.keys())}"),
    conn=Depends(get_conn),
):
    """
    Parsea y valida un CSV sin escribir nada en la DB.
    Devuelve errores, advertencias y estado de cada período detectado.
    """
    contenido = await file.read()
    return diario_service.validate_csv(conn, contenido, file.filename or "diario.csv", empresa_nombre)


@router.post("/upload", response_model=UploadResponse)
async def upload_diario(
    file: UploadFile = File(..., description="CSV del libro diario"),
    empresa_nombre: str = Form(..., description=f"Nombre de empresa: {list(EMPRESAS.keys())}"),
    periodos_json: str = Form(
        ...,
        description='JSON con decisión por período. Ej: {"2024/01": true, "2024/02": false}. '
                    'true = reemplazar si ya existe.',
    ),
    conn=Depends(get_conn),
):
    """
    Carga el CSV al libro_diario y recalcula el Mayor.
    Requiere que todos los períodos que se van a cargar estén declarados en periodos_json.
    """
    contenido = await file.read()
    return diario_service.upload_csv(
        conn, contenido, file.filename or "diario.csv", empresa_nombre, periodos_json
    )


@router.get("/periodos/{empresa_id}", response_model=list[PeriodoResumen])
def periodos_empresa(empresa_id: int, conn=Depends(get_conn)):
    """Devuelve todos los períodos cargados en libro_diario para la empresa indicada."""
    return diario_service.listar_periodos(conn, empresa_id)
