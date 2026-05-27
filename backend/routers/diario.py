from fastapi import APIRouter, Depends, Form, Query, UploadFile, File

from core.pagination import PaginationParams
from core.responses import PaginatedResponse
from database import get_conn
from schemas.diario import (
    DiarioRow,
    PeriodoResumen,
    UploadResponse,
    ValidateResponse,
)
from services import diario_service
from services.file_parser import EMPRESAS

router = APIRouter(prefix="/diario")

_EMPRESAS_VALIDAS = list(EMPRESAS.keys())


# ── Upload / Validate ─────────────────────────────────────────────────────────

@router.post("/validate", response_model=ValidateResponse)
async def validate_diario(
    file: UploadFile = File(..., description="CSV del libro diario"),
    empresa_nombre: str = Form(..., description=f"Nombre de empresa: {_EMPRESAS_VALIDAS}"),
    conn=Depends(get_conn),
):
    """
    Parsea y valida un CSV sin escribir nada en la DB.
    Devuelve errores, advertencias y estado de cada período detectado.
    """
    contenido = await file.read()
    return diario_service.validate_csv(
        conn, contenido, file.filename or "diario.csv", empresa_nombre
    )


@router.post("/upload", response_model=UploadResponse)
async def upload_diario(
    file: UploadFile = File(..., description="CSV del libro diario"),
    empresa_nombre: str = Form(..., description=f"Nombre de empresa: {_EMPRESAS_VALIDAS}"),
    periodos_json: str = Form(
        ...,
        description=(
            'JSON con decisión por período. Ej: {"2024/01": true, "2024/02": false}. '
            "true = reemplazar si ya existe."
        ),
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


# ── Consultas ─────────────────────────────────────────────────────────────────

@router.get("/periodos/{empresa_id}", response_model=list[PeriodoResumen])
def periodos_empresa(empresa_id: int, conn=Depends(get_conn)):
    """Períodos cargados en libro_diario para la empresa, con totales por período."""
    return diario_service.listar_periodos(conn, empresa_id)


@router.get("/{empresa_id}", response_model=PaginatedResponse[DiarioRow])
def consultar_diario(
    empresa_id: int,
    anio: int | None           = Query(None, description="Filtrar por año"),
    mes: int | None            = Query(None, description="Filtrar por mes"),
    cuenta_codigo: int | None  = Query(None, description="Filtrar por número de cuenta"),
    centro_costo: str | None   = Query(None, description="Filtrar por centro de costo"),
    descripcion: str | None    = Query(None, description="Búsqueda parcial en descripción (ILIKE)"),
    pagination: PaginationParams = Depends(PaginationParams),
    conn=Depends(get_conn),
):
    """
    Consulta el libro_diario con filtros opcionales.

    Retorna una respuesta paginada (PaginatedResponse) con filas enriquecidas
    del libro_diario (JOIN dim_cuenta para nombre y rubro de la cuenta).
    """
    filtros = {
        "anio":          anio,
        "mes":           mes,
        "cuenta_codigo": cuenta_codigo,
        "centro_costo":  centro_costo,
        "descripcion":   descripcion,
        "limit":         pagination.limit,
        "offset":        pagination.offset,
    }
    filas, total = diario_service.consultar_diario(conn, empresa_id, filtros)
    return PaginatedResponse.build(
        data=filas,
        total=total,
        limit=pagination.limit,
        offset=pagination.offset,
    )
