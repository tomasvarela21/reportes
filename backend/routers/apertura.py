from fastapi import APIRouter, Depends, File, Form, Query, UploadFile

from ..core.pagination import PaginationParams
from ..core.responses import PaginatedResponse
from ..database import get_conn
from ..schemas.apertura import AperturaRow, AperturaStats, UploadAperturaResponse
from ..services import apertura_service

router = APIRouter(prefix="/apertura")


@router.post("/upload", response_model=UploadAperturaResponse, status_code=201)
async def upload_apertura(
    file: UploadFile = File(..., description="CSV de saldos de apertura (sep=;)"),
    empresa_nombre: str | None = Form(
        None,
        description=(
            "Nombre de empresa (BATIA, NORFORK, GUARE, TORRES, WERCOLICH). "
            "Opcional si el CSV incluye columna 'empresa'."
        ),
    ),
    anio_fiscal: int = Form(..., description="Año fiscal (ej: 2024)"),
    conn=Depends(get_conn),
):
    """
    Carga saldos de apertura desde un CSV.

    Lógica:
    1. Parsea el CSV (Formato A sistema original o Formato B estándar).
    2. Valida cuentas contra dim_cuenta.
    3. Elimina los saldos existentes del año fiscal para cada empresa del archivo.
    4. Inserta los nuevos saldos en batch.
    5. Dispara recálculo del Libro Mayor desde (anio_fiscal, mes=1).
    """
    contenido = await file.read()
    resultado = apertura_service.upload_apertura(
        conn,
        contenido,
        file.filename or "apertura.csv",
        empresa_nombre,
        anio_fiscal,
    )
    return UploadAperturaResponse(**resultado)


@router.get("/{empresa_id}", response_model=PaginatedResponse[AperturaRow])
def consultar_apertura(
    empresa_id: int,
    anio_fiscal: int | None    = Query(None, description="Filtrar por año fiscal"),
    cuenta_codigo: int | None  = Query(None, description="Filtrar por cuenta"),
    centro_costo: str | None   = Query(None, description="Filtrar por centro de costo"),
    solo_saldo_nonzero: bool   = Query(False, description="Solo registros con saldo ≠ 0"),
    pagination: PaginationParams = Depends(PaginationParams),
    conn=Depends(get_conn),
):
    """
    Consulta saldos de apertura para una empresa con filtros opcionales.
    Respuesta paginada con datos de dim_cuenta y dim_empresa.
    """
    filtros = {
        "anio_fiscal":        anio_fiscal,
        "cuenta_codigo":      cuenta_codigo,
        "centro_costo":       centro_costo,
        "solo_saldo_nonzero": solo_saldo_nonzero,
        "limit":              pagination.limit,
        "offset":             pagination.offset,
    }
    filas, total = apertura_service.consultar_apertura(conn, empresa_id, filtros)
    return PaginatedResponse.build(
        data=filas,
        total=total,
        limit=pagination.limit,
        offset=pagination.offset,
    )


@router.get("/{empresa_id}/{anio_fiscal}/stats", response_model=AperturaStats)
def stats_apertura(empresa_id: int, anio_fiscal: int, conn=Depends(get_conn)):
    """
    Estadísticas de verificación de los saldos de apertura cargados:
    total registros, suma saldo, cuentas con saldo cero, con subcuenta, con ccosto.
    """
    stats = apertura_service.stats_apertura(conn, empresa_id, anio_fiscal)
    return AperturaStats(**stats)
