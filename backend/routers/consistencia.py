from fastapi import APIRouter, Depends, File, UploadFile

from backend.core.responses import MessageResponse
from backend.database import get_conn
from backend.schemas.consistencia import (
    CompararResponse,
    EstadoResponse,
    UploadConsistenciaResponse,
)
from backend.services import consistencia_service

router = APIRouter(prefix="/consistencia")


@router.post("/upload", response_model=UploadConsistenciaResponse, status_code=201)
async def upload_consistencia(
    files: list[UploadFile] = File(
        ...,
        description=(
            "CSV(s) del sistema contable — uno por empresa. "
            "El nombre del archivo debe incluir el nombre de la empresa "
            "(BATIA, NORFORK, GUARE, TORRES, WERCOLICH) y el período (ej: 04-01 = mes 01)."
        ),
    ),
    conn=Depends(get_conn),
):
    """
    Carga archivos CSV del sistema contable en la tabla staging stg_mayor_csv_cuenta.

    - Detecta empresa y período desde el nombre de cada archivo.
    - Valida que estén presentes las 5 empresas en el mismo período.
    - Ejecuta TRUNCATE + INSERT (reemplaza el staging completo).
    """
    archivos: list[tuple[bytes, str]] = []
    for f in files:
        contenido = await f.read()
        archivos.append((contenido, f.filename or f"archivo_{len(archivos)}.csv"))

    resultado = consistencia_service.upload_staging(conn, archivos)
    return UploadConsistenciaResponse(**resultado)


@router.get("/estado", response_model=EstadoResponse)
def estado_staging(conn=Depends(get_conn)):
    """
    Devuelve el estado actual de la tabla staging stg_mayor_csv_cuenta:
    qué empresas y períodos están cargados, y si está listo para comparar.
    """
    estado = consistencia_service.get_estado(conn)
    return EstadoResponse(**estado)


@router.post("/comparar", response_model=CompararResponse)
def comparar_consistencia(conn=Depends(get_conn)):
    """
    Ejecuta la comparación entre el staging (CSV del sistema) y el libro_mayor (DB).

    - FULL OUTER JOIN stg_mayor_csv_cuenta vs libro_mayor nivel='cuenta'.
    - Tolerancia: $1.
    - Clasifica cada diferencia en: "Diferencia" | "Solo en CSV" | "Solo en DB".
    - Requiere que el staging tenga las 5 empresas del mismo período.
    """
    resultado = consistencia_service.comparar(conn)
    return CompararResponse(**resultado)


@router.delete("/staging", response_model=MessageResponse)
def limpiar_staging(conn=Depends(get_conn)):
    """Vacía la tabla stg_mayor_csv_cuenta (TRUNCATE)."""
    consistencia_service.limpiar_staging(conn)
    return MessageResponse(message="Staging vaciado correctamente.")
