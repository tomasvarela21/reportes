from fastapi import APIRouter, Depends, Query

from database import get_conn
from repositories import mayor_repository
from schemas.mayor import MayorPeriodo, MayorResumenRow, MayorRow, RecalculoLog

router = APIRouter(prefix="/mayor")

_NIVEL_DESC = "'cuenta' | 'subcuenta' | 'centro_costo'"


# ── Rutas estáticas (van antes de /{empresa_id} para evitar ambigüedad) ───────

@router.get("/recalculos", response_model=list[RecalculoLog])
def listar_recalculos(
    limit: int = Query(100, ge=1, le=500, description="Máximo de entradas a devolver"),
    conn=Depends(get_conn),
):
    """
    Devuelve el historial de recálculos del libro_mayor (mayor_recalculo_log),
    con el nombre de empresa, ordenados por fecha descendente.
    """
    return mayor_repository.find_recalculos(conn, limit=limit)


# ── Rutas dinámicas por empresa_id ────────────────────────────────────────────

@router.get("/{empresa_id}/periodos", response_model=list[MayorPeriodo])
def periodos_mayor(empresa_id: int, conn=Depends(get_conn)):
    """
    Devuelve los períodos distintos disponibles en libro_mayor para la empresa.
    Útil para poblar selectores de período en frontends.
    """
    return mayor_repository.find_periodos(conn, empresa_id)


@router.get("/{empresa_id}/resumen", response_model=list[MayorResumenRow])
def resumen_mayor(
    empresa_id: int,
    anio: int | None = Query(None, description="Filtrar por año"),
    mes: int | None  = Query(None, description="Filtrar por mes"),
    conn=Depends(get_conn),
):
    """
    Totales por cuenta (nivel='cuenta') con nombre y rubro de dim_cuenta.
    Si no se filtra por período, suma todos los períodos disponibles.
    """
    filtros = {"anio": anio, "mes": mes}
    return mayor_repository.find_resumen(conn, empresa_id, filtros)


@router.get("/{empresa_id}", response_model=list[MayorRow])
def listar_mayor(
    empresa_id: int,
    anio: int | None          = Query(None, description="Filtrar por año"),
    mes: int | None           = Query(None, description="Filtrar por mes"),
    nivel: str | None         = Query(None, description=_NIVEL_DESC),
    cuenta_codigo: int | None = Query(None, description="Filtrar por número de cuenta"),
    centro_costo: str | None  = Query(None, description="Filtrar por centro de costo"),
    limit: int  = Query(1000, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    conn=Depends(get_conn),
):
    """Devuelve filas del libro_mayor para una empresa, con filtros opcionales."""
    filtros = {
        "anio":          anio,
        "mes":           mes,
        "nivel":         nivel,
        "cuenta_codigo": cuenta_codigo,
        "centro_costo":  centro_costo,
        "limit":         limit,
        "offset":        offset,
    }
    return mayor_repository.find_by_empresa(conn, empresa_id, filtros)
