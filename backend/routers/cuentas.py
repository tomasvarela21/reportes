from fastapi import APIRouter, Depends, Query, UploadFile, File

from backend.database import get_conn
from backend.schemas.cuenta import (
    CuentaCreate,
    CuentaResponse,
    CuentaUpdate,
    MovimientosResponse,
    UploadCuentasResponse,
)
from backend.services import cuenta_service

router = APIRouter(prefix="/cuentas")


# ── Colección ─────────────────────────────────────────────────────────────────

@router.get("/rubros", response_model=list[str])
def listar_rubros(conn=Depends(get_conn)):
    """Lista los rubros distintos del plan de cuentas."""
    return cuenta_service.listar_rubros(conn)


@router.get("", response_model=list[CuentaResponse])
def listar_cuentas(
    rubro: str | None = Query(None, description="Filtrar por rubro"),
    tipo: str | None = Query(None, description="Activo | Pasivo | Patrimonio | Resultado"),
    activa: bool | None = Query(None, description="Filtrar por estado activo"),
    search: str | None = Query(None, description="Buscar en nombre o nro_cta"),
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    conn=Depends(get_conn),
):
    """Lista cuentas del plan con filtros opcionales."""
    return cuenta_service.listar_cuentas(
        conn,
        rubro=rubro,
        tipo=tipo,
        activa=activa,
        search=search,
        limit=limit,
        offset=offset,
    )


# ── Upload masivo (antes del /{nro_cta} para evitar conflicto de ruta) ────────

@router.post("/upload", response_model=UploadCuentasResponse, status_code=201)
async def upload_plan_cuentas(
    file: UploadFile = File(..., description="CSV (sep=;) o Excel con el plan de cuentas"),
    conn=Depends(get_conn),
):
    """
    Upsert masivo del plan de cuentas desde un archivo CSV o Excel.

    - Detecta columnas extra y ejecuta ALTER TABLE dim_cuenta automáticamente.
    - ON CONFLICT (nro_cta) DO UPDATE SET — no elimina cuentas existentes.
    - Devuelve conteos de nuevas, actualizadas, renombradas y columnas agregadas.
    """
    contenido = await file.read()
    resultado = cuenta_service.upsert_plan_desde_archivo(
        conn, contenido, file.filename or "plan_cuentas.csv"
    )
    return UploadCuentasResponse(**resultado)


# ── Recurso individual ────────────────────────────────────────────────────────

@router.get("/{nro_cta}/movimientos", response_model=MovimientosResponse)
def movimientos_cuenta(nro_cta: int, conn=Depends(get_conn)):
    """
    Devuelve la cantidad de movimientos en libro_diario para el nro_cta indicado.
    Útil para verificar antes de intentar DELETE.
    """
    cuenta_service.obtener_cuenta(conn, nro_cta)  # lanza 404 si no existe
    n = cuenta_service.contar_movimientos(conn, nro_cta)
    return MovimientosResponse(nro_cta=nro_cta, movimientos=n)


@router.get("/{nro_cta}", response_model=CuentaResponse)
def obtener_cuenta(nro_cta: int, conn=Depends(get_conn)):
    """Devuelve una cuenta por su número."""
    return cuenta_service.obtener_cuenta(conn, nro_cta)


@router.post("", response_model=CuentaResponse, status_code=201)
def crear_cuenta(body: CuentaCreate, conn=Depends(get_conn)):
    """Crea una cuenta nueva en el plan de cuentas."""
    return cuenta_service.crear_cuenta(conn, body)


@router.put("/{nro_cta}", response_model=CuentaResponse)
def actualizar_cuenta(nro_cta: int, body: CuentaUpdate, conn=Depends(get_conn)):
    """
    Actualiza una cuenta existente.
    Solo se actualizan los campos enviados (campos omitidos o null se ignoran).
    """
    return cuenta_service.actualizar_cuenta(conn, nro_cta, body)


@router.delete("/{nro_cta}", status_code=204)
def eliminar_cuenta(nro_cta: int, conn=Depends(get_conn)):
    """
    Elimina una cuenta del plan de cuentas.
    Rechaza con 409 si la cuenta tiene movimientos en el libro diario.
    """
    cuenta_service.eliminar_cuenta(conn, nro_cta)
