from fastapi import APIRouter, Depends, Query

from backend.database import get_conn
from backend.schemas.cuenta import CuentaCreate, CuentaResponse, CuentaUpdate
from backend.services import cuenta_service

router = APIRouter(prefix="/cuentas")


@router.get("/rubros", response_model=list[str])
def listar_rubros(conn=Depends(get_conn)):
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
    return cuenta_service.listar_cuentas(
        conn,
        rubro=rubro,
        tipo=tipo,
        activa=activa,
        search=search,
        limit=limit,
        offset=offset,
    )


@router.get("/{nro_cta}", response_model=CuentaResponse)
def obtener_cuenta(nro_cta: int, conn=Depends(get_conn)):
    return cuenta_service.obtener_cuenta(conn, nro_cta)


@router.post("", response_model=CuentaResponse, status_code=201)
def crear_cuenta(body: CuentaCreate, conn=Depends(get_conn)):
    return cuenta_service.crear_cuenta(conn, body)


@router.patch("/{nro_cta}", response_model=CuentaResponse)
def actualizar_cuenta(nro_cta: int, body: CuentaUpdate, conn=Depends(get_conn)):
    return cuenta_service.actualizar_cuenta(conn, nro_cta, body)


@router.delete("/{nro_cta}", status_code=204)
def eliminar_cuenta(nro_cta: int, conn=Depends(get_conn)):
    cuenta_service.eliminar_cuenta(conn, nro_cta)
