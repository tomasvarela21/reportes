from fastapi import APIRouter, Depends, Query, UploadFile, File

from backend.database import get_conn
from backend.schemas.proyecto import (
    ProyectoResponse, PresupuestoResponse,
    UploadProyectosResponse, UploadPresupuestosResponse,
)
from backend.services import proyecto_service

router = APIRouter()


# ── Proyectos ─────────────────────────────────────────────────────────────────

@router.get("/proyectos", response_model=list[ProyectoResponse])
def listar_proyectos(
    incluir_inactivos: bool = Query(False, description="Incluir proyectos inactivos"),
    conn=Depends(get_conn),
):
    return proyecto_service.listar_proyectos(conn, incluir_inactivos=incluir_inactivos)


@router.get("/proyectos/{id_origen}", response_model=ProyectoResponse)
def obtener_proyecto(id_origen: int, conn=Depends(get_conn)):
    return proyecto_service.obtener_proyecto(conn, id_origen)


@router.post("/proyectos/upload", response_model=UploadProyectosResponse)
async def upload_proyectos(
    file: UploadFile = File(..., description="CSV de proyectos"),
    conn=Depends(get_conn),
):
    contenido = await file.read()
    result = proyecto_service.upsert_proyectos_desde_csv(conn, contenido, file.filename or "proyectos.csv")
    return result


# ── Presupuestos ──────────────────────────────────────────────────────────────

@router.get("/presupuestos", response_model=list[PresupuestoResponse])
def listar_presupuestos(
    proyecto_id: int | None = Query(None, description="Filtrar por proyecto"),
    conn=Depends(get_conn),
):
    return proyecto_service.listar_presupuestos(conn, proyecto_id=proyecto_id)


@router.post("/presupuestos/upload", response_model=UploadPresupuestosResponse)
async def upload_presupuestos(
    file: UploadFile = File(..., description="CSV de presupuestos"),
    conn=Depends(get_conn),
):
    contenido = await file.read()
    result = proyecto_service.upsert_presupuestos_desde_csv(conn, contenido, file.filename or "presupuestos.csv")
    return result
