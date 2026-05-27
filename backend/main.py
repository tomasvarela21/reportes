import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import logging
import traceback

from fastapi import FastAPI, Request, Security, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader

from config import settings
from core.exceptions import (
    BusinessValidationError,
    ConflictError,
    DatabaseError,
    NotFoundError,
)
from routers import (
    apertura,
    centros_costo,
    consistencia,
    cuentas,
    diario,
    empresas,
    mayor,
    powerbi,
    proyectos,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

app = FastAPI(
    title="ReporteApp API",
    description="API REST para el sistema contable multi-empresa",
    version="2.0.0",
    docs_url="/api/v1/docs",
    redoc_url="/api/v1/redoc",
    openapi_url="/api/v1/openapi.json",
    redirect_slashes=False,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Auth ──────────────────────────────────────────────────────────────────────

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str = Security(_api_key_header)):
    if api_key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API Key inválida o ausente",
        )
    return api_key


# ── Routers ───────────────────────────────────────────────────────────────────

_ROUTER_KWARGS = dict(
    prefix="/api/v1",
    dependencies=[Security(verify_api_key)],
)

app.include_router(empresas.router,      tags=["empresas"],      **_ROUTER_KWARGS)
app.include_router(cuentas.router,       tags=["cuentas"],       **_ROUTER_KWARGS)
app.include_router(centros_costo.router, tags=["centros-costo"], **_ROUTER_KWARGS)
app.include_router(proyectos.router,     tags=["proyectos"],     **_ROUTER_KWARGS)
app.include_router(diario.router,        tags=["diario"],        **_ROUTER_KWARGS)
app.include_router(mayor.router,         tags=["mayor"],         **_ROUTER_KWARGS)
app.include_router(apertura.router,      tags=["apertura"],      **_ROUTER_KWARGS)
app.include_router(consistencia.router,  tags=["consistencia"],  **_ROUTER_KWARGS)
app.include_router(powerbi.router,       tags=["powerbi"],       **_ROUTER_KWARGS)


# ── Exception handlers de dominio ─────────────────────────────────────────────

@app.exception_handler(NotFoundError)
async def not_found_handler(request: Request, exc: NotFoundError):
    return JSONResponse(status_code=404, content={"detail": str(exc)})


@app.exception_handler(ConflictError)
async def conflict_handler(request: Request, exc: ConflictError):
    return JSONResponse(status_code=409, content={"detail": str(exc)})


@app.exception_handler(BusinessValidationError)
async def validation_handler(request: Request, exc: BusinessValidationError):
    return JSONResponse(status_code=422, content={"detail": str(exc)})


@app.exception_handler(DatabaseError)
async def database_error_handler(request: Request, exc: DatabaseError):
    log.error("DatabaseError en %s %s: %s", request.method, request.url, exc)
    return JSONResponse(status_code=500, content={"detail": str(exc)})


# ── Catch-all para excepciones no manejadas ───────────────────────────────────

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    log.error(
        "Excepción no manejada en %s %s:\n%s",
        request.method,
        request.url,
        traceback.format_exc(),
    )
    return JSONResponse(
        status_code=500,
        content={"detail": f"Error interno: {type(exc).__name__}: {exc}"},
    )


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/api/v1/health", tags=["health"])
def health():
    return {"status": "ok", "version": app.version}
