import logging
import traceback
from fastapi import FastAPI, Request, Security, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader

from backend.config import settings
from backend.routers import empresas, cuentas, proyectos, diario, mayor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

app = FastAPI(
    title="ReporteApp API",
    description="API REST para el sistema contable multi-empresa",
    version="1.0.0",
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

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str = Security(_api_key_header)):
    if api_key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API Key inválida o ausente",
        )
    return api_key


app.include_router(
    empresas.router,
    prefix="/api/v1",
    tags=["empresas"],
    dependencies=[Security(verify_api_key)],
)
app.include_router(
    cuentas.router,
    prefix="/api/v1",
    tags=["cuentas"],
    dependencies=[Security(verify_api_key)],
)
app.include_router(
    proyectos.router,
    prefix="/api/v1",
    tags=["proyectos"],
    dependencies=[Security(verify_api_key)],
)
app.include_router(
    diario.router,
    prefix="/api/v1",
    tags=["diario"],
    dependencies=[Security(verify_api_key)],
)
app.include_router(
    mayor.router,
    prefix="/api/v1",
    tags=["mayor"],
    dependencies=[Security(verify_api_key)],
)


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


@app.get("/api/v1/health", tags=["health"])
def health():
    return {"status": "ok"}
