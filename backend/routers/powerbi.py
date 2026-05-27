from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from backend.database import get_conn
from backend.schemas.powerbi import RefreshResponse
from backend.services import powerbi_service

router = APIRouter(prefix="/powerbi")


@router.post("/refresh", response_model=RefreshResponse)
def refresh_powerbi(conn=Depends(get_conn)):
    """
    Dispara el refresh del dataset de Power BI Service.

    - Requiere que las variables POWERBI_* estén definidas en .env.
    - Si alguna variable falta, devuelve 503 con un mensaje descriptivo.
    - Si hay un refresh en curso, devuelve 200 con ok=false y un mensaje explicativo.
    - En éxito: devuelve 200 con ok=true (Power BI acepta el refresh con HTTP 202).
    """
    resultado = powerbi_service.trigger_refresh()

    # Si faltan variables de configuración → 503 Service Unavailable
    if not resultado["ok"] and resultado.get("error", "").startswith("Variables de entorno"):
        return JSONResponse(
            status_code=503,
            content={
                "ok":    False,
                "error": resultado["error"],
            },
        )

    return RefreshResponse(**resultado)
