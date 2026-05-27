from pydantic import BaseModel


class RefreshResponse(BaseModel):
    """Respuesta del endpoint POST /powerbi/refresh."""

    ok: bool
    message: str | None = None
    error: str | None = None
