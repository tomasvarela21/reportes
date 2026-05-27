from datetime import datetime
from pydantic import BaseModel


class AperturaRow(BaseModel):
    """Fila de saldos_apertura enriquecida con JOINs a dim_empresa y dim_cuenta."""

    empresa_id: int
    empresa_nombre: str | None = None
    anio_fiscal: int
    cuenta_codigo: int
    cuenta_nombre: str | None = None
    tipo_subcuenta: str | None = None
    nro_subcuenta: str | None = None
    centro_costo: str | None = None
    saldo: float
    cargado_en: datetime | None = None
    archivo_origen: str | None = None


class AperturaStats(BaseModel):
    """Estadísticas de verificación de saldos_apertura para empresa + año fiscal."""

    empresa_id: int
    anio_fiscal: int
    total_registros: int
    suma_saldo: float
    cuentas_con_saldo_cero: int
    cuentas_unicas: int
    con_subcuenta: int
    con_ccosto: int


class UploadAperturaResponse(BaseModel):
    """Resultado del endpoint POST /apertura/upload."""

    registros: int
    suma_saldo: float
    empresa_id: int  # empresa principal (o primera en archivo consolidado)
    anio_fiscal: int
    empresas_cargadas: list[int]
    registros_mayor: int
    cuentas_invalidas: list[int]
    advertencias: list[str]
    archivo: str
