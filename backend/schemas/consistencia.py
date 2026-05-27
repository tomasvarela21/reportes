from datetime import datetime
from pydantic import BaseModel


class StagingEmpresaInfo(BaseModel):
    """Resumen de una empresa cargada en stg_mayor_csv_cuenta."""

    empresa_id: int
    periodo_anio: int
    periodo_mes: int
    total_cuentas: int
    archivo: str | None = None
    ultima_carga: datetime | None = None


class EstadoResponse(BaseModel):
    """Estado actual de la tabla staging stg_mayor_csv_cuenta."""

    cargado: bool
    empresas: list[StagingEmpresaInfo]
    periodo: str | None = None           # "2024/01" o None si vacío
    listo_para_comparar: bool


class UploadConsistenciaResponse(BaseModel):
    """Resultado del endpoint POST /consistencia/upload."""

    empresas_cargadas: list[int]         # empresa_ids procesados
    periodo: str                         # "YYYY/MM"
    total_cuentas: int
    listo_para_comparar: bool
    advertencias: list[str]


class DiferenciaRow(BaseModel):
    """Fila del resultado de la comparación CSV vs libro_mayor."""

    empresa_id: int
    empresa_nombre: str | None = None
    cuenta_codigo: int
    descripcion: str | None = None
    saldo_csv: float | None = None
    saldo_db: float | None = None
    diferencia: float
    tipo: str    # "Diferencia" | "Solo en CSV" | "Solo en DB"


class ResumenEmpresa(BaseModel):
    """Resumen de diferencias para una empresa."""

    empresa_id: int
    empresa_nombre: str | None = None
    ok: bool
    diferencias: int    # |diff| > TOL, existe en ambos
    solo_csv: int       # existe en CSV pero no en DB
    solo_db: int        # existe en DB pero no en CSV


class CompararResponse(BaseModel):
    """Resultado completo del endpoint POST /consistencia/comparar."""

    resumen: list[ResumenEmpresa]
    diferencias: list[DiferenciaRow]
    total_diferencias: int
    periodo: str
