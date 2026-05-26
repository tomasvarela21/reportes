from pydantic import BaseModel


class PeriodoInfo(BaseModel):
    anio: int
    mes: int
    existe: bool
    total_registros: int = 0
    total_debe: float = 0.0
    total_haber: float = 0.0
    fecha_carga: str | None = None
    archivo_origen: str | None = None


class ValidateResponse(BaseModel):
    ok: bool
    empresa_nombre: str
    empresa_detectada: str | None
    formato: str
    total_filas_raw: int
    total_filas_validas: int
    errores: list
    advertencias: list[str]
    periodos: list[PeriodoInfo]


class UploadResponse(BaseModel):
    ok: bool
    accion: str
    registros_cargados: int
    registros_mayor: int
    duracion_ms: int
    periodos_cargados: list[list]
    periodos_reemplazados: list[list]
    errores: list[str]


class PeriodoResumen(BaseModel):
    empresa_id: int
    periodo_anio: int
    periodo_mes: int
    total_registros: int
    total_debe: float
    total_haber: float
    ultima_carga: str | None = None
    archivo_origen: str | None = None
