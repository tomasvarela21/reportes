from datetime import date
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


class DiarioRow(BaseModel):
    """Fila del libro_diario con datos enriquecidos de dim_cuenta (JOIN)."""

    id: int
    empresa_id: int
    fecha: date | None = None
    periodo_anio: int
    periodo_mes: int
    tipo_asiento: str | None = None
    nro_asiento: str | None = None
    nro_renglon: str | None = None
    cuenta_codigo: int
    cuenta_nombre: str | None = None   # dc.nombre  (JOIN dim_cuenta)
    cuenta_rubro: str | None = None    # dc.rubro   (JOIN dim_cuenta)
    tipo_subcuenta: str | None = None
    nro_subcuenta: str | None = None
    centro_costo: str | None = None
    debe: float
    haber: float
    descripcion: str | None = None
    archivo_origen: str | None = None
