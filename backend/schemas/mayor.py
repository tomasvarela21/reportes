from datetime import date
from pydantic import BaseModel


class MayorRow(BaseModel):
    # PK: id BIGINT autoincrement
    id: int

    periodo_anio: int
    periodo_mes: int
    fecha_periodo: date | None = None     # columna en producción

    nivel: str                             # 'cuenta' | 'subcuenta' | 'centro_costo'
    cuenta_codigo: int
    tipo_subcuenta: str | None = None
    nro_subcuenta: str | None = None
    centro_costo: str | None = None

    total_debe: float
    total_haber: float
    saldo_anterior: float
    saldo_periodo: float
    saldo_acumulado: float


class MayorResumenRow(BaseModel):
    cuenta_codigo: int
    nombre: str | None = None
    rubro: str | None = None
    tipo: str | None = None
    total_debe: float
    total_haber: float
    saldo_acumulado: float
