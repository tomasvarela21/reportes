from pydantic import BaseModel, ConfigDict, field_validator
from typing import Literal


TipoCuenta = Literal["Activo", "Pasivo", "Patrimonio", "Resultado"]
Moneda = Literal["ARS", "USD", "EUR"]


class CuentaBase(BaseModel):
    nro_cta: int
    extendido: str | None = None
    nombre: str
    rubro: str | None = None
    sub_rubro: str | None = None
    analisis: str | None = None
    fases: str | None = None
    tipo: TipoCuenta
    moneda: Moneda = "ARS"
    activa: bool = True
    es_resultado: Literal["S", "N"] | None = None
    nivel_1: int | None = None
    nivel_2: int | None = None
    nivel_3: int | None = None


class CuentaCreate(CuentaBase):
    @field_validator("nombre")
    @classmethod
    def nombre_no_vacio(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("El nombre no puede estar vacío")
        return v.strip()


class CuentaUpdate(BaseModel):
    extendido: str | None = None
    nombre: str | None = None
    rubro: str | None = None
    sub_rubro: str | None = None
    analisis: str | None = None
    fases: str | None = None
    tipo: TipoCuenta | None = None
    moneda: Moneda | None = None
    activa: bool | None = None
    es_resultado: Literal["S", "N"] | None = None
    nivel_1: int | None = None
    nivel_2: int | None = None
    nivel_3: int | None = None


class CuentaResponse(CuentaBase):
    model_config = ConfigDict(from_attributes=True)


class CuentaFiltros(BaseModel):
    rubro: str | None = None
    tipo: TipoCuenta | None = None
    activa: bool | None = None
    search: str | None = None
