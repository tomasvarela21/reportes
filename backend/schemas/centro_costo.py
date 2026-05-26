from pydantic import BaseModel, ConfigDict, field_validator


class CentroCostoBase(BaseModel):
    codigo: str
    descripcion: str | None = None
    empresa_id: int | None = None


class CentroCostoCreate(CentroCostoBase):
    @field_validator("codigo")
    @classmethod
    def codigo_no_vacio(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("El código no puede estar vacío")
        return v


class CentroCostoResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    codigo: str
    descripcion: str | None = None
    empresa_nombre: str | None = None  # resultado del JOIN con dim_empresa
    activo: bool | None = None
