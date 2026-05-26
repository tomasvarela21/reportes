from pydantic import BaseModel, ConfigDict


class EmpresaBase(BaseModel):
    empresa_nombre: str
    grupo: str | None = None
    activa: bool = True


class EmpresaCreate(EmpresaBase):
    empresa_id: int


class EmpresaUpdate(BaseModel):
    empresa_nombre: str | None = None
    grupo: str | None = None
    activa: bool | None = None


class EmpresaResponse(EmpresaBase):
    model_config = ConfigDict(from_attributes=True)

    empresa_id: int
