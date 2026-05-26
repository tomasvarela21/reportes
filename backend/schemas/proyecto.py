from datetime import date, datetime
from pydantic import BaseModel, ConfigDict


class ProyectoResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    # PK: ccosto VARCHAR NOT NULL
    ccosto: str
    nombre: str

    # Identificadores externos
    id_origen: int | None = None
    oportunidad_id: int | None = None
    responsable_id: int | None = None
    version: int | None = None

    # Fechas
    fc_inicio: date | None = None
    fc_fin: date | None = None
    deleted_at: date | None = None
    actualizado_en: datetime | None = None

    # Estado y descripción
    estado: str | None = None
    comentario: str | None = None
    activo: bool | None = None

    # Financieros
    ingresos: float | None = None
    cto_mo_propia: float | None = None
    cto_mo_terceros: float | None = None
    cto_materiales: float | None = None
    cto_herramientas: float | None = None
    cto_diversos: int | None = None       # INTEGER en producción

    # Métricas
    superficie: float | None = None
    avance: float | None = None           # NUMERIC default 0
    horas: float | None = None            # NUMERIC default 0


class PresupuestoResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    # PK: id INTEGER NOT NULL
    id: int
    proyecto_id: int

    fecha: date | None = None
    mo_propia: float | None = None
    mo_terceros: float | None = None
    materiales: float | None = None
    herramientas: float | None = None
    horas: float | None = None
    metros: float | None = None
    importe: float | None = None
    descripcion: str | None = None
    cargado_en: datetime | None = None    # columna de auditoría


class UploadProyectosResponse(BaseModel):
    nuevos: int
    actualizados: int
    inactivos: int
    archivo: str


class UploadPresupuestosResponse(BaseModel):
    nuevos: int
    actualizados: int
    archivo: str
