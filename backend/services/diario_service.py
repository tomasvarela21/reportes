"""
Lógica de negocio para Libro Diario.
Orquesta FileParser, Validator y StagingService — todo dentro de /backend.
Las queries simples se delegan a diario_repository.
"""
import io
import json
import logging

from fastapi import HTTPException

from repositories import diario_repository
from services.file_parser import FileParser, EMPRESAS
from services.validator import Validator
from services.staging_service import StagingService
from schemas.diario import ValidateResponse, UploadResponse, PeriodoInfo, PeriodoResumen

log = logging.getLogger(__name__)


# ── Consultas ─────────────────────────────────────────────────────────────────

def listar_periodos(conn, empresa_id: int) -> list[PeriodoResumen]:
    """Períodos cargados en libro_diario para la empresa (agrupados con totales)."""
    rows = diario_repository.find_resumen_periodos(conn, empresa_id)
    return [
        PeriodoResumen(
            empresa_id=r["empresa_id"],
            periodo_anio=r["periodo_anio"],
            periodo_mes=r["periodo_mes"],
            total_registros=r["total_registros"],
            total_debe=float(r["total_debe"]),
            total_haber=float(r["total_haber"]),
            ultima_carga=r["ultima_carga"],
            archivo_origen=r["archivo_origen"],
        )
        for r in rows
    ]


def consultar_diario(conn, empresa_id: int, filtros: dict) -> tuple[list[dict], int]:
    """
    Devuelve (filas, total_sin_paginar) del libro_diario con filtros.
    filtros: anio, mes, cuenta_codigo, centro_costo, descripcion, limit, offset.
    """
    filas = diario_repository.find_by_empresa(conn, empresa_id, filtros)
    total = diario_repository.count_by_empresa(conn, empresa_id, filtros)
    return filas, total


# ── Upload / Validate ─────────────────────────────────────────────────────────

def validate_csv(conn, contenido: bytes, nombre_archivo: str, empresa_nombre: str) -> ValidateResponse:
    """Parsea y valida un CSV del diario. No escribe nada en la DB."""
    archivo = _BytesFile(contenido, nombre_archivo)

    parser = FileParser()
    result = parser.parsear(archivo, empresa_nombre)

    if result.ok and result.dataframe is not None:
        validator = Validator(conn)
        errores_val, advertencias_val = validator.validar(result.dataframe, result.empresa_id)
        result.errores      += errores_val
        result.advertencias += advertencias_val
        result.ok = len(result.errores) == 0

    # Calcular períodos del archivo siempre que el dataframe exista,
    # incluso cuando ok=False por errores de validación (cuentas inválidas, etc.).
    # El dataframe es None solo cuando el archivo no pudo parsearse en absoluto.
    periodos_info = []
    if result.dataframe is not None:
        svc = StagingService(conn)
        for pi in svc.verificar_periodos_df(result.dataframe, result.empresa_id):
            periodos_info.append(PeriodoInfo(
                anio=pi.periodo_anio,
                mes=pi.periodo_mes,
                existe=pi.existe,
                total_registros=pi.total_registros,
                total_debe=pi.total_debe,
                total_haber=pi.total_haber,
                fecha_carga=pi.fecha_carga.isoformat() if pi.fecha_carga else None,
                archivo_origen=pi.archivo_origen,
            ))

    return ValidateResponse(
        ok=result.ok,
        empresa_nombre=result.empresa_nombre,
        empresa_detectada=result.empresa_detectada,
        formato=result.formato,
        total_filas_raw=result.total_filas_raw,
        total_filas_validas=result.total_filas_validas,
        errores=result.errores,
        advertencias=result.advertencias,
        periodos=periodos_info,
    )


def upload_csv(
    conn,
    contenido: bytes,
    nombre_archivo: str,
    empresa_nombre: str,
    periodos_json: str,
) -> UploadResponse:
    """
    Parsea, valida y carga el CSV al libro_diario. Recalcula el Mayor.

    periodos_json: JSON string con formato {"2024/01": true, "2024/02": false}
    Clave: "YYYY/MM", valor: true = reemplazar si existe.
    """
    try:
        periodos_raw: dict = json.loads(periodos_json)
    except Exception:
        raise HTTPException(status_code=400, detail="periodos_json no es JSON válido")

    periodos_reemplazar: dict[tuple[int, int], bool] = {}
    for k, v in periodos_raw.items():
        try:
            partes = k.split("/")
            periodos_reemplazar[(int(partes[0]), int(partes[1]))] = bool(v)
        except Exception:
            raise HTTPException(
                status_code=400,
                detail=f"Clave de período inválida: '{k}'. Usar formato YYYY/MM",
            )

    archivo = _BytesFile(contenido, nombre_archivo)
    parser = FileParser()
    result = parser.parsear(archivo, empresa_nombre)

    if not result.ok:
        raise HTTPException(
            status_code=422,
            detail={"errores": result.errores, "advertencias": result.advertencias},
        )

    validator = Validator(conn)
    errores_val, _ = validator.validar(result.dataframe, result.empresa_id)
    if errores_val:
        raise HTTPException(status_code=422, detail={"errores": errores_val})

    svc = StagingService(conn)
    carga = svc.ejecutar_carga_multiperiodo(
        df=result.dataframe,
        empresa_id=result.empresa_id,
        archivo_nombre=nombre_archivo,
        periodos_reemplazar=periodos_reemplazar,
    )

    return UploadResponse(
        ok=carga.ok,
        accion=carga.accion,
        registros_cargados=carga.registros_cargados,
        registros_mayor=carga.registros_mayor,
        duracion_ms=carga.duracion_ms,
        periodos_cargados=carga.periodos_cargados,
        periodos_reemplazados=carga.periodos_reemplazados,
        errores=carga.errores,
    )


# ── Helper interno ────────────────────────────────────────────────────────────

class _BytesFile:
    """Wrapper que hace que bytes parezca un file-like object con .name."""

    def __init__(self, contenido: bytes, nombre: str):
        self._buf = io.BytesIO(contenido)
        self.name = nombre

    def read(self, *args):
        return self._buf.read(*args)

    def seek(self, *args):
        return self._buf.seek(*args)
