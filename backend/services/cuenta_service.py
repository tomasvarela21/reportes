"""
Lógica de negocio para dim_cuenta.
Delega SQL a cuenta_repository — no contiene queries directas.
Lanza excepciones de core/exceptions.py (no HTTPException).
"""
from __future__ import annotations

import logging
import re

import pandas as pd
import psycopg2

from ..core.db_utils import get_columnas_tabla
from ..core.exceptions import (
    BusinessValidationError,
    ConflictError,
    DatabaseError,
    NotFoundError,
)
from ..repositories import cuenta_repository
from ..schemas.cuenta import CuentaCreate, CuentaUpdate

log = logging.getLogger(__name__)

# Columnas fijas de dim_cuenta (mismas que en el repository)
_COLS_FIJAS_SET = {
    "nro_cta", "extendido", "nombre", "rubro", "sub_rubro", "analisis",
    "fases", "tipo", "moneda", "activa", "es_resultado",
    "nivel_1", "nivel_2", "nivel_3",
}
_COLS_FIJAS_ORD = [
    "nro_cta", "extendido", "nombre", "rubro", "sub_rubro", "analisis",
    "fases", "tipo", "moneda", "activa", "es_resultado",
    "nivel_1", "nivel_2", "nivel_3",
]


# ── Queries (delegadas al repository) ────────────────────────────────────────

def listar_cuentas(conn, *, rubro=None, tipo=None, activa=None,
                   search=None, limit=500, offset=0) -> list[dict]:
    return cuenta_repository.find_all(
        conn,
        {"rubro": rubro, "tipo": tipo, "activa": activa,
         "search": search, "limit": limit, "offset": offset},
    )


def listar_rubros(conn) -> list[str]:
    return cuenta_repository.find_rubros(conn)


def obtener_cuenta(conn, nro_cta: int) -> dict:
    row = cuenta_repository.find_by_id(conn, nro_cta)
    if row is None:
        raise NotFoundError(f"Cuenta {nro_cta} no encontrada")
    return row


def contar_movimientos(conn, nro_cta: int) -> int:
    return cuenta_repository.count_movimientos(conn, nro_cta)


# ── Escritura ─────────────────────────────────────────────────────────────────

def crear_cuenta(conn, body: CuentaCreate) -> dict:
    _verificar_nro_disponible(conn, body.nro_cta)
    _verificar_nombre_disponible(conn, body.nombre)

    data = body.model_dump()
    try:
        return cuenta_repository.create(conn, data)
    except psycopg2.IntegrityError as e:
        conn.rollback()
        raise ConflictError(f"Conflicto al insertar cuenta: {e}")
    except Exception as e:
        conn.rollback()
        raise DatabaseError(f"Error al crear cuenta: {e}")


def actualizar_cuenta(conn, nro_cta: int, body: CuentaUpdate) -> dict:
    obtener_cuenta(conn, nro_cta)  # lanza NotFoundError si no existe

    campos = {k: v for k, v in body.model_dump(exclude_none=True).items()}
    if not campos:
        raise BusinessValidationError("No se enviaron campos a actualizar")

    try:
        row = cuenta_repository.update(conn, nro_cta, campos)
    except Exception as e:
        conn.rollback()
        raise DatabaseError(f"Error al actualizar cuenta: {e}")

    if row is None:
        raise NotFoundError(f"Cuenta {nro_cta} no encontrada")
    return row


def eliminar_cuenta(conn, nro_cta: int) -> None:
    obtener_cuenta(conn, nro_cta)  # lanza NotFoundError si no existe

    n_mov = cuenta_repository.count_movimientos(conn, nro_cta)
    if n_mov > 0:
        raise ConflictError(
            f"La cuenta {nro_cta} tiene {n_mov} movimiento(s) y no puede eliminarse"
        )

    try:
        cuenta_repository.delete(conn, nro_cta)
    except Exception as e:
        conn.rollback()
        raise DatabaseError(f"Error al eliminar cuenta: {e}")


# ── Upload masivo ─────────────────────────────────────────────────────────────

def upsert_plan_desde_archivo(
    conn,
    contenido: bytes,
    nombre_archivo: str,
) -> dict:
    """
    Parsea un CSV/Excel de plan de cuentas y hace upsert masivo en dim_cuenta.

    Lógica:
    1. Parsear el archivo con _parsear_df_plan().
    2. Detectar columnas extra (no en las 14 fijas).
    3. ALTER TABLE dim_cuenta ADD COLUMN IF NOT EXISTS para cada columna nueva.
    4. Calcular nuevas / actualizadas / renombradas.
    5. Upsert batch con ON CONFLICT (nro_cta) DO UPDATE SET.

    Retorna dict con: nuevas, actualizadas, renombradas, cols_agregadas, archivo.
    """
    from ..core.file_utils import parse_bytes_to_df

    df, _fmt = parse_bytes_to_df(contenido, nombre_archivo)
    df, cols_extra_map = _parsear_df_plan(df, nombre_archivo)
    # cols_extra_map: {col_db_name: tipo_sql}

    # ── Agregar columnas nuevas a la tabla ─────────────────────────────────────
    cols_existentes_db = get_columnas_tabla(conn, "dim_cuenta")
    cols_agregadas: list[str] = []

    for col_db, tipo_sql in cols_extra_map.items():
        if col_db not in cols_existentes_db:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"ALTER TABLE dim_cuenta ADD COLUMN IF NOT EXISTS {col_db} {tipo_sql}"
                    )
                conn.commit()
                cols_agregadas.append(col_db)
                log.info("ALTER TABLE dim_cuenta ADD COLUMN %s %s", col_db, tipo_sql)
            except Exception as e:
                conn.rollback()
                raise DatabaseError(f"Error al agregar columna '{col_db}': {e}")

    # ── Calcular estadísticas (antes del upsert) ───────────────────────────────
    existentes_nombres = cuenta_repository.find_nombres_actuales(
        conn, df["nro_cta"].tolist()
    )
    existentes_ids = set(existentes_nombres.keys())
    nros_csv = set(df["nro_cta"].tolist())

    n_nuevas      = len(nros_csv - existentes_ids)
    n_actualizadas = len(nros_csv & existentes_ids)
    n_renombradas = sum(
        1 for nro, nombre_nuevo in zip(df["nro_cta"], df["nombre"])
        if nro in existentes_nombres and existentes_nombres[nro] != nombre_nuevo
    )

    # ── Construir cols y rows para el upsert ──────────────────────────────────
    cols_extra_activas = list(cols_extra_map.keys())
    cols_activas = _COLS_FIJAS_ORD + cols_extra_activas  # nro_cta primero

    rows: list[tuple] = []
    for _, r in df.iterrows():
        fila_fija = (
            int(r["nro_cta"]),
            _sv(r.get("extendido")),
            _sv(r.get("nombre")),
            _sv(r.get("rubro")),
            _sv(r.get("sub_rubro")),
            _sv(r.get("analisis")),
            _sv(r.get("fases")),
            _sv(r.get("tipo")),
            _sv(r.get("moneda")) or "ARS",
            _parse_bool(r.get("activa")),
            _parse_sn(r.get("es_resultado")),
            _parse_int(r.get("nivel_1")),
            _parse_int(r.get("nivel_2")),
            _parse_int(r.get("nivel_3")),
        )
        fila_extra = tuple(_sv(r.get(c)) for c in cols_extra_activas)
        rows.append(fila_fija + fila_extra)

    try:
        cuenta_repository.upsert_batch(conn, cols_activas, rows)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise DatabaseError(f"Error en upsert del plan de cuentas: {e}")

    return {
        "nuevas":        n_nuevas,
        "actualizadas":  n_actualizadas,
        "renombradas":   n_renombradas,
        "cols_agregadas": cols_agregadas,
        "archivo":       nombre_archivo,
    }


# ── Helpers de validación ─────────────────────────────────────────────────────

def _verificar_nro_disponible(conn, nro_cta: int) -> None:
    if cuenta_repository.find_by_id(conn, nro_cta) is not None:
        raise ConflictError(f"El número de cuenta {nro_cta} ya existe")


def _verificar_nombre_disponible(conn, nombre: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM dim_cuenta WHERE LOWER(nombre) = LOWER(%s)", (nombre,)
        )
        if cur.fetchone():
            raise ConflictError(f"Ya existe una cuenta con el nombre '{nombre}'")


# ── Helpers de parseo ─────────────────────────────────────────────────────────

def _parsear_df_plan(df: pd.DataFrame, nombre_archivo: str) -> tuple[pd.DataFrame, dict]:
    """
    Normaliza el DataFrame de plan de cuentas.
    Retorna (df_normalizado, {col_db: tipo_sql}) para columnas extra detectadas.
    """
    # Mapa de aliases → nombre canónico de columna
    _COL_MAP = {
        "nro_cta": "nro_cta", "Nro Cta": "nro_cta", "NroCta": "nro_cta",
        "Extendido": "extendido", "extendido": "extendido",
        "Nombre": "nombre", "nombre": "nombre",
        "Rubro": "rubro", "rubro": "rubro",
        "SubRubro": "sub_rubro", "Sub-rubro": "sub_rubro", "sub_rubro": "sub_rubro",
        "Analisis": "analisis", "analisis": "analisis", "Análisis": "analisis",
        "Fases": "fases", "fases": "fases",
        "Tipo": "tipo", "tipo": "tipo",
        "Moneda": "moneda", "moneda": "moneda",
        "Activa": "activa", "activa": "activa",
        "EsResultado": "es_resultado", "Es Resultado": "es_resultado",
        "es_resultado": "es_resultado",
        "Nivel 1": "nivel_1", "nivel_1": "nivel_1",
        "Nivel 2": "nivel_2", "nivel_2": "nivel_2",
        "Nivel 3": "nivel_3", "nivel_3": "nivel_3",
    }
    df = df.rename(columns={c: _COL_MAP[c] for c in df.columns if c in _COL_MAP})

    if "nro_cta" not in df.columns:
        raise BusinessValidationError(
            f"El archivo '{nombre_archivo}' no contiene columna de número de cuenta"
        )
    if "nombre" not in df.columns:
        raise BusinessValidationError(
            f"El archivo '{nombre_archivo}' no contiene columna 'Nombre'"
        )

    df["nro_cta"] = pd.to_numeric(df["nro_cta"].astype(str).str.strip(), errors="coerce")
    df = df[df["nro_cta"].notna()].copy()
    if df.empty:
        raise BusinessValidationError("No se encontraron filas con nro_cta válido")
    df["nro_cta"] = df["nro_cta"].astype(int)
    df = df.drop_duplicates(subset=["nro_cta"], keep="last")

    # Columnas extra (no fijas)
    cols_extra_archivo = [c for c in df.columns if c not in _COLS_FIJAS_SET]
    cols_extra_map: dict[str, str] = {}  # {col_db: tipo_sql}
    for c in cols_extra_archivo:
        col_db = _normalizar_col(c)
        if col_db and col_db not in _COLS_FIJAS_SET:
            tipo_sql = _inferir_tipo_sql(df[c])
            cols_extra_map[col_db] = tipo_sql
            if col_db != c:
                df = df.rename(columns={c: col_db})

    return df, cols_extra_map


def _normalizar_col(nombre: str) -> str:
    s = nombre.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _inferir_tipo_sql(serie: pd.Series) -> str:
    valores = serie.dropna().astype(str).str.strip()
    valores = valores[valores != ""]
    if valores.empty:
        return "TEXT"
    if set(valores.str.upper().unique()) <= {"S", "N", "SI", "NO", "TRUE", "FALSE", "1", "0"}:
        return "VARCHAR(1)"
    if valores.str.len().max() <= 1:
        return "VARCHAR(1)"
    try:
        valores.astype(int)
        return "INTEGER"
    except (ValueError, TypeError):
        pass
    try:
        valores.str.replace(",", ".").astype(float)
        return "NUMERIC(18,2)"
    except (ValueError, TypeError):
        pass
    return "VARCHAR(100)" if valores.str.len().max() <= 50 else "TEXT"


def _sv(v) -> str | None:
    """String value — None si vacío."""
    if v is None:
        return None
    s = str(v).strip()
    return None if s in ("", "nan", "NaN", "None") else s


def _parse_bool(v) -> bool | None:
    if v is None:
        return None
    return str(v).strip().upper() in ("S", "SI", "TRUE", "1", "YES")


def _parse_sn(v) -> str | None:
    if v is None:
        return None
    return "S" if str(v).strip().upper() in ("S", "SI", "TRUE", "1", "YES") else "N"


def _parse_int(v) -> int | None:
    if v is None:
        return None
    try:
        f = float(str(v).strip().replace(",", "."))
        return int(f)
    except (ValueError, TypeError):
        return None
