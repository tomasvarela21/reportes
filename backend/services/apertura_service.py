"""
Lógica de negocio para saldos_apertura.
Copiada y adaptada de pages/_4_Saldos_Apertura.py — no importa desde fuera de /backend.
"""
from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from ..core.exceptions import BusinessValidationError, DatabaseError, NotFoundError
from ..repositories import apertura_repository
from .mayor_calculator import MayorCalculator

log = logging.getLogger(__name__)

# Mapa empresa_nombre (uppercase) → empresa_id
EMPRESAS: dict[str, int] = {
    "BATIA": 1, "NORFORK": 2, "GUARE": 3, "TORRES": 4, "WERCOLICH": 5,
}
_EMPRESAS_INV: dict[int, str] = {v: k for k, v in EMPRESAS.items()}


# ── Consultas ─────────────────────────────────────────────────────────────────

def consultar_apertura(conn, empresa_id: int, filtros: dict) -> tuple[list[dict], int]:
    filas = apertura_repository.find_by_empresa(conn, empresa_id, filtros)
    total = apertura_repository.count_by_empresa(conn, empresa_id, filtros)
    return filas, total


def stats_apertura(conn, empresa_id: int, anio_fiscal: int) -> dict:
    stats = apertura_repository.find_stats(conn, empresa_id, anio_fiscal)
    if not stats or stats.get("total_registros", 0) == 0:
        raise NotFoundError(
            f"No hay saldos de apertura para empresa_id={empresa_id}, año={anio_fiscal}"
        )
    return {**stats, "empresa_id": empresa_id, "anio_fiscal": anio_fiscal}


# ── Upload ────────────────────────────────────────────────────────────────────

def upload_apertura(
    conn,
    contenido: bytes,
    nombre_archivo: str,
    empresa_nombre: str | None,
    anio_fiscal: int,
) -> dict:
    """
    Parsea CSV de saldos de apertura, valida, borra el período existente,
    inserta en batch y dispara recálculo del Mayor.

    empresa_nombre: nombre como "BATIA", "NORFORK", etc.
                    Puede ser None si el CSV incluye columna 'empresa'.
    """
    # ── Determinar empresa_id inicial ─────────────────────────────────────────
    empresa_id_param: int | None = None
    if empresa_nombre:
        clave = empresa_nombre.strip().upper()
        if clave not in EMPRESAS:
            raise BusinessValidationError(
                f"Empresa '{empresa_nombre}' no reconocida. "
                f"Valores válidos: {list(EMPRESAS.keys())}"
            )
        empresa_id_param = EMPRESAS[clave]

    # ── Parsear el CSV ────────────────────────────────────────────────────────
    from ..core.file_utils import parse_bytes_to_df
    df_raw, _fmt = parse_bytes_to_df(contenido, nombre_archivo)

    df, errores, advertencias = _parsear_csv_apertura(df_raw, empresa_id_param)

    if errores:
        raise BusinessValidationError("; ".join(errores))
    if df.empty:
        raise BusinessValidationError("El archivo no contiene registros válidos.")

    # ── Validar cuentas contra dim_cuenta ─────────────────────────────────────
    cuentas_csv   = df["cuenta_codigo"].unique().tolist()
    cuentas_invalidas = _validar_cuentas(conn, cuentas_csv)
    if cuentas_invalidas:
        advertencias.append(
            f"{len(cuentas_invalidas)} cuenta(s) no encontradas en dim_cuenta: "
            f"{sorted(cuentas_invalidas)[:20]}"
        )

    empresas_ids = sorted([int(x) for x in df["empresa_id"].unique().tolist()])
    ahora        = datetime.now()

    # ── DELETE + INSERT + recalculo (todo en una transacción) ─────────────────
    try:
        # DELETE por empresa y año
        for eid in empresas_ids:
            apertura_repository.delete_periodo(conn, eid, anio_fiscal)

        # Construir tuplas para el insert batch
        rows: list[tuple] = [
            (
                int(row["empresa_id"]),
                int(anio_fiscal),
                int(row["cuenta_codigo"]),
                row["tipo_subcuenta"],
                row["nro_subcuenta"],
                row["centro_costo"],
                float(row["saldo"]),
                ahora,
                nombre_archivo,
            )
            for _, row in df.iterrows()
        ]

        apertura_repository.insert_batch(conn, rows)
        conn.commit()

    except Exception as e:
        conn.rollback()
        raise DatabaseError(f"Error al cargar saldos de apertura: {e}")

    # ── Recalcular Mayor por empresa ──────────────────────────────────────────
    registros_mayor = 0
    calc = MayorCalculator(conn)
    for eid in empresas_ids:
        try:
            registros_mayor += calc.recalcular(
                eid, anio_fiscal, 1, motivo="recarga_apertura"
            )
        except Exception as e:
            log.warning("Recálculo falló para empresa_id=%s: %s", eid, e)
            advertencias.append(f"Recálculo del mayor falló para empresa_id={eid}: {e}")

    return {
        "registros":          len(df),
        "suma_saldo":         float(df["saldo"].sum().round(2)),
        "empresa_id":         empresas_ids[0] if len(empresas_ids) == 1 else empresas_ids[0],
        "anio_fiscal":        anio_fiscal,
        "empresas_cargadas":  empresas_ids,
        "registros_mayor":    registros_mayor,
        "cuentas_invalidas":  list(cuentas_invalidas),
        "advertencias":       advertencias,
        "archivo":            nombre_archivo,
    }


# ── Parseo interno ────────────────────────────────────────────────────────────

def _parsear_csv_apertura(
    df_raw: pd.DataFrame,
    empresa_id: int | None,
) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Normaliza el DataFrame de saldos de apertura.
    Soporta dos formatos (adaptado de pages/_4_Saldos_Apertura.py):

    Formato A (sistema original):
        nro_cta;tipo_subcta;nro_subcuenta;ccosto;sdInicial;totalDebe;totalHaber;sdFinal

    Formato B (estándar):
        cuenta_codigo;tipo_subcuenta;nro_subcuenta;centro_costo;saldo
        (puede incluir columna 'empresa' para cargas consolidadas multi-empresa)
    """
    errores: list[str] = []
    advertencias: list[str] = []
    df = df_raw.copy()

    # Normalizar nombres de columna
    df.columns = [
        str(c).strip().lower()
        .replace("ï»¿", "").replace("﻿", "")
        for c in df.columns
    ]

    # ── Detectar Formato A ────────────────────────────────────────────────────
    es_formato_a = "nro_cta" in df.columns and "sdfinal" in df.columns
    if es_formato_a:
        df = df.rename(columns={
            "nro_cta":     "cuenta_codigo",
            "tipo_subcta": "tipo_subcuenta",
            "ccosto":      "centro_costo",
            "sdfinal":     "saldo",
        })
        advertencias.append(
            "Formato A detectado — usando columna `sdFinal` como saldo de apertura."
        )
        # Advertir si sdInicial ≠ sdFinal
        if "sdinicial" in df.columns:
            df["_sdi"] = pd.to_numeric(
                df["sdinicial"].astype(str).str.replace(",", "."), errors="coerce"
            ).fillna(0.0)
            df["_sdf"] = pd.to_numeric(
                df["saldo"].astype(str).str.replace(",", "."), errors="coerce"
            ).fillna(0.0)
            n_diff = (df["_sdi"] != df["_sdf"]).sum()
            if n_diff > 0:
                advertencias.append(
                    f"{n_diff} fila(s) tienen sdFinal ≠ sdInicial — se usa sdFinal."
                )
            df = df.drop(columns=["_sdi", "_sdf"], errors="ignore")

    # ── Validar columnas obligatorias ─────────────────────────────────────────
    if "cuenta_codigo" not in df.columns:
        errores.append("Columna 'cuenta_codigo' (o 'nro_cta') no encontrada.")
        return pd.DataFrame(), errores, advertencias
    if "saldo" not in df.columns:
        errores.append("Columna 'saldo' (o 'sdFinal') no encontrada.")
        return pd.DataFrame(), errores, advertencias

    # ── Asignar empresa_id ────────────────────────────────────────────────────
    if "empresa_id" not in df.columns:
        if "empresa" in df.columns:
            df["empresa_id"] = (
                df["empresa"].astype(str).str.strip().str.upper().map(EMPRESAS)
            )
            inv = df["empresa_id"].isna()
            if inv.any():
                empresas_desc = df[inv]["empresa"].unique().tolist()
                errores.append(
                    f"Empresas desconocidas en columna 'empresa': {empresas_desc}"
                )
                return pd.DataFrame(), errores, advertencias
            df["empresa_id"] = df["empresa_id"].astype(int)
        elif empresa_id is not None:
            df["empresa_id"] = empresa_id
        else:
            errores.append(
                "No se pudo determinar la empresa. "
                "Pasá empresa_nombre en el request o incluí columna 'empresa' en el CSV."
            )
            return pd.DataFrame(), errores, advertencias

    df["empresa_id"] = pd.to_numeric(df["empresa_id"], errors="coerce").astype("Int64")

    # ── cuenta_codigo ─────────────────────────────────────────────────────────
    df["cuenta_codigo"] = pd.to_numeric(
        df["cuenta_codigo"].astype(str).str.strip(), errors="coerce"
    )
    n_inv = df["cuenta_codigo"].isna().sum()
    if n_inv > 0:
        advertencias.append(
            f"{n_inv} fila(s) con cuenta_codigo inválido — serán descartadas."
        )
    df = df[df["cuenta_codigo"].notna() & df["empresa_id"].notna()].copy()
    df["cuenta_codigo"] = df["cuenta_codigo"].astype(int)
    df["empresa_id"]    = df["empresa_id"].astype(int)

    if df.empty:
        errores.append("No hay filas válidas después del filtrado.")
        return pd.DataFrame(), errores, advertencias

    # ── saldo ─────────────────────────────────────────────────────────────────
    df["saldo"] = pd.to_numeric(
        df["saldo"].astype(str).str.strip().str.replace(",", ".", regex=False),
        errors="coerce",
    ).fillna(0.0).round(2)

    # ── subcuenta / ccosto ────────────────────────────────────────────────────
    for col in ["tipo_subcuenta", "nro_subcuenta"]:
        if col in df.columns:
            df[col] = df[col].apply(_limpiar_subcta)
        else:
            df[col] = None

    if "centro_costo" in df.columns:
        df["centro_costo"] = df["centro_costo"].apply(_limpiar_ccosto)
    else:
        df["centro_costo"] = None

    # ── Deduplicar ────────────────────────────────────────────────────────────
    keys = ["empresa_id", "cuenta_codigo", "tipo_subcuenta", "nro_subcuenta", "centro_costo"]
    dupes = df.duplicated(subset=keys, keep=False)
    if dupes.any():
        advertencias.append(
            f"{dupes.sum()} fila(s) duplicadas — se toma la última ocurrencia."
        )
        df = df.drop_duplicates(subset=keys, keep="last")

    cols_out = ["empresa_id", "cuenta_codigo", "tipo_subcuenta",
                "nro_subcuenta", "centro_costo", "saldo"]
    return df[cols_out].copy(), errores, advertencias


def _limpiar_subcta(v) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if s in ("", "nan", "NaN", "0", "0.0"):
        return None
    try:
        f = float(s)
        return None if f == 0 else str(int(f))
    except (ValueError, TypeError):
        return s or None


def _limpiar_ccosto(v) -> str | None:
    return _limpiar_subcta(v)


def _validar_cuentas(conn, cuentas: list[int]) -> set[int]:
    """Retorna el conjunto de cuenta_codigos que NO existen en dim_cuenta."""
    if not cuentas:
        return set()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT nro_cta FROM dim_cuenta WHERE nro_cta = ANY(%s)",
            (cuentas,),
        )
        validas = {r[0] for r in cur.fetchall()}
    return set(cuentas) - validas
