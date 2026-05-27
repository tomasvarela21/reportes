"""
Lógica de negocio para el chequeo de consistencia (stg_mayor_csv_cuenta).
Copiada y adaptada de pages/6_Chequeo_Consistencia.py — no importa desde fuera de /backend.
"""
from __future__ import annotations

import logging
import re
from datetime import date

import pandas as pd

from core.exceptions import BusinessValidationError, DatabaseError
from repositories import consistencia_repository

log = logging.getLogger(__name__)

_TOL = 1.0  # tolerancia monetaria para considerar diferencia

# Mapa empresa_nombre (lowercase) → empresa_id  (igual que página 6)
_EMPRESAS: dict[str, int] = {
    "batia": 1, "norfork": 2, "guare": 3, "torres": 4, "wercolich": 5,
}
_EMPRESAS_INV: dict[int, str] = {v: k.upper() for k, v in _EMPRESAS.items()}
_TODAS_EMPRESAS = set(_EMPRESAS.values())  # {1,2,3,4,5}


# ── Estado del staging ────────────────────────────────────────────────────────

def get_estado(conn) -> dict:
    """Estado actual de stg_mayor_csv_cuenta."""
    rows = consistencia_repository.get_estado(conn)

    empresas = [
        {
            "empresa_id":   r["empresa_id"],
            "periodo_anio": r["periodo_anio"],
            "periodo_mes":  r["periodo_mes"],
            "total_cuentas": r["total_cuentas"],
            "archivo":      r["archivo"],
            "ultima_carga": r["ultima_carga"],
        }
        for r in rows
    ]

    if not empresas:
        return {
            "cargado": False,
            "empresas": [],
            "periodo": None,
            "listo_para_comparar": False,
        }

    # Período predominante (primera fila, ya que deben ser iguales)
    anio = empresas[0]["periodo_anio"]
    mes  = empresas[0]["periodo_mes"]
    periodo_str = f"{anio}/{str(mes).zfill(2)}"

    empresas_ids = {e["empresa_id"] for e in empresas}
    listo        = (
        _TODAS_EMPRESAS.issubset(empresas_ids)
        and len({(e["periodo_anio"], e["periodo_mes"]) for e in empresas}) == 1
    )

    return {
        "cargado": True,
        "empresas": empresas,
        "periodo": periodo_str,
        "listo_para_comparar": listo,
    }


# ── Upload al staging ─────────────────────────────────────────────────────────

def upload_staging(
    conn,
    archivos: list[tuple[bytes, str]],  # [(contenido, nombre_archivo), ...]
) -> dict:
    """
    Parsea los CSVs, valida que sean las 5 empresas en el mismo período,
    TRUNCATE + INSERT en stg_mayor_csv_cuenta.

    archivos: lista de (bytes, nombre_archivo)
    """
    advertencias: list[str] = []
    dfs_validos: list[pd.DataFrame] = []
    metadatos: list[dict] = []  # [{empresa_id, anio, mes, nombre_archivo, cuentas}, ...]

    for contenido, nombre in archivos:
        empresa_id, anio, mes, df_parsed, adv = _parsear_archivo_consistencia(
            contenido, nombre
        )
        advertencias.extend(adv)
        dfs_validos.append(df_parsed)
        metadatos.append({
            "empresa_id": empresa_id,
            "anio": anio,
            "mes":  mes,
            "nombre": nombre,
            "cuentas": len(df_parsed),
        })

    # ── Validar: 5 empresas distintas ────────────────────────────────────────
    empresas_en_archivos = {m["empresa_id"] for m in metadatos}
    faltantes = _TODAS_EMPRESAS - empresas_en_archivos
    if faltantes:
        nombres_faltantes = [_EMPRESAS_INV.get(e, str(e)) for e in sorted(faltantes)]
        raise BusinessValidationError(
            f"Faltan archivos para: {', '.join(nombres_faltantes)}. "
            f"Se necesitan las 5 empresas para la comparación."
        )

    # ── Validar: mismo período ────────────────────────────────────────────────
    periodos = {(m["anio"], m["mes"]) for m in metadatos}
    if len(periodos) > 1:
        raise BusinessValidationError(
            f"Los archivos tienen períodos distintos: {periodos}. "
            f"Todos deben ser del mismo mes."
        )

    anio_final, mes_final = list(periodos)[0]
    periodo_str = f"{anio_final}/{str(mes_final).zfill(2)}"

    # ── TRUNCATE + INSERT ─────────────────────────────────────────────────────
    df_total = pd.concat(dfs_validos, ignore_index=True)

    try:
        consistencia_repository.truncate_staging(conn)

        rows: list[tuple] = [
            (
                str(row["archivo_origen"]),
                int(row["empresa_id"]),
                int(row["periodo_anio"]),
                int(row["periodo_mes"]),
                int(row["cuenta_codigo"]),
                str(row["descripcion"]) if row["descripcion"] else None,
                float(row["saldo_no_ajustado"]),
            )
            for _, row in df_total.iterrows()
        ]

        consistencia_repository.insert_batch(conn, rows)
        conn.commit()

    except Exception as e:
        conn.rollback()
        raise DatabaseError(f"Error al cargar staging: {e}")

    return {
        "empresas_cargadas":  sorted(empresas_en_archivos),
        "periodo":            periodo_str,
        "total_cuentas":      len(df_total),
        "listo_para_comparar": True,
        "advertencias":       advertencias,
    }


# ── Comparación ───────────────────────────────────────────────────────────────

def comparar(conn) -> dict:
    """
    Ejecuta el FULL OUTER JOIN entre stg_mayor_csv_cuenta y libro_mayor.
    Lee el período y empresas directamente del staging actual.
    """
    estado = get_estado(conn)

    if not estado["cargado"]:
        raise BusinessValidationError(
            "El staging está vacío. Cargá los archivos primero."
        )
    if not estado["listo_para_comparar"]:
        empresas_faltantes = _TODAS_EMPRESAS - {
            e["empresa_id"] for e in estado["empresas"]
        }
        nombres = [_EMPRESAS_INV.get(e, str(e)) for e in sorted(empresas_faltantes)]
        raise BusinessValidationError(
            f"Faltan empresas en el staging: {', '.join(nombres)}."
        )

    empresa_ids = [e["empresa_id"] for e in estado["empresas"]]
    anio = estado["empresas"][0]["periodo_anio"]
    mes  = estado["empresas"][0]["periodo_mes"]

    filas = consistencia_repository.comparar(conn, empresa_ids, anio, mes)

    # ── Armar resumen por empresa ─────────────────────────────────────────────
    diferencias_filtradas: list[dict] = []
    resumen_por_empresa: dict[int, dict] = {
        eid: {
            "empresa_id":    eid,
            "empresa_nombre": _EMPRESAS_INV.get(eid),
            "ok":        True,
            "diferencias": 0,
            "solo_csv":  0,
            "solo_db":   0,
        }
        for eid in _TODAS_EMPRESAS
    }

    for f in filas:
        diff = float(f["diferencia"] or 0)
        eid  = f["empresa_id"]
        if abs(diff) > _TOL:
            tipo = f["tipo"]
            diferencias_filtradas.append({
                "empresa_id":     eid,
                "empresa_nombre": f.get("empresa_nombre") or _EMPRESAS_INV.get(eid),
                "cuenta_codigo":  f["cuenta_codigo"],
                "descripcion":    f.get("descripcion"),
                "saldo_csv":      float(f["saldo_csv"]) if f["saldo_csv"] is not None else None,
                "saldo_db":       float(f["saldo_db"])  if f["saldo_db"]  is not None else None,
                "diferencia":     diff,
                "tipo":           tipo,
            })
            if eid in resumen_por_empresa:
                resumen_por_empresa[eid]["ok"] = False
                if tipo == "Diferencia":
                    resumen_por_empresa[eid]["diferencias"] += 1
                elif tipo == "Solo en CSV":
                    resumen_por_empresa[eid]["solo_csv"] += 1
                elif tipo == "Solo en DB":
                    resumen_por_empresa[eid]["solo_db"] += 1

    return {
        "resumen":           list(resumen_por_empresa.values()),
        "diferencias":       diferencias_filtradas,
        "total_diferencias": len(diferencias_filtradas),
        "periodo":           estado["periodo"],
    }


# ── Limpiar staging ───────────────────────────────────────────────────────────

def limpiar_staging(conn) -> None:
    try:
        consistencia_repository.truncate_staging(conn)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise DatabaseError(f"Error al truncar staging: {e}")


# ── Parseo interno ────────────────────────────────────────────────────────────

def _parsear_archivo_consistencia(
    contenido: bytes,
    nombre_archivo: str,
) -> tuple[int, int, int, pd.DataFrame, list[str]]:
    """
    Parsea un CSV del sistema contable para chequeo de consistencia.
    Detecta empresa y período desde el nombre del archivo.
    Retorna: (empresa_id, anio, mes, df, advertencias)
    """
    advertencias: list[str] = []

    # ── Detectar empresa ──────────────────────────────────────────────────────
    empresa_id = _detectar_empresa(nombre_archivo)
    if empresa_id is None:
        raise BusinessValidationError(
            f"No se pudo detectar la empresa en el nombre del archivo '{nombre_archivo}'. "
            f"El nombre debe contener: BATIA, NORFORK, GUARE, TORRES o WERCOLICH."
        )

    # ── Detectar período ──────────────────────────────────────────────────────
    anio, mes = _detectar_periodo(nombre_archivo)
    if anio is None or mes is None:
        raise BusinessValidationError(
            f"No se pudo detectar el período en el nombre del archivo '{nombre_archivo}'. "
            f"Incluí un patrón como '04-01' (mes-año o mes/año) o '2024' en el nombre."
        )

    # ── Parsear CSV ───────────────────────────────────────────────────────────
    import io
    df: pd.DataFrame | None = None
    last_err: Exception | None = None

    for enc in ["latin-1", "utf-8-sig", "utf-8"]:
        try:
            df = pd.read_csv(
                io.BytesIO(contenido),
                sep=";",
                dtype=str,
                encoding=enc,
            ).fillna("")
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            last_err = e

    if df is None:
        raise BusinessValidationError(
            f"No se pudo leer '{nombre_archivo}'. Último error: {last_err}"
        )

    df.columns = [str(c).strip() for c in df.columns]

    # Validar columnas obligatorias
    if "nrocta" not in df.columns:
        raise BusinessValidationError(
            f"El archivo '{nombre_archivo}' no tiene columna 'nrocta'."
        )
    desc_col = "descripcion" if "descripcion" in df.columns else (
        "descrip" if "descrip" in df.columns else None
    )
    if desc_col is None:
        raise BusinessValidationError(
            f"El archivo '{nombre_archivo}' no tiene columna 'descrip' o 'descripcion'."
        )
    if "saldo_no_ajustado" not in df.columns:
        raise BusinessValidationError(
            f"El archivo '{nombre_archivo}' no tiene columna 'saldo_no_ajustado'."
        )

    # Solo cuentas hoja (nrocta no vacío)
    df = df[df["nrocta"].str.strip() != ""].copy()
    df["cuenta_codigo"]     = pd.to_numeric(df["nrocta"], errors="coerce")
    df["saldo_no_ajustado"] = df["saldo_no_ajustado"].apply(_parse_num)
    df["descripcion"]       = df[desc_col].astype(str).str.strip()
    df["empresa_id"]        = empresa_id
    df["periodo_anio"]      = anio
    df["periodo_mes"]       = mes
    df["archivo_origen"]    = nombre_archivo

    df = df[df["cuenta_codigo"].notna()].copy()
    df["cuenta_codigo"] = df["cuenta_codigo"].astype(int)

    n_total = len(df)
    advertencias.append(
        f"{nombre_archivo}: {n_total} cuentas — "
        f"empresa {_EMPRESAS_INV.get(empresa_id, empresa_id)}, "
        f"período {anio}/{str(mes).zfill(2)}"
    )

    cols_out = [
        "archivo_origen", "empresa_id", "periodo_anio", "periodo_mes",
        "cuenta_codigo", "descripcion", "saldo_no_ajustado",
    ]
    return empresa_id, anio, mes, df[cols_out].copy(), advertencias


def _detectar_empresa(nombre: str) -> int | None:
    n = nombre.lower()
    for clave, eid in _EMPRESAS.items():
        if clave in n:
            return eid
    return None


def _detectar_periodo(nombre: str) -> tuple[int | None, int | None]:
    """
    Detecta (anio, mes) desde el nombre del archivo.
    Busca un patrón DD-MM o DD/MM para el mes.
    Para el año: busca 4 dígitos tipo '20XX'; si no encuentra, usa el año actual.
    """
    # Año de 4 dígitos
    year_m = re.search(r"(20\d{2})", nombre)
    anio   = int(year_m.group(1)) if year_m else date.today().year

    # Mes (patrón ##-## o ##/##, toma el segundo grupo como mes)
    m = re.search(r"(\d{2})[-/](\d{2})", nombre)
    if m:
        mes = int(m.group(2))
        if 1 <= mes <= 12:
            return anio, mes

    return None, None


def _parse_num(v: str) -> float:
    s = str(v).strip()
    if not s:
        return 0.0
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0
