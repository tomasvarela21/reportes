"""
Helpers para lectura de archivos subidos via UploadFile.
Auto-detecta encoding y separador; soporta CSV y Excel.
"""
from __future__ import annotations

import io
import logging

import pandas as pd
from fastapi import UploadFile, HTTPException

log = logging.getLogger(__name__)

_ENCODINGS = ["latin-1", "utf-8-sig", "utf-8"]
_SEPS = [";", ","]


async def read_upload_file(file: UploadFile) -> tuple[pd.DataFrame, str]:
    """
    Lee un UploadFile y lo convierte en DataFrame.

    Retorna: (df, formato_detectado)
    Formatos posibles: "excel", "csv_semicolon", "csv_comma"

    Lanza HTTPException 400 si no puede leer el archivo con ningún encoding/sep.
    """
    contenido = await file.read()
    nombre = file.filename or "upload"
    df, fmt = _parse_bytes(contenido, nombre)
    return df, fmt


def parse_bytes_to_df(contenido: bytes, nombre_archivo: str) -> tuple[pd.DataFrame, str]:
    """
    Versión síncrona para contextos que ya tienen bytes (ej. tests, servicios).
    Retorna: (df, formato_detectado)
    """
    return _parse_bytes(contenido, nombre_archivo)


# ── Internal ──────────────────────────────────────────────────────────────────

def _parse_bytes(contenido: bytes, nombre: str) -> tuple[pd.DataFrame, str]:
    nombre_lower = nombre.lower()

    # ── Excel ──────────────────────────────────────────────────────────────────
    if nombre_lower.endswith((".xlsx", ".xls")):
        try:
            df = pd.read_excel(io.BytesIO(contenido), dtype=str)
            df.columns = [str(c).strip() for c in df.columns]
            return df.fillna(""), "excel"
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"No se pudo leer el archivo Excel: {e}")

    # ── CSV — probar combinaciones sep × encoding ──────────────────────────────
    last_error: Exception | None = None
    for sep in _SEPS:
        for enc in _ENCODINGS:
            try:
                df = pd.read_csv(
                    io.BytesIO(contenido),
                    sep=sep,
                    dtype=str,
                    encoding=enc,
                    keep_default_na=False,
                )
                if df.empty or len(df.columns) < 2:
                    # Probable wrong separator — try next
                    continue
                df.columns = [str(c).strip() for c in df.columns]
                fmt = "csv_semicolon" if sep == ";" else "csv_comma"
                return df.fillna(""), fmt
            except UnicodeDecodeError:
                continue
            except Exception as e:
                last_error = e
                continue

    raise HTTPException(
        status_code=400,
        detail=f"No se pudo leer el archivo con ningún encoding/separador conocido. "
               f"Último error: {last_error}",
    )
