"""
file_parser.py
==============
Parser de archivos CSV del Libro Diario para ReporteApp v2.

Soporta dos formatos:

FORMATO A — 22 columnas (posicional, sin encabezado empresa):
  0  Fecasi, 1 tipo_asiento, 2 nro_asiento, 3 Nro_renglon, 4 referencia,
  5  descrip_movs, 6 nro_cta, 7 desc_pdc, 8 subs_descrip, 9 tipo_subcta,
  10 nro_subcuenta, 11-18 descartados, 19 debe, 20 haber, 21 ccosto
  → empresa se toma del selector

FORMATO B — 17 columnas con encabezado (formato exportación sistema):
  id_empresa, fecasi, tipo_asiento, nro_asiento, Nro_renglon, referencia,
  nro_cta, descrip_movs, descrip_asis, nombre, debe, haber, feccar,
  tipo_subcta, nro_subcuenta, subs_descrip, ccosto
  → empresa_id se toma de id_empresa (columna del archivo)
"""

import io
import logging
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd

log = logging.getLogger(__name__)

EMPRESAS = {
    'BATIA':     1,
    'GUARE':     3,
    'NORFORK':   2,
    'TORRES':    4,
    'WERCOLICH': 5,
}

COLS_FORMATO_A = 22
COLS_FORMATO_B = 17
COLS_FORMATO_B_NAMES = [
    'id_empresa', 'fecasi', 'tipo_asiento', 'nro_asiento', 'Nro_renglon',
    'referencia', 'nro_cta', 'descrip_movs', 'descrip_asis', 'nombre',
    'debe', 'haber', 'feccar', 'tipo_subcta', 'nro_subcuenta',
    'subs_descrip', 'ccosto'
]


@dataclass
class ParseResult:
    ok: bool
    dataframe: Optional[pd.DataFrame] = None
    dataframe_raw: Optional[pd.DataFrame] = None
    errores: list = field(default_factory=list)
    advertencias: list = field(default_factory=list)
    total_filas_raw: int = 0
    total_filas_validas: int = 0
    empresa_id: int = 0
    empresa_nombre: str = ""
    empresa_detectada: Optional[str] = None
    formato: str = ""
    periodo_anio: Optional[int] = None
    periodo_mes: Optional[int] = None


class FileParser:

    ENCODING  = "latin-1"
    SEPARADOR = ";"

    @staticmethod
    def detectar_empresa(nombre_archivo: str) -> Optional[str]:
        nombre_upper = nombre_archivo.upper()
        for emp in EMPRESAS:
            if emp in nombre_upper:
                return emp
        return None

    @staticmethod
    def empresa_id_desde_nombre(nombre: str) -> Optional[int]:
        return EMPRESAS.get(nombre.upper().strip())

    def parsear(self, archivo, empresa_nombre: str) -> ParseResult:
        empresa_id = self.empresa_id_desde_nombre(empresa_nombre)
        if empresa_id is None:
            r = ParseResult(ok=False)
            r.errores.append(f"Empresa desconocida: '{empresa_nombre}'. Válidas: {list(EMPRESAS.keys())}")
            return r

        resultado = ParseResult(ok=False, empresa_id=empresa_id, empresa_nombre=empresa_nombre)
        nombre_archivo = getattr(archivo, 'name', '') or ''
        resultado.empresa_detectada = self.detectar_empresa(nombre_archivo)

        try:
            if hasattr(archivo, 'read'):
                contenido = archivo.read()
                if isinstance(contenido, bytes):
                    contenido = contenido.decode(self.ENCODING, errors='replace')
                df_input = pd.read_csv(
                    io.StringIO(contenido), sep=self.SEPARADOR,
                    header=0, dtype=str, keep_default_na=False, engine='python')
            else:
                df_input = pd.read_csv(
                    archivo, sep=self.SEPARADOR, header=0, encoding=self.ENCODING,
                    dtype=str, keep_default_na=False, engine='python')
        except Exception as e:
            resultado.errores.append(f"Error al leer el archivo: {e}")
            return resultado

        resultado.total_filas_raw = len(df_input)
        n_cols = len(df_input.columns)
        log.info(f"Archivo leído: {resultado.total_filas_raw} filas, {n_cols} columnas")

        cols_lower = [c.lower().strip() for c in df_input.columns]
        es_formato_b = 'id_empresa' in cols_lower and n_cols == COLS_FORMATO_B

        if es_formato_b:
            resultado.formato = 'B'
            return self._parsear_formato_b(df_input, resultado)
        elif n_cols >= COLS_FORMATO_A:
            resultado.formato = 'A'
            if n_cols > COLS_FORMATO_A:
                resultado.advertencias.append(
                    f"El archivo tiene {n_cols} columnas. Se procesarán las primeras {COLS_FORMATO_A}.")
                df_input = df_input.iloc[:, :COLS_FORMATO_A]
            return self._parsear_formato_a(df_input, resultado)
        else:
            resultado.errores.append(
                f"El archivo tiene {n_cols} columnas. "
                f"Se esperan {COLS_FORMATO_A} (formato estándar) o {COLS_FORMATO_B} columnas "
                f"con encabezado (formato exportación con id_empresa). "
                f"Verificá que el separador sea punto y coma (;).")
            return resultado

    # =========================================================================
    # Formato B — 17 columnas con encabezado
    # =========================================================================
    def _parsear_formato_b(self, df_input: pd.DataFrame, resultado: ParseResult) -> ParseResult:
        resultado.advertencias.append("Formato B detectado (17 cols con id_empresa).")

        df = pd.DataFrame()
        df['fecha_raw']      = df_input['fecasi'].str.strip()
        df['tipo_asiento']   = df_input['tipo_asiento'].str.strip()
        df['nro_asiento']    = df_input['nro_asiento'].str.strip()
        df['nro_renglon']    = df_input['Nro_renglon'].str.strip()
        df['descripcion']    = df_input['descrip_movs'].str.strip()
        df['cuenta_codigo']  = df_input['nro_cta'].str.strip()
        df['tipo_subcuenta'] = df_input['tipo_subcta'].str.strip()
        df['nro_subcuenta']  = df_input['nro_subcuenta'].str.strip()
        df['debe_raw']       = df_input['debe'].str.strip()
        df['haber_raw']      = df_input['haber'].str.strip()
        df['centro_costo']   = df_input['ccosto'].str.strip()

        def limpiar_float_int(v):
            try:
                f = float(v)
                return str(int(f)) if f == int(f) else v
            except Exception:
                return v

        df['tipo_subcuenta'] = df['tipo_subcuenta'].apply(
            lambda v: limpiar_float_int(v) if v.strip() not in ('', 'nan') else '')
        df['nro_subcuenta'] = df['nro_subcuenta'].apply(
            lambda v: limpiar_float_int(v) if v.strip() not in ('', 'nan') else '')
        df['centro_costo'] = df['centro_costo'].apply(
            lambda v: limpiar_float_int(v) if v.strip() not in ('', 'nan') else '')

        return self._procesar_df_comun(df, resultado)

    # =========================================================================
    # Formato A — 22 columnas posicional
    # =========================================================================
    def _parsear_formato_a(self, df_input: pd.DataFrame, resultado: ParseResult) -> ParseResult:
        df = pd.DataFrame()
        df['fecha_raw']      = df_input.iloc[:, 0].str.strip()
        df['tipo_asiento']   = df_input.iloc[:, 1].str.strip()
        df['nro_asiento']    = df_input.iloc[:, 2].str.strip()
        df['nro_renglon']    = df_input.iloc[:, 3].str.strip()
        df['descripcion']    = df_input.iloc[:, 5].str.strip()
        df['cuenta_codigo']  = df_input.iloc[:, 6].str.strip()
        df['tipo_subcuenta'] = df_input.iloc[:, 9].str.strip()
        df['nro_subcuenta']  = df_input.iloc[:, 10].str.strip()
        df['debe_raw']       = df_input.iloc[:, 19].str.strip()
        df['haber_raw']      = df_input.iloc[:, 20].str.strip()
        df['centro_costo']   = df_input.iloc[:, 21].str.strip()

        return self._procesar_df_comun(df, resultado)

    # =========================================================================
    # Procesamiento común para ambos formatos
    # =========================================================================
    def _procesar_df_comun(self, df: pd.DataFrame, resultado: ParseResult) -> ParseResult:

        # Limpiar filas vacías
        df = df.dropna(how='all')
        df = df[df['fecha_raw'].str.strip().str.len() > 0].copy()
        df = df.reset_index(drop=True)

        # ── Fecha ─────────────────────────────────────────────────────────────
        df['fecha'] = pd.to_datetime(df['fecha_raw'], format='%d/%m/%Y', errors='coerce')
        mask_no_parsed = df['fecha'].isna()
        if mask_no_parsed.any():
            df.loc[mask_no_parsed, 'fecha'] = pd.to_datetime(
                df.loc[mask_no_parsed, 'fecha_raw'], format='%Y/%m/%d', errors='coerce')

        n_fecha_inv = df['fecha'].isna().sum()
        if n_fecha_inv > 0:
            resultado.errores.append(
                f"{n_fecha_inv} fila(s) con fecha inválida. "
                f"Formatos aceptados: DD/MM/YYYY o YYYY/MM/DD.")

        # ── Período ───────────────────────────────────────────────────────────
        primer_fecha = df['fecha'].dropna().iloc[0] if not df['fecha'].dropna().empty else None
        if primer_fecha is not None:
            resultado.periodo_anio = int(primer_fecha.year)
            resultado.periodo_mes  = int(primer_fecha.month)
            periodos = df['fecha'].dropna().apply(lambda d: (d.year, d.month)).unique()
            if len(periodos) > 1:
                resultado.advertencias.append(
                    f"El archivo contiene {len(periodos)} períodos: "
                    f"{', '.join(f'{a}/{m:02d}' for a, m in sorted(periodos))}. "
                    f"Se procesarán todos.")

        # ── Cuenta código ─────────────────────────────────────────────────────
        df['cuenta_codigo'] = pd.to_numeric(df['cuenta_codigo'], errors='coerce')
        mask_cta_inv = df['cuenta_codigo'].isna()
        if mask_cta_inv.any():
            filas_inv = df[mask_cta_inv]
            detalle = []
            for _, row in filas_inv.iterrows():
                fecha   = self._fmt_fecha(row.get('fecha'))
                asiento = row.get('nro_asiento', '—')
                renglon = row.get('nro_renglon', '—')
                valor   = row.get('cuenta_codigo_raw', str(row.get('cuenta_codigo', '')))
                detalle.append(
                    f"    • fecha={fecha} | asiento={asiento} | "
                    f"renglon={renglon} | valor='{valor}'"
                )
            resultado.errores.append(
                f"{mask_cta_inv.sum()} fila(s) con cuenta_codigo inválido o vacío:\n"
                + "\n".join(detalle)
            )

        # ── Montos: debe y haber ──────────────────────────────────────────────
        df['debe']  = df['debe_raw'].apply(self._parsear_monto)
        df['haber'] = df['haber_raw'].apply(self._parsear_monto)

        # Errores en debe
        mask_debe_inv = df['debe'].isna()
        if mask_debe_inv.any():
            filas_inv = df[mask_debe_inv]
            detalle = []
            for _, row in filas_inv.iterrows():
                fecha   = self._fmt_fecha(row.get('fecha'))
                asiento = row.get('nro_asiento', '—')
                renglon = row.get('nro_renglon', '—')
                cuenta  = row.get('cuenta_codigo', '—')
                valor   = str(row.get('debe_raw', '')).strip()
                desc    = str(row.get('descripcion', '') or '').strip()[:40]
                detalle.append(
                    f"    • fecha={fecha} | asiento={asiento} | renglon={renglon} | "
                    f"cuenta={cuenta} | valor='{valor}'"
                    + (f" | {desc}" if desc else "")
                )
            resultado.errores.append(
                f"{mask_debe_inv.sum()} fila(s) con valor no numérico en columna 'debe':\n"
                + "\n".join(detalle)
            )

        # Errores en haber
        mask_haber_inv = df['haber'].isna()
        if mask_haber_inv.any():
            filas_inv = df[mask_haber_inv]
            detalle = []
            for _, row in filas_inv.iterrows():
                fecha   = self._fmt_fecha(row.get('fecha'))
                asiento = row.get('nro_asiento', '—')
                renglon = row.get('nro_renglon', '—')
                cuenta  = row.get('cuenta_codigo', '—')
                valor   = str(row.get('haber_raw', '')).strip()
                desc    = str(row.get('descripcion', '') or '').strip()[:40]
                detalle.append(
                    f"    • fecha={fecha} | asiento={asiento} | renglon={renglon} | "
                    f"cuenta={cuenta} | valor='{valor}'"
                    + (f" | {desc}" if desc else "")
                )
            resultado.errores.append(
                f"{mask_haber_inv.sum()} fila(s) con valor no numérico en columna 'haber':\n"
                + "\n".join(detalle)
            )

        # ── Normalizar nullables ───────────────────────────────────────────────
        df_raw_out = df[['debe_raw', 'haber_raw']].copy()

        for col in ['tipo_asiento', 'tipo_subcuenta', 'nro_subcuenta', 'centro_costo',
                    'nro_asiento', 'nro_renglon', 'descripcion']:
            df[col] = df[col].replace({'': None, 'nan': None})

        # ── Contexto ──────────────────────────────────────────────────────────
        df['empresa_id']   = resultado.empresa_id
        df['periodo_anio'] = df['fecha'].dt.year
        df['periodo_mes']  = df['fecha'].dt.month

        df_final = df[[
            'empresa_id', 'fecha', 'periodo_anio', 'periodo_mes',
            'tipo_asiento', 'nro_asiento', 'nro_renglon', 'cuenta_codigo',
            'debe', 'haber', 'descripcion', 'tipo_subcuenta', 'nro_subcuenta', 'centro_costo'
        ]].copy()

        # ── Filtrar inválidos ─────────────────────────────────────────────────
        mask_validas = (
            df_final['fecha'].notna() &
            df_final['cuenta_codigo'].notna() &
            df_final['debe'].notna() &
            df_final['haber'].notna()
        )
        n_desc = (~mask_validas).sum()
        if n_desc > 0:
            resultado.advertencias.append(f"{n_desc} fila(s) descartadas por datos inválidos.")

        df_raw_out = df_raw_out[mask_validas].reset_index(drop=True)
        df_final   = df_final[mask_validas].copy().reset_index(drop=True)

        df_final['cuenta_codigo'] = df_final['cuenta_codigo'].astype(int)
        df_final['periodo_anio']  = df_final['periodo_anio'].astype(int)
        df_final['periodo_mes']   = df_final['periodo_mes'].astype(int)
        df_final['empresa_id']    = df_final['empresa_id'].astype(int)

        resultado.total_filas_validas = len(df_final)
        resultado.dataframe           = df_final
        resultado.dataframe_raw       = df_raw_out
        resultado.ok = len(resultado.errores) == 0

        log.info(
            f"Parseo {'OK' if resultado.ok else 'CON ERRORES'} "
            f"(formato {resultado.formato}) — {resultado.total_filas_validas} filas válidas"
        )
        return resultado

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _fmt_fecha(fecha) -> str:
        try:
            return pd.Timestamp(fecha).strftime('%d/%m/%Y')
        except Exception:
            return '—'

    @staticmethod
    def _parsear_monto(valor: str) -> Optional[float]:
        if not isinstance(valor, str):
            return None
        valor = valor.strip()
        if valor in ('', '-'):
            return 0.0
        try:
            if ',' in valor:
                valor = valor.replace('.', '').replace(',', '.')
            return float(valor)
        except (ValueError, AttributeError):
            return None