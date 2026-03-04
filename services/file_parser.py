"""
file_parser.py
==============
Parser de archivos CSV del Libro Diario para ReporteApp v2.

Columnas del CSV (22 columnas, índice 0-21):
  0  Fecasi          -> fecha
  1  tipo_asiento    -> (descartado)
  2  nro_asiento     -> nro_asiento
  3  Nro_renglon     -> nro_renglon  (usado para detectar duplicados reales)
  4  referencia      -> (descartado)
  5  descrip_movs    -> descripcion
  6  nro_cta         -> cuenta_codigo (INTEGER)
  7  desc_pdc        -> (descartado)
  8  subs_descrip    -> (descartado)
  9  tipo_subcta     -> tipo_subcuenta (nullable)
  10 nro_subcuenta   -> nro_subcuenta (nullable)
  11-18              -> (descartados)
  19 debe            -> debe (NUMERIC 18,2)
  20 haber           -> haber (NUMERIC 18,2) conservar signo negativo
  21 ccosto          -> centro_costo (nullable)
"""

import io
import logging
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd

log = logging.getLogger(__name__)

COL_FECHA          = 0
COL_NRO_ASIENTO    = 2
COL_NRO_RENGLON    = 3
COL_DESCRIPCION    = 5
COL_CUENTA_CODIGO  = 6
COL_TIPO_SUBCUENTA = 9
COL_NRO_SUBCUENTA  = 10
COL_DEBE           = 19
COL_HABER          = 20
COL_CENTRO_COSTO   = 21

TOTAL_COLUMNAS_ESPERADAS = 22
EMPRESAS_CONOCIDAS = ['BATIA', 'GUARE', 'NORFORK', 'TORRES', 'WERCOLICH']


@dataclass
class ParseResult:
    ok: bool
    dataframe: Optional[pd.DataFrame] = None
    dataframe_raw: Optional[pd.DataFrame] = None
    errores: list = field(default_factory=list)
    advertencias: list = field(default_factory=list)
    total_filas_raw: int = 0
    total_filas_validas: int = 0
    empresa: str = ""
    empresa_detectada: Optional[str] = None  # empresa detectada del nombre del archivo
    periodo_anio: Optional[int] = None
    periodo_mes: Optional[int] = None


class FileParser:

    ENCODING  = "latin-1"
    SEPARADOR = ";"

    @staticmethod
    def detectar_empresa(nombre_archivo: str) -> Optional[str]:
        """
        Intenta detectar el código de empresa a partir del nombre del archivo.
        Retorna el código si lo encuentra, None si no puede determinarlo.
        """
        nombre_upper = nombre_archivo.upper()
        for emp in EMPRESAS_CONOCIDAS:
            if emp in nombre_upper:
                return emp
        return None

    def parsear(self, archivo, empresa: str) -> ParseResult:
        resultado = ParseResult(ok=False, empresa=empresa)
        errores = []
        advertencias = []

        # Detectar empresa del nombre del archivo si está disponible
        nombre_archivo = getattr(archivo, 'name', '') or ''
        resultado.empresa_detectada = self.detectar_empresa(nombre_archivo)

        # Leer archivo
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
        log.info(f"Archivo leído: {resultado.total_filas_raw} filas, {len(df_input.columns)} columnas")

        # Validar columnas
        if len(df_input.columns) < TOTAL_COLUMNAS_ESPERADAS:
            errores.append(
                f"El archivo tiene {len(df_input.columns)} columnas, "
                f"se esperan {TOTAL_COLUMNAS_ESPERADAS}. "
                f"Verificá que el separador sea punto y coma (;).")
            resultado.errores = errores
            return resultado
        if len(df_input.columns) > TOTAL_COLUMNAS_ESPERADAS:
            advertencias.append(
                f"El archivo tiene {len(df_input.columns)} columnas. "
                f"Se procesarán solo las primeras {TOTAL_COLUMNAS_ESPERADAS}.")
            df_input = df_input.iloc[:, :TOTAL_COLUMNAS_ESPERADAS]

        # Extraer columnas por posición
        df = pd.DataFrame()
        df['fecha_raw']      = df_input.iloc[:, COL_FECHA].str.strip()
        df['nro_asiento']    = df_input.iloc[:, COL_NRO_ASIENTO].str.strip()
        df['nro_renglon']    = df_input.iloc[:, COL_NRO_RENGLON].str.strip()
        df['descripcion']    = df_input.iloc[:, COL_DESCRIPCION].str.strip()
        df['cuenta_codigo']  = df_input.iloc[:, COL_CUENTA_CODIGO].str.strip()
        df['tipo_subcuenta'] = df_input.iloc[:, COL_TIPO_SUBCUENTA].str.strip()
        df['nro_subcuenta']  = df_input.iloc[:, COL_NRO_SUBCUENTA].str.strip()
        df['debe_raw']       = df_input.iloc[:, COL_DEBE].str.strip()
        df['haber_raw']      = df_input.iloc[:, COL_HABER].str.strip()
        df['centro_costo']   = df_input.iloc[:, COL_CENTRO_COSTO].str.strip()

        # Limpiar filas vacías
        df = df.dropna(how='all')
        df = df[df['fecha_raw'].str.len() > 0].copy()
        df = df.reset_index(drop=True)

        # Parsear fecha
        df['fecha'] = pd.to_datetime(df['fecha_raw'], format='%d/%m/%Y', errors='coerce')
        n_fecha_inv = df['fecha'].isna().sum()
        if n_fecha_inv > 0:
            errores.append(f"{n_fecha_inv} fila(s) con fecha inválida. Formato esperado: DD/MM/YYYY.")

        # Período
        primer_fecha = df['fecha'].dropna().iloc[0] if not df['fecha'].dropna().empty else None
        if primer_fecha is not None:
            resultado.periodo_anio = int(primer_fecha.year)
            resultado.periodo_mes  = int(primer_fecha.month)
            periodos = df['fecha'].dropna().apply(lambda d: (d.year, d.month)).unique()
            if len(periodos) > 1:
                advertencias.append(
                    f"El archivo contiene {len(periodos)} períodos distintos: "
                    f"{', '.join(f'{a}/{m:02d}' for a, m in sorted(periodos))}. Se procesarán todos.")

        # Cuenta codigo
        df['cuenta_codigo'] = pd.to_numeric(df['cuenta_codigo'], errors='coerce')
        n_cta_inv = df['cuenta_codigo'].isna().sum()
        if n_cta_inv > 0:
            errores.append(f"{n_cta_inv} fila(s) con cuenta_codigo inválido o vacío.")

        # Montos
        df['debe']  = df['debe_raw'].apply(self._parsear_monto)
        df['haber'] = df['haber_raw'].apply(self._parsear_monto)

        filas_debe_inv  = df['debe'].isna().sum()
        filas_haber_inv = df['haber'].isna().sum()
        if filas_debe_inv > 0:
            errores.append(f"{filas_debe_inv} fila(s) con valor 'debe' inválido.")
        if filas_haber_inv > 0:
            errores.append(f"{filas_haber_inv} fila(s) con valor 'haber' inválido.")

        # Guardar raw ANTES de filtrar para mantener índices alineados
        df_raw_out = df[['debe_raw', 'haber_raw']].copy()

        # Normalizar nullables
        for col in ['tipo_subcuenta', 'nro_subcuenta', 'centro_costo',
                    'nro_asiento', 'nro_renglon', 'descripcion']:
            df[col] = df[col].replace('', None)

        # Columnas de contexto
        df['empresa']      = empresa
        df['periodo_anio'] = df['fecha'].dt.year
        df['periodo_mes']  = df['fecha'].dt.month

        # Columnas finales — nro_renglon incluido para deteccion de duplicados
        df_final = df[[
            'empresa', 'fecha', 'periodo_anio', 'periodo_mes',
            'nro_asiento', 'nro_renglon', 'cuenta_codigo', 'debe', 'haber',
            'descripcion', 'tipo_subcuenta', 'nro_subcuenta', 'centro_costo'
        ]].copy()

        # Filtrar inválidos
        mask_validas = (
            df_final['fecha'].notna() &
            df_final['cuenta_codigo'].notna() &
            df_final['debe'].notna() &
            df_final['haber'].notna()
        )
        n_desc = (~mask_validas).sum()
        if n_desc > 0:
            advertencias.append(
                f"{n_desc} fila(s) descartadas por datos críticos inválidos (fecha, cuenta o montos).")

        # Alinear raw con válidas
        df_raw_out = df_raw_out[mask_validas].reset_index(drop=True)
        df_final   = df_final[mask_validas].copy().reset_index(drop=True)

        # Tipos finales
        df_final['cuenta_codigo'] = df_final['cuenta_codigo'].astype(int)
        df_final['periodo_anio']  = df_final['periodo_anio'].astype(int)
        df_final['periodo_mes']   = df_final['periodo_mes'].astype(int)

        resultado.total_filas_validas = len(df_final)
        resultado.dataframe           = df_final
        resultado.dataframe_raw       = df_raw_out
        resultado.errores             = errores
        resultado.advertencias        = advertencias
        resultado.ok = len(errores) == 0

        if resultado.ok:
            log.info(f"Parseo OK — {resultado.total_filas_validas} filas válidas ({resultado.periodo_anio}/{resultado.periodo_mes:02d})")
        else:
            log.warning(f"Parseo con errores: {errores}")

        return resultado

    @staticmethod
    def _parsear_monto(valor: str) -> Optional[float]:
        if not isinstance(valor, str):
            return None
        valor = valor.strip()
        if valor == '' or valor == '-':
            return 0.0
        try:
            if ',' in valor:
                valor = valor.replace('.', '').replace(',', '.')
            elif len(valor.split('.')) > 1:
                valor = valor.replace('.', '')
            return float(valor)
        except (ValueError, AttributeError):
            return None