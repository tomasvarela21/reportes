"""
validator.py
============
Validación de DataFrames del Libro Diario contra la base de datos.

Validaciones implementadas:
  - Cuentas existentes en dim_cuenta
  - Centros de costo existentes en dim_centro_costo
  - Descuadre contable por asiento (SUM(debe) + SUM(haber) = 0 ± 0.01)
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)

TOLERANCIA_DESCUADRE = 0.01


class Validator:

    def __init__(self, conn):
        self.conn = conn
        self._cuentas_validas = None
        self._centros_validos = None

    def validar(self, df: pd.DataFrame, empresa_id: int) -> tuple[list, list]:
        """
        Valida el DataFrame contra dim_cuenta, dim_centro_costo
        y reglas contables internas.
        Retorna (errores, advertencias).
        """
        errores      = []
        advertencias = []

        self._cuentas_validas = self._cargar_cuentas()
        self._centros_validos = self._cargar_centros()

        errores      += self._validar_cuentas(df)
        errores      += self._validar_descuadres(df)
        advertencias += self._validar_centros_costo(df)

        return errores, advertencias

    # ── Carga de catálogos ────────────────────────────────────────────────────

    def _cargar_cuentas(self) -> set:
        cur = self.conn.cursor()
        cur.execute("SELECT nro_cta FROM dim_cuenta WHERE activa = true")
        result = {row[0] for row in cur.fetchall()}
        cur.close()
        return result

    def _cargar_centros(self) -> set:
        cur = self.conn.cursor()
        cur.execute("SELECT codigo FROM dim_centro_costo WHERE activo = true")
        result = {str(row[0]) for row in cur.fetchall()}
        cur.close()
        return result

    # ── Validaciones ──────────────────────────────────────────────────────────

    def _validar_cuentas(self, df: pd.DataFrame) -> list:
        errores = []
        cuentas_archivo = set(df['cuenta_codigo'].dropna().astype(int).unique())
        cuentas_invalidas = cuentas_archivo - self._cuentas_validas
        if cuentas_invalidas:
            errores.append(
                f"{len(cuentas_invalidas)} cuenta(s) no existen en el plan: "
                f"{sorted(cuentas_invalidas)}"
            )
        return errores

    def _validar_descuadres(self, df: pd.DataFrame) -> list:
        """
        Verifica que cada asiento cuadre: SUM(debe) + SUM(haber) = 0
        Tolerancia: ±0.01 para cubrir diferencias de redondeo.
        """
        errores = []

        if 'nro_asiento' not in df.columns:
            log.warning("Columna nro_asiento no encontrada, se omite validación de descuadre.")
            return errores

        df_valid = df.dropna(subset=['nro_asiento', 'debe', 'haber']).copy()
        if df_valid.empty:
            return errores

        balance = (
            df_valid
            .groupby('nro_asiento')
            .apply(lambda g: round(g['debe'].sum() + g['haber'].sum(), 2))
        )

        descuadres = balance[balance.abs() > TOLERANCIA_DESCUADRE]

        if not descuadres.empty:
            detalle = ', '.join(
                f"asiento {a} (diff={v:+.2f})"
                for a, v in descuadres.items()
            )
            errores.append(
                f"{len(descuadres)} asiento(s) descuadrado(s): {detalle}"
            )
            log.error(f"Descuadres detectados: {detalle}")

        return errores

    def _validar_centros_costo(self, df: pd.DataFrame) -> list:
        advertencias = []
        if 'centro_costo' not in df.columns:
            return advertencias

        centros_archivo = set(
            df['centro_costo'].dropna()
            .astype(str).str.strip()
            .replace('', pd.NA).dropna().unique()
        )
        if not centros_archivo:
            return advertencias

        centros_invalidos = centros_archivo - self._centros_validos
        if centros_invalidos:
            advertencias.append(
                f"{len(centros_invalidos)} centro(s) de costo no registrados: "
                f"{sorted(centros_invalidos)}"
            )
        return advertencias