"""
validator.py
============
Validación de DataFrames del Libro Diario contra la base de datos.

Validaciones implementadas:
  - Cuentas existentes en dim_cuenta              → bloqueante
  - Centros de costo existentes en dim_centro_costo → bloqueante
  - Descuadre contable por asiento                → bloqueante
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)

TOLERANCIA_DESCUADRE = 0.10


class Validator:

    def __init__(self, conn):
        self.conn = conn
        self._cuentas_validas = None
        self._centros_validos = None

    def validar(self, df: pd.DataFrame, empresa_id: int) -> tuple[list, list]:
        errores      = []
        advertencias = []

        self._cuentas_validas = self._cargar_cuentas()
        self._centros_validos = self._cargar_centros()

        errores += self._validar_cuentas(df)
        errores += self._validar_descuadres(df)
        errores += self._validar_centros_costo(df)

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

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _fmt_fecha(fecha) -> str:
        try:
            return pd.Timestamp(fecha).strftime('%d/%m/%Y')
        except Exception:
            return '—'

    @staticmethod
    def _fmt_monto(valor) -> str:
        try:
            return f"{float(valor):+,.2f}"
        except Exception:
            return str(valor)

    # ── Validaciones ──────────────────────────────────────────────────────────

    def _validar_cuentas(self, df: pd.DataFrame) -> list:
        errores = []
        cuentas_invalidas = sorted(
            set(df['cuenta_codigo'].dropna().astype(int).unique()) - self._cuentas_validas
        )
        if not cuentas_invalidas:
            return errores

        lineas = [f"{len(cuentas_invalidas)} cuenta(s) no existen en el plan de cuentas:"]
        for cuenta in cuentas_invalidas:
            filas = df[df['cuenta_codigo'] == cuenta]
            primera = filas.iloc[0]
            fecha       = self._fmt_fecha(primera.get('fecha'))
            asiento     = primera.get('nro_asiento', '—')
            tipo        = primera.get('tipo_asiento', '—') or '—'
            renglon     = primera.get('nro_renglon', '—')
            tipo_subcta = primera.get('tipo_subcuenta', '—') or '—'
            n           = len(filas)
            lineas.append(
                f"  tipo={tipo} | nro={asiento} | renglon={renglon} | "
                f"cta={cuenta} | tiposubcta={tipo_subcta} | "
                f"fecasi={fecha} | aparece en {n} fila(s)"
            )

        errores.append("\n".join(lineas))
        return errores

    def _validar_descuadres(self, df: pd.DataFrame) -> list:
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

        if descuadres.empty:
            return errores

        asientos_detalle = []
        for nro_asiento, diff in descuadres.items():
            filas_asiento = df_valid[df_valid['nro_asiento'] == nro_asiento]
            primera       = filas_asiento.iloc[0]
            fecha         = self._fmt_fecha(primera.get('fecha'))
            tipo          = primera.get('tipo_asiento', '—') or '—'

            renglones = []
            for _, row in filas_asiento.iterrows():
                renglones.append({
                    'Renglón': row.get('nro_renglon', '—'),
                    'Cuenta':  int(row['cuenta_codigo']) if pd.notna(row.get('cuenta_codigo')) else '—',
                    'Debe':    float(row.get('debe', 0)),
                    'Haber':   float(row.get('haber', 0)),
                })

            asientos_detalle.append({
                'nro_asiento': nro_asiento,
                'tipo':        tipo,
                'fecha':       fecha,
                'diff':        diff,
                'renglones':   renglones,
            })

        errores.append({
            '__tipo__':  'descuadre',
            'resumen':   f"{len(descuadres)} asiento(s) descuadrado(s)",
            'asientos':  asientos_detalle,
        })
        log.error(f"Descuadres: {list(descuadres.index)}")
        return errores

    def _validar_centros_costo(self, df: pd.DataFrame) -> list:
        errores = []

        if 'centro_costo' not in df.columns:
            return errores

        centros_archivo = set(
            df['centro_costo'].dropna()
            .astype(str).str.strip()
            .replace('', pd.NA).dropna().unique()
        )
        if not centros_archivo:
            return errores

        centros_invalidos = sorted(centros_archivo - self._centros_validos)
        if not centros_invalidos:
            return errores

        lineas = [f"{len(centros_invalidos)} centro(s) de costo no registrados en el maestro:"]
        for centro in centros_invalidos:
            filas = df[df['centro_costo'].astype(str).str.strip() == str(centro)]
            primera = filas.iloc[0]
            fecha       = self._fmt_fecha(primera.get('fecha'))
            asiento     = primera.get('nro_asiento', '—')
            tipo        = primera.get('tipo_asiento', '—') or '—'
            renglon     = primera.get('nro_renglon', '—')
            cuenta      = int(primera['cuenta_codigo']) if pd.notna(primera.get('cuenta_codigo')) else '—'
            tipo_subcta = primera.get('tipo_subcuenta', '—') or '—'
            n           = len(filas)
            lineas.append(
                f"  tipo={tipo} | nro={asiento} | renglon={renglon} | "
                f"cta={cuenta} | tiposubcta={tipo_subcta} | "
                f"fecasi={fecha} | ccosto='{centro}' | aparece en {n} fila(s)"
            )

        errores.append("\n".join(lineas))
        return errores