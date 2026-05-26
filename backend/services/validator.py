"""
Copiado de services/validator.py — no importa desde fuera de /backend.
Valida DataFrames del Libro Diario contra dim_cuenta y dim_centro_costo.
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

    @staticmethod
    def _fmt_fecha(fecha) -> str:
        try:
            return pd.Timestamp(fecha).strftime('%d/%m/%Y')
        except Exception:
            return '—'

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
            fecha   = self._fmt_fecha(primera.get('fecha'))
            asiento = primera.get('nro_asiento', '—')
            tipo    = primera.get('tipo_asiento', '—') or '—'
            renglon = primera.get('nro_renglon', '—')
            n       = len(filas)
            lineas.append(
                f"  tipo={tipo} | nro={asiento} | renglon={renglon} | "
                f"cta={cuenta} | fecasi={fecha} | aparece en {n} fila(s)"
            )
        errores.append("\n".join(lineas))
        return errores

    def _validar_descuadres(self, df: pd.DataFrame) -> list:
        errores = []
        if 'nro_asiento' not in df.columns:
            return errores

        df_valid = df.dropna(subset=['nro_asiento', 'debe', 'haber']).copy()
        if df_valid.empty:
            return errores

        claves = ['fecha', 'tipo_asiento', 'nro_asiento']
        claves_presentes = [c for c in claves if c in df_valid.columns]

        balance = (
            df_valid
            .groupby(claves_presentes)
            .apply(lambda g: round(g['debe'].sum() + g['haber'].sum(), 2))
            .reset_index(name='diferencia')
        )
        descuadres = balance[balance['diferencia'].abs() > TOLERANCIA_DESCUADRE]

        if descuadres.empty:
            return errores

        asientos_detalle = []
        for _, desc_row in descuadres.iterrows():
            mask = pd.Series([True] * len(df_valid), index=df_valid.index)
            for clave in claves_presentes:
                mask &= (df_valid[clave] == desc_row[clave])
            filas_asiento = df_valid[mask]

            fecha  = self._fmt_fecha(desc_row.get('fecha', '—'))
            tipo   = str(desc_row.get('tipo_asiento', '—')) if 'tipo_asiento' in desc_row else '—'
            nro    = desc_row.get('nro_asiento', '—')
            diff   = desc_row['diferencia']
            total_debe  = round(filas_asiento['debe'].sum(),  2)
            total_haber = round(filas_asiento['haber'].sum(), 2)

            renglones = [
                {
                    'renglon': row.get('nro_renglon', '—'),
                    'cuenta':  int(row['cuenta_codigo']) if pd.notna(row.get('cuenta_codigo')) else '—',
                    'debe':    float(row.get('debe', 0)),
                    'haber':   float(row.get('haber', 0)),
                }
                for _, row in filas_asiento.iterrows()
            ]

            asientos_detalle.append({
                'nro_asiento': nro,
                'tipo':        tipo,
                'fecha':       fecha,
                'diff':        diff,
                'total_debe':  total_debe,
                'total_haber': total_haber,
                'renglones':   renglones,
            })

        errores.append({
            '__tipo__': 'descuadre',
            'resumen':  f"{len(descuadres)} asiento(s) descuadrado(s)",
            'asientos': asientos_detalle,
        })
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
            fecha   = self._fmt_fecha(primera.get('fecha'))
            asiento = primera.get('nro_asiento', '—')
            tipo    = primera.get('tipo_asiento', '—') or '—'
            n       = len(filas)
            lineas.append(
                f"  tipo={tipo} | nro={asiento} | fecasi={fecha} | "
                f"ccosto='{centro}' | aparece en {n} fila(s)"
            )
        errores.append("\n".join(lineas))
        return errores
