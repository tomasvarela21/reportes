"""
validator.py
============
Validador de datos del Libro Diario para ReporteApp v2.

Validaciones en cadena:
  1. Tipos de montos    - debe y haber son numericos (BLOQUEA)
  2. Balance            - debe + haber = 0 por asiento (BLOQUEA)
  3. Plan de cuentas    - cuentas existen en dim_cuenta (ADVERTENCIA)
  4. Centros de costo   - centros existen en dim_centro_costo (ADVERTENCIA)
  5. Duplicados         - sin filas duplicadas en el archivo (BLOQUEA)

Uso:
    from validator import Validator
    v = Validator(conn)
    resultado = v.validar(df, df_raw, empresa, periodo_anio, periodo_mes)
"""

import logging
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd

log = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    ok: bool
    errores: list = field(default_factory=list)
    advertencias: list = field(default_factory=list)

    # Estado de cada validacion
    tipos_ok: bool = False
    balance_ok: bool = False
    cuentas_ok: bool = False
    centros_costo_ok: bool = False
    duplicados_ok: bool = False

    # Detalle para UI
    tipos_problemas: list = field(default_factory=list)        # [{fila, columna, valor}]
    cuentas_inexistentes: list = field(default_factory=list)
    centros_inexistentes: list = field(default_factory=list)
    asientos_desbalanceados: list = field(default_factory=list)
    duplicados_encontrados: int = 0

    # Totales
    total_debe: float = 0.0
    total_haber: float = 0.0
    total_registros: int = 0


class Validator:

    def __init__(self, conn):
        self.conn = conn

    def validar(
        self,
        df: pd.DataFrame,
        df_raw: pd.DataFrame,
        empresa: str,
        periodo_anio: int,
        periodo_mes: int
    ) -> ValidationResult:
        resultado = ValidationResult(ok=False)
        resultado.total_registros = len(df)
        resultado.total_debe  = float(df['debe'].sum())
        resultado.total_haber = float(df['haber'].sum())

        log.info(f"Iniciando validacion -- {len(df)} registros -- {empresa} {periodo_anio}/{periodo_mes:02d}")

        self._validar_tipos_montos(df, df_raw, resultado)
        self._validar_balance(df, resultado)
        self._validar_cuentas(df, resultado)
        self._validar_centros_costo(df, resultado)
        self._validar_duplicados(df, resultado)

        resultado.ok = len(resultado.errores) == 0

        if resultado.ok:
            log.info("OK Validacion sin errores bloqueantes")
        else:
            log.warning(f"ERROR {len(resultado.errores)} error(es) bloqueante(s)")

        return resultado

    # =========================================================================
    # Validacion 1 - Tipos de montos (BLOQUEA)
    # =========================================================================

    def _validar_tipos_montos(self, df, df_raw, resultado):
        """
        Detecta valores no numericos en debe/haber comparando df_raw (strings originales)
        con df (ya convertido a float). Si la conversion produjo NaN pero habia un valor
        no vacio -> problema. Reporta numero de fila exacto y valor original.
        """
        problemas = []
        pares = []

        if df_raw is not None and not df_raw.empty:
            if 'debe_raw' in df_raw.columns and 'debe' in df.columns:
                pares.append(('debe_raw', 'debe'))
            if 'haber_raw' in df_raw.columns and 'haber' in df.columns:
                pares.append(('haber_raw', 'haber'))

        if not pares:
            resultado.tipos_ok = True
            resultado.advertencias.append("No se pudieron verificar tipos de montos (df_raw no disponible).")
            return

        for col_raw, col_num in pares:
            raw_series = df_raw[col_raw].astype(str).str.strip()
            num_series = df[col_num]
            mask_fallo = (
                num_series.isna() &
                raw_series.notna() &
                (raw_series != '') &
                (raw_series != '-') &
                (raw_series.str.lower() != 'nan')
            )
            for idx in df_raw[mask_fallo].index:
                problemas.append({
                    'fila':    int(idx) + 2,
                    'columna': col_num,
                    'valor':   raw_series.loc[idx],
                })

        if not problemas:
            resultado.tipos_ok = True
            log.info("  OK Tipos de montos correctos")
        else:
            resultado.tipos_ok = False
            resultado.tipos_problemas = problemas
            primeros = problemas[:10]
            detalle = "; ".join(
                f"fila {p['fila']} ({p['columna']} = '{p['valor']}')"
                for p in primeros
            )
            sufijo = f" (y {len(problemas) - 10} mas)" if len(problemas) > 10 else ""
            resultado.errores.append(
                f"{len(problemas)} valor(es) no numerico(s) en debe/haber: "
                f"{detalle}{sufijo}. "
                f"Revisa y corregi esas filas en el archivo CSV antes de continuar."
            )
            log.warning(f"  ERROR {len(problemas)} valores no numericos")

    # =========================================================================
    # Validacion 2 - Balance por asiento (BLOQUEA)
    # =========================================================================

    def _validar_balance(self, df: pd.DataFrame, resultado: ValidationResult):
        """
        Valida que cada asiento balancee: SUM(debe + haber) aprox 0.
        Haber viene negativo del origen, por eso la formula es debe + haber.
        """
        TOLERANCIA = 0.01
        df_con_asiento = df[df['nro_asiento'].notna()].copy()

        if df_con_asiento.empty:
            resultado.advertencias.append(
                "No se encontraron numeros de asiento. Se valida balance global del archivo.")
            balance_global = round(resultado.total_debe + resultado.total_haber, 2)
            if abs(balance_global) <= TOLERANCIA:
                resultado.balance_ok = True
            else:
                resultado.balance_ok = False
                resultado.errores.append(
                    f"El archivo no balancea globalmente. "
                    f"Total debe: {resultado.total_debe:,.2f} | "
                    f"Total haber: {resultado.total_haber:,.2f} | "
                    f"Diferencia: {balance_global:,.2f}"
                )
            return

        balance_por_asiento = (
            df_con_asiento
            .groupby('nro_asiento')
            .apply(lambda g: round((g['debe'] + g['haber']).sum(), 2))
            .reset_index()
        )
        balance_por_asiento.columns = ['nro_asiento', 'balance']
        desbalanceados = balance_por_asiento[balance_por_asiento['balance'].abs() > TOLERANCIA]

        if desbalanceados.empty:
            resultado.balance_ok = True
            log.info(f"  OK Balance correcto -- {len(balance_por_asiento)} asientos verificados")
        else:
            resultado.balance_ok = False
            resultado.asientos_desbalanceados = desbalanceados.to_dict('records')
            resultado.errores.append(
                f"{len(desbalanceados)} asiento(s) no balancean (debe + haber != 0). "
                f"Ver detalle en el expander de abajo."
            )
            log.warning(f"  ERROR {len(desbalanceados)} asientos desbalanceados")

    # =========================================================================
    # Validacion 3 - Plan de cuentas (ADVERTENCIA, no bloquea)
    # =========================================================================

    def _validar_cuentas(self, df: pd.DataFrame, resultado: ValidationResult):
        cuentas_archivo = set(df['cuenta_codigo'].dropna().astype(int).unique())
        if not cuentas_archivo:
            resultado.errores.append("No se encontraron cuentas validas en el archivo.")
            resultado.cuentas_ok = False
            return

        cur = self.conn.cursor()
        try:
            cur.execute("SELECT codigo FROM dim_cuenta")
            cuentas_db = set(row[0] for row in cur.fetchall())
        finally:
            cur.close()

        inexistentes = sorted(cuentas_archivo - cuentas_db)
        if not inexistentes:
            resultado.cuentas_ok = True
            log.info(f"  OK Cuentas -- {len(cuentas_archivo)} verificadas")
        else:
            resultado.cuentas_ok = False
            resultado.cuentas_inexistentes = inexistentes
            resultado.advertencias.append(
                f"{len(inexistentes)} cuenta(s) no existen en el plan de cuentas: "
                f"{', '.join(str(c) for c in inexistentes[:10])}"
                f"{'...' if len(inexistentes) > 10 else ''}. "
                f"Se permite la carga pero revisa el plan de cuentas."
            )
            log.warning(f"  ADVERTENCIA {len(inexistentes)} cuentas inexistentes")

    # =========================================================================
    # Validacion 4 - Centros de costo (ADVERTENCIA, no bloquea)
    # =========================================================================

    def _validar_centros_costo(self, df: pd.DataFrame, resultado: ValidationResult):
        df_con_cc = df[df['centro_costo'].notna() & (df['centro_costo'] != '')]
        centros_archivo = set(df_con_cc['centro_costo'].unique())

        if not centros_archivo:
            resultado.centros_costo_ok = True
            log.info("  OK Centros de costo -- ninguno en el archivo")
            return

        cur = self.conn.cursor()
        try:
            cur.execute("SELECT codigo FROM dim_centro_costo")
            centros_db = set(row[0] for row in cur.fetchall())
        finally:
            cur.close()

        if not centros_db:
            resultado.advertencias.append(
                f"dim_centro_costo esta vacia. No se pudieron validar "
                f"{len(centros_archivo)} centro(s). Carga el maestro antes de procesar."
            )
            resultado.centros_costo_ok = True
            log.warning("  ADVERTENCIA dim_centro_costo vacia")
            return

        inexistentes = sorted(centros_archivo - centros_db)
        if not inexistentes:
            resultado.centros_costo_ok = True
            log.info(f"  OK Centros de costo -- {len(centros_archivo)} verificados")
        else:
            resultado.centros_costo_ok = False
            resultado.centros_inexistentes = list(inexistentes)
            resultado.advertencias.append(
                f"{len(inexistentes)} centro(s) de costo no existen en el maestro: "
                f"{', '.join(str(c) for c in inexistentes[:10])}"
                f"{'...' if len(inexistentes) > 10 else ''}. "
                f"Se permite la carga pero revisa el maestro."
            )
            log.warning(f"  ADVERTENCIA {len(inexistentes)} centros inexistentes")

    # =========================================================================
    # Validacion 5 - Duplicados (BLOQUEA)
    # =========================================================================

    def _validar_duplicados(self, df: pd.DataFrame, resultado: ValidationResult):
        # nro_renglon diferencia renglones legitimos dentro del mismo asiento
        # con misma cuenta/monto/descripcion (ej: distribucion por obra)
        columnas_clave = ['fecha', 'nro_asiento', 'nro_renglon', 'cuenta_codigo',
                          'debe', 'haber', 'tipo_subcuenta', 'nro_subcuenta', 'descripcion']
        columnas_clave = [c for c in columnas_clave if c in df.columns]
        duplicados = df[df.duplicated(subset=columnas_clave, keep=False)]
        cantidad = len(duplicados)

        if cantidad == 0:
            resultado.duplicados_ok = True
            log.info("  OK Sin duplicados")
        else:
            resultado.duplicados_ok = False
            resultado.duplicados_encontrados = cantidad
            resultado.errores.append(
                f"{cantidad} fila(s) duplicadas en el archivo. Revisa antes de continuar."
            )
            log.warning(f"  ERROR {cantidad} filas duplicadas")

    # =========================================================================
    # Resumen para UI
    # =========================================================================

    def resumen_texto(self, resultado: ValidationResult) -> dict:
        """
        Devuelve resumen estructurado para la UI de Streamlit.
        El campo 'bloquea' indica si el check fallido impide la carga o es solo advertencia.
        """
        return {
            "total_registros": resultado.total_registros,
            "total_debe":      resultado.total_debe,
            "total_haber":     resultado.total_haber,
            "checks": [
                {
                    "nombre":  "Tipos de montos",
                    "ok":      resultado.tipos_ok,
                    "detalle": (
                        f"{len(resultado.tipos_problemas)} valor(es) no numerico(s) -- "
                        f"primero en fila {resultado.tipos_problemas[0]['fila']}, "
                        f"columna '{resultado.tipos_problemas[0]['columna']}', "
                        f"valor '{resultado.tipos_problemas[0]['valor']}'"
                        if not resultado.tipos_ok and resultado.tipos_problemas
                        else "Debe y haber son todos numericos"
                    ),
                    "bloquea": True,
                },
                {
                    "nombre":  "Balance por asiento",
                    "ok":      resultado.balance_ok,
                    "detalle": (
                        f"{len(resultado.asientos_desbalanceados)} asiento(s) desbalanceado(s)"
                        if not resultado.balance_ok
                        else "Todos los asientos balancean"
                    ),
                    "bloquea": True,
                },
                {
                    "nombre":  "Plan de cuentas",
                    "ok":      resultado.cuentas_ok,
                    "detalle": (
                        f"Cuentas no encontradas: {resultado.cuentas_inexistentes[:10]}"
                        if not resultado.cuentas_ok
                        else "Todas las cuentas existen"
                    ),
                    "bloquea": False,
                },
                {
                    "nombre":  "Centros de costo",
                    "ok":      resultado.centros_costo_ok,
                    "detalle": (
                        f"Centros no encontrados: {resultado.centros_inexistentes[:10]}"
                        if not resultado.centros_costo_ok
                        else "Todos los centros son validos"
                    ),
                    "bloquea": False,
                },
                {
                    "nombre":  "Duplicados",
                    "ok":      resultado.duplicados_ok,
                    "detalle": (
                        f"{resultado.duplicados_encontrados} fila(s) duplicadas"
                        if not resultado.duplicados_ok
                        else "Sin duplicados"
                    ),
                    "bloquea": True,
                },
            ],
            "errores":      resultado.errores,
            "advertencias": resultado.advertencias,
        }