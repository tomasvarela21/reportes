"""
mayor_calculator.py
===================
Motor de cálculo del Libro Mayor acumulado.

Lógica central:
  1. Para cada empresa, obtiene todos los períodos desde `desde_periodo` hasta el último cargado
  2. Para cada período, agrupa el diario por (cuenta, tipo_subcuenta, nro_subcuenta, centro_costo)
     → genera filas nivel='subcuenta'
  3. Suma las subcuentas por cuenta
     → genera filas nivel='cuenta'
  4. Calcula saldo_acumulado arrastrando el saldo del período anterior
     (partiendo de saldos_apertura si es el primer período del año fiscal)
  5. Hace DELETE + INSERT del período en libro_mayor (nunca UPDATE parcial)
  6. Registra el recálculo en mayor_recalculo_log

Uso desde otros módulos:
    from mayor_calculator import MayorCalculator
    calc = MayorCalculator(conn)
    calc.recalcular(empresa='BATIA', desde_anio=2024, desde_mes=1)

Uso standalone:
    python mayor_calculator.py --empresa BATIA --desde 2024-01
"""

import os
import time
import logging
import argparse
from datetime import datetime

import psycopg2
import psycopg2.extras
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ No se encontró DATABASE_URL en el archivo .env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


class MayorCalculator:

    def __init__(self, conn=None):
        """
        Si se pasa una conexión existente, la usa.
        Si no, abre una conexión propia.
        """
        self._conn_externo = conn is not None
        self.conn = conn or psycopg2.connect(DATABASE_URL)

    def recalcular(self, empresa: str, desde_anio: int, desde_mes: int, motivo: str = "manual"):
        """
        Recalcula el libro_mayor para una empresa desde un período en adelante.
        Recorre todos los períodos existentes en libro_diario desde (desde_anio, desde_mes).
        """
        inicio = time.time()
        log.info(f"▶ Iniciando recálculo — empresa={empresa} desde={desde_anio}/{desde_mes:02d}")

        cur = self.conn.cursor()
        try:
            # 1. Obtener todos los períodos a recalcular (ordenados)
            cur.execute("""
                SELECT DISTINCT periodo_anio, periodo_mes
                FROM libro_diario
                WHERE empresa = %s
                  AND (periodo_anio > %s OR (periodo_anio = %s AND periodo_mes >= %s))
                ORDER BY periodo_anio, periodo_mes
            """, (empresa, desde_anio, desde_anio, desde_mes))
            periodos = cur.fetchall()

            if not periodos:
                log.warning(f"No hay períodos en libro_diario para {empresa} desde {desde_anio}/{desde_mes:02d}")
                return 0

            log.info(f"  Períodos a recalcular: {len(periodos)}")

            # 2. Obtener saldo acumulado del período ANTERIOR al inicio (punto de arranque)
            saldos_previos = self._get_saldos_previos(cur, empresa, desde_anio, desde_mes)
            log.info(f"  Saldos previos cargados: {len(saldos_previos)} combinaciones")

            total_registros = 0

            for anio, mes in periodos:
                registros = self._calcular_periodo(cur, empresa, anio, mes, saldos_previos)
                total_registros += registros

                # Los saldos_previos para el siguiente período son los acumulados de este
                saldos_previos = self._get_saldos_acumulados_periodo(cur, empresa, anio, mes)
                log.info(f"  ✅ {anio}/{mes:02d} — {registros} registros")

            # 3. Registrar en log
            duracion_ms = int((time.time() - inicio) * 1000)
            ultimo_anio, ultimo_mes = periodos[-1]
            self._log_recalculo(
                cur, empresa,
                desde_anio, desde_mes,
                ultimo_anio, ultimo_mes,
                motivo, total_registros, duracion_ms
            )

            self.conn.commit()
            log.info(f"✅ Recálculo completo — {total_registros} registros — {duracion_ms}ms")
            return total_registros

        except Exception as e:
            self.conn.rollback()
            log.error(f"❌ Error en recálculo: {e}")
            raise
        finally:
            cur.close()

    # =========================================================================
    # Métodos internos
    # =========================================================================

    def _get_saldos_previos(self, cur, empresa: str, desde_anio: int, desde_mes: int) -> dict:
        """
        Obtiene el saldo_acumulado que debe usarse como punto de partida.

        Prioridad:
          A) Si existe un período anterior en libro_mayor → usa esos acumulados (nivel='subcuenta')
          B) Si es el primer período del año → usa saldos_apertura
          C) Si no hay nada → saldo 0
        """
        # Calcular período anterior
        if desde_mes == 1:
            anio_ant, mes_ant = desde_anio - 1, 12
        else:
            anio_ant, mes_ant = desde_anio, desde_mes - 1

        # A) Buscar en libro_mayor el período anterior
        cur.execute("""
            SELECT cuenta_codigo, tipo_subcuenta, nro_subcuenta, centro_costo, saldo_acumulado
            FROM libro_mayor
            WHERE empresa = %s
              AND periodo_anio = %s
              AND periodo_mes = %s
              AND nivel = 'subcuenta'
        """, (empresa, anio_ant, mes_ant))
        rows = cur.fetchall()

        if rows:
            return {
                self._key(r[0], r[1], r[2], r[3]): r[4]
                for r in rows
            }

        # B) Buscar saldos de apertura del año fiscal
        cur.execute("""
            SELECT cuenta_codigo, tipo_subcuenta, nro_subcuenta, centro_costo, saldo
            FROM saldos_apertura
            WHERE empresa = %s
              AND anio_fiscal = %s
        """, (empresa, desde_anio))
        rows = cur.fetchall()

        if rows:
            log.info(f"  Usando saldos_apertura {desde_anio} como punto de partida")
            return {
                self._key(r[0], r[1], r[2], r[3]): r[4]
                for r in rows
            }

        # C) Sin saldo previo → arranca desde 0
        log.info(f"  Sin saldo previo, arranca desde 0")
        return {}

    def _calcular_periodo(self, cur, empresa: str, anio: int, mes: int, saldos_previos: dict) -> int:
        """
        Calcula y persiste libro_mayor para un período específico.
        Primero borra el período existente, luego inserta el recalculado.
        """
        # Paso 1: Obtener movimientos del diario para este período
        cur.execute("""
            SELECT
                cuenta_codigo,
                COALESCE(tipo_subcuenta, '')    AS tipo_subcuenta,
                COALESCE(nro_subcuenta, '')     AS nro_subcuenta,
                COALESCE(centro_costo, '')      AS centro_costo,
                SUM(debe)                       AS total_debe,
                SUM(haber)                      AS total_haber,
                SUM(debe + haber)               AS saldo_periodo
            FROM libro_diario
            WHERE empresa = %s
              AND periodo_anio = %s
              AND periodo_mes = %s
            GROUP BY cuenta_codigo, tipo_subcuenta, nro_subcuenta, centro_costo
            ORDER BY cuenta_codigo, tipo_subcuenta, nro_subcuenta, centro_costo
        """, (empresa, anio, mes))
        rows = cur.fetchall()

        if not rows:
            # Período sin movimientos — limpiar libro_mayor por si tenía datos viejos
            cur.execute("""
                DELETE FROM libro_mayor
                WHERE empresa = %s AND periodo_anio = %s AND periodo_mes = %s
            """, (empresa, anio, mes))
            return 0

        df = pd.DataFrame(rows, columns=[
            'cuenta_codigo', 'tipo_subcuenta', 'nro_subcuenta',
            'centro_costo', 'total_debe', 'total_haber', 'saldo_periodo'
        ])

        # Paso 2: Calcular saldo_acumulado nivel='subcuenta'
        # Normalizar NULLs para el key (COALESCE ya lo hace en SQL, pero por seguridad)
        df['tipo_subcuenta'] = df['tipo_subcuenta'].fillna('')
        df['nro_subcuenta']  = df['nro_subcuenta'].fillna('')
        df['centro_costo']   = df['centro_costo'].fillna('')

        registros_subcuenta = []
        for _, row in df.iterrows():
            key = self._key(row['cuenta_codigo'], row['tipo_subcuenta'],
                            row['nro_subcuenta'], row['centro_costo'])
            saldo_ant = saldos_previos.get(key, 0)
            saldo_acum = float(saldo_ant) + float(row['saldo_periodo'])

            registros_subcuenta.append({
                'empresa':         empresa,
                'periodo_anio':    anio,
                'periodo_mes':     mes,
                'nivel':           'subcuenta',
                'cuenta_codigo':   int(row['cuenta_codigo']),
                'tipo_subcuenta':  row['tipo_subcuenta'] or None,
                'nro_subcuenta':   row['nro_subcuenta'] or None,
                'centro_costo':    row['centro_costo'] or None,
                'total_debe':      float(row['total_debe']),
                'total_haber':     float(row['total_haber']),
                'saldo_periodo':   float(row['saldo_periodo']),
                'saldo_acumulado': saldo_acum,
            })

        # Paso 3: Calcular nivel='cuenta' — suma de subcuentas
        df_cuenta = df.groupby('cuenta_codigo').agg(
            total_debe=('total_debe', 'sum'),
            total_haber=('total_haber', 'sum'),
            saldo_periodo=('saldo_periodo', 'sum')
        ).reset_index()

        registros_cuenta = []
        for _, row in df_cuenta.iterrows():
            # Saldo acumulado de cuenta = suma de acumulados de sus subcuentas en este período
            saldo_acum_cuenta = sum(
                r['saldo_acumulado']
                for r in registros_subcuenta
                if r['cuenta_codigo'] == int(row['cuenta_codigo'])
            )
            registros_cuenta.append({
                'empresa':         empresa,
                'periodo_anio':    anio,
                'periodo_mes':     mes,
                'nivel':           'cuenta',
                'cuenta_codigo':   int(row['cuenta_codigo']),
                'tipo_subcuenta':  None,
                'nro_subcuenta':   None,
                'centro_costo':    None,
                'total_debe':      float(row['total_debe']),
                'total_haber':     float(row['total_haber']),
                'saldo_periodo':   float(row['saldo_periodo']),
                'saldo_acumulado': saldo_acum_cuenta,
            })

        todos_registros = registros_subcuenta + registros_cuenta

        # Paso 4: DELETE del período + INSERT masivo
        cur.execute("""
            DELETE FROM libro_mayor
            WHERE empresa = %s AND periodo_anio = %s AND periodo_mes = %s
        """, (empresa, anio, mes))

        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO libro_mayor (
                empresa, periodo_anio, periodo_mes, nivel,
                cuenta_codigo, tipo_subcuenta, nro_subcuenta, centro_costo,
                total_debe, total_haber, saldo_periodo, saldo_acumulado,
                recalculado_en
            ) VALUES %s
            """,
            [
                (
                    r['empresa'], r['periodo_anio'], r['periodo_mes'], r['nivel'],
                    r['cuenta_codigo'], r['tipo_subcuenta'], r['nro_subcuenta'], r['centro_costo'],
                    r['total_debe'], r['total_haber'], r['saldo_periodo'], r['saldo_acumulado'],
                    datetime.now()
                )
                for r in todos_registros
            ],
            page_size=1000
        )

        return len(todos_registros)

    def _get_saldos_acumulados_periodo(self, cur, empresa: str, anio: int, mes: int) -> dict:
        """
        Devuelve los saldos_acumulado del período recién calculado (nivel='subcuenta')
        para usarlos como saldos_previos del siguiente período.
        """
        cur.execute("""
            SELECT cuenta_codigo, tipo_subcuenta, nro_subcuenta, centro_costo, saldo_acumulado
            FROM libro_mayor
            WHERE empresa = %s
              AND periodo_anio = %s
              AND periodo_mes = %s
              AND nivel = 'subcuenta'
        """, (empresa, anio, mes))
        rows = cur.fetchall()
        return {
            self._key(r[0], r[1], r[2], r[3]): r[4]
            for r in rows
        }

    def _log_recalculo(self, cur, empresa, desde_anio, desde_mes,
                       hasta_anio, hasta_mes, motivo, registros, duracion_ms):
        cur.execute("""
            INSERT INTO mayor_recalculo_log
                (empresa, desde_anio, desde_mes, hasta_anio, hasta_mes,
                 motivo, registros_afectados, duracion_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (empresa, desde_anio, desde_mes, hasta_anio, hasta_mes,
              motivo, registros, duracion_ms))

    @staticmethod
    def _key(cuenta, tipo_subcuenta, nro_subcuenta, centro_costo) -> tuple:
        """Clave normalizada para el dict de saldos previos."""
        return (
            int(cuenta),
            tipo_subcuenta or '',
            nro_subcuenta or '',
            centro_costo or '',
        )

    def close(self):
        if not self._conn_externo:
            self.conn.close()


# =============================================================================
# Uso standalone
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Recalcular Libro Mayor")
    parser.add_argument("--empresa",  required=True, help="Código de empresa (ej: BATIA)")
    parser.add_argument("--desde",    required=True, help="Período inicio YYYY-MM (ej: 2024-01)")
    parser.add_argument("--motivo",   default="manual", help="Motivo del recálculo")
    args = parser.parse_args()

    try:
        anio, mes = map(int, args.desde.split("-"))
    except ValueError:
        print("❌ Formato de --desde inválido. Usá YYYY-MM (ej: 2024-01)")
        return

    calc = MayorCalculator()
    try:
        total = calc.recalcular(args.empresa, anio, mes, motivo=args.motivo)
        print(f"\n✅ Recálculo completado — {total} registros generados en libro_mayor")
    finally:
        calc.close()


if __name__ == "__main__":
    main()