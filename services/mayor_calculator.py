"""
mayor_calculator.py
===================
Motor de cálculo del Libro Mayor acumulado.
"""

import os
import time
import logging
import argparse
from collections import defaultdict
from datetime import datetime

import psycopg2
import psycopg2.extras
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ No se encontró DATABASE_URL en el archivo .env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

EMPRESAS = {
    'BATIA':     1,
    'GUARE':     3,
    'NORFORK':   2,
    'TORRES':    4,
    'WERCOLICH': 5,
}
EMPRESAS_INV = {v: k for k, v in EMPRESAS.items()}


class MayorCalculator:

    def __init__(self, conn=None):
        self._conn_externo = conn is not None
        self.conn = conn or psycopg2.connect(DATABASE_URL)

    def recalcular(self, empresa_id: int, desde_anio: int, desde_mes: int, motivo: str = "manual"):
        inicio = time.time()
        empresa_nombre = EMPRESAS_INV.get(empresa_id, str(empresa_id))
        log.info(f"▶ Recálculo — empresa_id={empresa_id} ({empresa_nombre}) desde={desde_anio}/{desde_mes:02d}")

        cur = self.conn.cursor()
        try:
            cur.execute("""
                SELECT DISTINCT periodo_anio, periodo_mes
                FROM libro_diario
                WHERE empresa_id = %s
                  AND (periodo_anio > %s OR (periodo_anio = %s AND periodo_mes >= %s))
                ORDER BY periodo_anio, periodo_mes
            """, (empresa_id, desde_anio, desde_anio, desde_mes))
            periodos = cur.fetchall()

            if not periodos:
                log.warning(f"Sin períodos en libro_diario para empresa_id={empresa_id} desde {desde_anio}/{desde_mes:02d}")
                return 0

            log.info(f"  Períodos a recalcular: {len(periodos)}")

            saldos_previos = self._get_saldos_previos(cur, empresa_id, desde_anio, desde_mes)
            log.info(f"  Saldos previos: {len(saldos_previos)} combinaciones")

            total_registros = 0
            for anio, mes in periodos:
                registros = self._calcular_periodo(cur, empresa_id, anio, mes, saldos_previos)
                total_registros += registros
                saldos_previos = self._get_saldos_acumulados_periodo(cur, empresa_id, anio, mes)
                log.info(f"  ✅ {anio}/{mes:02d} — {registros} registros")

            duracion_ms = int((time.time() - inicio) * 1000)
            ultimo_anio, ultimo_mes = periodos[-1]
            self._log_recalculo(cur, empresa_id, desde_anio, desde_mes,
                                ultimo_anio, ultimo_mes, motivo, total_registros, duracion_ms)

            self.conn.commit()
            log.info(f"✅ Recálculo completo — {total_registros} registros — {duracion_ms}ms")
            return total_registros

        except Exception as e:
            self.conn.rollback()
            log.error(f"❌ Error en recálculo: {e}")
            raise
        finally:
            cur.close()

    def _get_saldos_previos(self, cur, empresa_id: int, desde_anio: int, desde_mes: int) -> dict:
        if desde_mes == 1:
            anio_ant, mes_ant = desde_anio - 1, 12
        else:
            anio_ant, mes_ant = desde_anio, desde_mes - 1

        # A) Período anterior en libro_mayor
        cur.execute("""
            SELECT cuenta_codigo, tipo_subcuenta, nro_subcuenta, centro_costo, saldo_acumulado
            FROM libro_mayor
            WHERE empresa_id = %s AND periodo_anio = %s AND periodo_mes = %s AND nivel = 'subcuenta'
        """, (empresa_id, anio_ant, mes_ant))
        rows = cur.fetchall()
        if rows:
            return {self._key(r[0], r[1], r[2], r[3]): r[4] for r in rows}

        # B) saldos_apertura
        cur.execute("""
            SELECT cuenta_codigo, tipo_subcuenta, nro_subcuenta, centro_costo, saldo
            FROM saldos_apertura
            WHERE empresa_id = %s AND anio_fiscal = %s
        """, (empresa_id, desde_anio))
        rows = cur.fetchall()
        if rows:
            log.info(f"  Usando saldos_apertura {desde_anio}")
            return {self._key(r[0], r[1], r[2], r[3]): r[4] for r in rows}

        log.info("  Sin saldo previo, arranca desde 0")
        return {}

    def _calcular_periodo(self, cur, empresa_id: int, anio: int, mes: int, saldos_previos: dict) -> int:
        cur.execute("""
            SELECT
                cuenta_codigo,
                COALESCE(tipo_subcuenta, '') AS tipo_subcuenta,
                COALESCE(nro_subcuenta,  '') AS nro_subcuenta,
                COALESCE(centro_costo,   '') AS centro_costo,
                SUM(debe)                    AS total_debe,
                SUM(haber)                   AS total_haber,
                SUM(debe + haber)            AS saldo_periodo
            FROM libro_diario
            WHERE empresa_id = %s AND periodo_anio = %s AND periodo_mes = %s
            GROUP BY cuenta_codigo, tipo_subcuenta, nro_subcuenta, centro_costo
            ORDER BY cuenta_codigo, tipo_subcuenta, nro_subcuenta, centro_costo
        """, (empresa_id, anio, mes))
        rows = cur.fetchall()

        movimientos = {}
        for r in rows:
            key = self._key(r[0], r[1], r[2], r[3])
            movimientos[key] = (float(r[4]), float(r[5]), float(r[6]))

        todas_las_claves = set(movimientos.keys()) | set(saldos_previos.keys())

        if not todas_las_claves:
            cur.execute("""
                DELETE FROM libro_mayor
                WHERE empresa_id = %s AND periodo_anio = %s AND periodo_mes = %s
            """, (empresa_id, anio, mes))
            return 0

        # nivel='subcuenta' — incluye saldo_anterior
        registros_subcuenta = []
        for key in todas_las_claves:
            cuenta_codigo, tipo_subcuenta, nro_subcuenta, centro_costo = key
            saldo_ant = round(float(saldos_previos.get(key, 0)), 2)

            if key in movimientos:
                total_debe, total_haber, saldo_periodo = movimientos[key]
            else:
                total_debe, total_haber, saldo_periodo = 0.0, 0.0, 0.0

            total_debe      = round(float(total_debe),    2)
            total_haber     = round(float(total_haber),   2)
            saldo_periodo   = round(float(saldo_periodo), 2)
            saldo_acumulado = round(saldo_ant + saldo_periodo, 2)

            registros_subcuenta.append({
                'empresa_id':      empresa_id,
                'periodo_anio':    anio,
                'periodo_mes':     mes,
                'nivel':           'subcuenta',
                'cuenta_codigo':   int(cuenta_codigo),
                'tipo_subcuenta':  tipo_subcuenta or None,
                'nro_subcuenta':   nro_subcuenta or None,
                'centro_costo':    centro_costo or None,
                'total_debe':      total_debe,
                'total_haber':     total_haber,
                'saldo_anterior':  saldo_ant,
                'saldo_periodo':   saldo_periodo,
                'saldo_acumulado': saldo_acumulado,
            })

        # nivel='cuenta' — suma de subcuentas
        cuenta_totales = defaultdict(lambda: {
            'debe': 0.0, 'haber': 0.0,
            'saldo_anterior': 0.0, 'saldo_periodo': 0.0, 'saldo_acumulado': 0.0
        })
        for r in registros_subcuenta:
            c = r['cuenta_codigo']
            cuenta_totales[c]['debe']            += r['total_debe']
            cuenta_totales[c]['haber']           += r['total_haber']
            cuenta_totales[c]['saldo_anterior']  += r['saldo_anterior']
            cuenta_totales[c]['saldo_periodo']   += r['saldo_periodo']
            cuenta_totales[c]['saldo_acumulado'] += r['saldo_acumulado']

        registros_cuenta = [{
            'empresa_id':      empresa_id,
            'periodo_anio':    anio,
            'periodo_mes':     mes,
            'nivel':           'cuenta',
            'cuenta_codigo':   cta,
            'tipo_subcuenta':  None,
            'nro_subcuenta':   None,
            'centro_costo':    None,
            'total_debe':      round(t['debe'],            2),
            'total_haber':     round(t['haber'],           2),
            'saldo_anterior':  round(t['saldo_anterior'],  2),
            'saldo_periodo':   round(t['saldo_periodo'],   2),
            'saldo_acumulado': round(t['saldo_acumulado'], 2),
        } for cta, t in cuenta_totales.items()]

        todos = registros_subcuenta + registros_cuenta

        cur.execute("""
            DELETE FROM libro_mayor
            WHERE empresa_id = %s AND periodo_anio = %s AND periodo_mes = %s
        """, (empresa_id, anio, mes))

        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO libro_mayor (
                empresa_id, periodo_anio, periodo_mes, nivel,
                cuenta_codigo, tipo_subcuenta, nro_subcuenta, centro_costo,
                total_debe, total_haber, saldo_anterior, saldo_periodo, saldo_acumulado,
                recalculado_en
            ) VALUES %s
            """,
            [(r['empresa_id'], r['periodo_anio'], r['periodo_mes'], r['nivel'],
              r['cuenta_codigo'], r['tipo_subcuenta'], r['nro_subcuenta'], r['centro_costo'],
              r['total_debe'], r['total_haber'], r['saldo_anterior'], r['saldo_periodo'],
              r['saldo_acumulado'], datetime.now()) for r in todos],
            page_size=1000
        )
        return len(todos)

    def _get_saldos_acumulados_periodo(self, cur, empresa_id: int, anio: int, mes: int) -> dict:
        cur.execute("""
            SELECT cuenta_codigo, tipo_subcuenta, nro_subcuenta, centro_costo, saldo_acumulado
            FROM libro_mayor
            WHERE empresa_id = %s AND periodo_anio = %s AND periodo_mes = %s AND nivel = 'subcuenta'
        """, (empresa_id, anio, mes))
        return {self._key(r[0], r[1], r[2], r[3]): r[4] for r in cur.fetchall()}

    def _log_recalculo(self, cur, empresa_id, desde_anio, desde_mes,
                       hasta_anio, hasta_mes, motivo, registros, duracion_ms):
        cur.execute("""
            INSERT INTO mayor_recalculo_log
                (empresa_id, desde_anio, desde_mes, hasta_anio, hasta_mes,
                 motivo, registros_afectados, duracion_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (empresa_id, desde_anio, desde_mes, hasta_anio, hasta_mes,
              motivo, registros, duracion_ms))

    @staticmethod
    def _key(cuenta, tipo_subcuenta, nro_subcuenta, centro_costo) -> tuple:
        """
        Normaliza los componentes de la clave para garantizar consistencia
        entre libro_diario (donde '0' puede venir del sistema origen),
        saldos_apertura (donde NULL viene de limpiar_tipo_subcta) y
        libro_mayor. Trata '0', '0.0', NULL y '' como equivalentes (sin dato).
        """
        def norm(v):
            s = str(v).strip() if v is not None else ''
            return '' if s in ('', '0', '0.0', 'nan') else s
        return (int(cuenta), norm(tipo_subcuenta), norm(nro_subcuenta), norm(centro_costo))

    def close(self):
        if not self._conn_externo:
            self.conn.close()


def main():
    parser = argparse.ArgumentParser(description="Recalcular Libro Mayor")
    parser.add_argument("--empresa", required=True, help="Nombre de empresa (ej: BATIA)")
    parser.add_argument("--desde",   required=True, help="YYYY-MM")
    parser.add_argument("--motivo",  default="manual")
    args = parser.parse_args()

    empresa_id = EMPRESAS.get(args.empresa.upper())
    if empresa_id is None:
        print(f"❌ Empresa desconocida: {args.empresa}. Válidas: {list(EMPRESAS.keys())}")
        return

    try:
        anio, mes = map(int, args.desde.split("-"))
    except ValueError:
        print("❌ Formato de --desde inválido. Usá YYYY-MM (ej: 2024-01)")
        return

    calc = MayorCalculator()
    try:
        total = calc.recalcular(empresa_id, anio, mes, motivo=args.motivo)
        print(f"\n✅ Recálculo completado — {total} registros generados en libro_mayor")
    finally:
        calc.close()


if __name__ == "__main__":
    main()