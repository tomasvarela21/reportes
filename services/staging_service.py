"""
staging_service.py
==================
Servicio de staging y carga del Libro Diario para ReporteApp v2.
"""

import io
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from mayor_calculator import MayorCalculator

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ No se encontró DATABASE_URL en el archivo .env")

log = logging.getLogger(__name__)

# Mapeo nombre → empresa_id (fuente de verdad)
EMPRESAS = {
    'BATIA':     1,
    'GUARE':     3,
    'NORFORK':   2,
    'TORRES':    4,
    'WERCOLICH': 5,
}


@dataclass
class PeriodoInfo:
    existe: bool
    empresa_id: int
    periodo_anio: int
    periodo_mes: int
    total_registros: int = 0
    total_debe: float = 0.0
    total_haber: float = 0.0
    fecha_carga: Optional[datetime] = None
    archivo_origen: Optional[str] = None


@dataclass
class CargaResult:
    ok: bool
    accion: str = ""
    registros_cargados: int = 0
    registros_mayor: int = 0
    errores: list = field(default_factory=list)
    staging_id: Optional[int] = None
    duracion_ms: int = 0
    periodos_cargados: list = field(default_factory=list)
    periodos_reemplazados: list = field(default_factory=list)


class StagingService:

    def __init__(self, conn=None):
        self._conn_externo = conn is not None
        self.conn = conn or psycopg2.connect(DATABASE_URL)

    def verificar_periodo(self, empresa_id: int, anio: int, mes: int) -> PeriodoInfo:
        cur = self.conn.cursor()
        try:
            cur.execute("""
                SELECT
                    COUNT(*),
                    COALESCE(SUM(debe), 0),
                    COALESCE(SUM(haber), 0),
                    MIN(cargado_en),
                    MIN(archivo_origen)
                FROM libro_diario
                WHERE empresa_id = %s AND periodo_anio = %s AND periodo_mes = %s
            """, (empresa_id, anio, mes))
            row = cur.fetchone()
            if row and row[0] > 0:
                return PeriodoInfo(
                    existe=True, empresa_id=empresa_id,
                    periodo_anio=anio, periodo_mes=mes,
                    total_registros=row[0], total_debe=float(row[1]),
                    total_haber=float(row[2]), fecha_carga=row[3],
                    archivo_origen=row[4],
                )
            return PeriodoInfo(existe=False, empresa_id=empresa_id, periodo_anio=anio, periodo_mes=mes)
        finally:
            cur.close()

    def verificar_periodos_df(self, df: pd.DataFrame, empresa_id: int) -> List[PeriodoInfo]:
        periodos = sorted(
            df[['periodo_anio', 'periodo_mes']].drop_duplicates().values.tolist()
        )
        return [self.verificar_periodo(empresa_id, anio, mes) for anio, mes in periodos]

    def _cargar_periodo(self, cur, df_mes: pd.DataFrame, empresa_id: int,
                        anio: int, mes: int, archivo_nombre: str,
                        reemplazar: bool) -> tuple:
        info = self.verificar_periodo(empresa_id, anio, mes)

        if info.existe and not reemplazar:
            raise ValueError(
                f"El período {anio}/{mes:02d} ya existe y no se indicó reemplazar."
            )

        accion = 'reemplazo' if info.existe else 'carga_nueva'

        cur.execute("""
            INSERT INTO input_staging
                (empresa_id, periodo_anio, periodo_mes, archivo_nombre,
                 estado, total_registros, total_debe, total_haber, periodo_existia)
            VALUES (%s, %s, %s, %s, 'pendiente', %s, %s, %s, %s)
            RETURNING id
        """, (
            empresa_id, anio, mes, archivo_nombre,
            len(df_mes),
            float(df_mes['debe'].sum()),
            float(df_mes['haber'].sum()),
            info.existe,
        ))
        staging_id = cur.fetchone()[0]

        if info.existe:
            cur.execute("""
                DELETE FROM libro_diario
                WHERE empresa_id = %s AND periodo_anio = %s AND periodo_mes = %s
            """, (empresa_id, anio, mes))

        registros = self._bulk_insert(cur, df_mes, archivo_nombre)

        cur.execute("""
            UPDATE input_staging SET estado = 'procesado', procesado_en = NOW()
            WHERE id = %s
        """, (staging_id,))

        return staging_id, registros, accion

    def ejecutar_carga(
        self,
        df: pd.DataFrame,
        empresa_id: int,
        periodo_anio: int,
        periodo_mes: int,
        archivo_nombre: str,
        reemplazar: bool = False,
    ) -> CargaResult:
        """Carga de un único período (compatibilidad hacia atrás)."""
        df_mes = df[
            (df['periodo_anio'] == periodo_anio) &
            (df['periodo_mes'] == periodo_mes)
        ].copy()
        return self.ejecutar_carga_multiperiodo(
            df=df_mes,
            empresa_id=empresa_id,
            archivo_nombre=archivo_nombre,
            periodos_reemplazar={(periodo_anio, periodo_mes): reemplazar},
        )

    def ejecutar_carga_multiperiodo(
        self,
        df: pd.DataFrame,
        empresa_id: int,
        archivo_nombre: str,
        periodos_reemplazar: dict,
    ) -> CargaResult:
        inicio = time.time()
        resultado = CargaResult(ok=False)
        cur = self.conn.cursor()

        try:
            periodos = sorted(periodos_reemplazar.keys())
            total_cargados = 0
            periodos_cargados = []
            periodos_reemplazados = []

            for anio, mes in periodos:
                reemplazar = periodos_reemplazar[(anio, mes)]
                df_mes = df[
                    (df['periodo_anio'] == anio) &
                    (df['periodo_mes'] == mes)
                ].copy()

                if df_mes.empty:
                    log.warning(f"DataFrame vacío para {anio}/{mes:02d}, saltando.")
                    continue

                staging_id, registros, accion = self._cargar_periodo(
                    cur, df_mes, empresa_id, anio, mes, archivo_nombre, reemplazar
                )

                total_cargados += registros
                periodos_cargados.append((anio, mes, registros))
                if accion == 'reemplazo':
                    periodos_reemplazados.append((anio, mes))

                log.info(f"  ✅ {anio}/{mes:02d} — {registros} registros ({accion})")

            resultado.registros_cargados    = total_cargados
            resultado.periodos_cargados     = periodos_cargados
            resultado.periodos_reemplazados = periodos_reemplazados

            n_reemplazos = len(periodos_reemplazados)
            if n_reemplazos == 0:
                resultado.accion = 'carga_nueva'
            elif n_reemplazos == len(periodos_cargados):
                resultado.accion = 'reemplazo'
            else:
                resultado.accion = 'mixto'

            self.conn.commit()

            anio_desde, mes_desde = periodos[0]
            calc = MayorCalculator(self.conn)
            registros_mayor = calc.recalcular(
                empresa_id=empresa_id,
                desde_anio=anio_desde,
                desde_mes=mes_desde,
                motivo=f"carga_multiperiodo_{len(periodos)}_meses",
            )
            resultado.registros_mayor = registros_mayor
            resultado.duracion_ms = int((time.time() - inicio) * 1000)
            resultado.ok = True

        except Exception as e:
            self.conn.rollback()
            resultado.errores.append(f"Error en la carga: {str(e)}")
            log.error(f"❌ Error: {e}")
            try:
                cur2 = self.conn.cursor()
                if resultado.staging_id:
                    cur2.execute("""
                        UPDATE input_staging SET estado = 'rechazado',
                            errores_json = %s::jsonb
                        WHERE id = %s
                    """, (f'{{"error": "{str(e)}"}}', resultado.staging_id))
                    self.conn.commit()
                cur2.close()
            except Exception:
                pass
        finally:
            cur.close()

        return resultado

    def _bulk_insert(self, cur, df: pd.DataFrame, archivo_nombre: str) -> int:
        ahora = datetime.now()
        registros = []
        for _, row in df.iterrows():
            registros.append((
                int(row['empresa_id']),
                row['fecha'].date() if hasattr(row['fecha'], 'date') else row['fecha'],
                int(row['periodo_anio']),
                int(row['periodo_mes']),
                str(row.get('tipo_asiento', '') or '') or None,
                str(row['nro_asiento']) if pd.notna(row.get('nro_asiento')) else None,
                str(row['nro_renglon']) if pd.notna(row.get('nro_renglon')) else None,
                int(row['cuenta_codigo']),
                float(row['debe']),
                float(row['haber']),
                str(row['descripcion']) if pd.notna(row.get('descripcion')) else None,
                str(row['tipo_subcuenta']) if pd.notna(row.get('tipo_subcuenta')) else None,
                str(row['nro_subcuenta']) if pd.notna(row.get('nro_subcuenta')) else None,
                str(row['centro_costo']) if pd.notna(row.get('centro_costo')) else None,
                ahora,
                archivo_nombre,
            ))

        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO libro_diario (
                empresa_id, fecha, periodo_anio, periodo_mes,
                tipo_asiento, nro_asiento, nro_renglon,
                cuenta_codigo, debe, haber,
                descripcion, tipo_subcuenta, nro_subcuenta, centro_costo,
                cargado_en, archivo_origen
            ) VALUES %s
            """,
            registros,
            page_size=1000
        )
        return len(registros)

    def close(self):
        if not self._conn_externo:
            self.conn.close()