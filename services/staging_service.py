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


@dataclass
class PeriodoInfo:
    existe: bool
    empresa: str
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
    accion: str = ""           # 'carga_nueva' | 'reemplazo' | 'mixto'
    registros_cargados: int = 0
    registros_mayor: int = 0
    errores: list = field(default_factory=list)
    staging_id: Optional[int] = None
    duracion_ms: int = 0
    periodos_cargados: list = field(default_factory=list)   # [(anio, mes, n_registros)]
    periodos_reemplazados: list = field(default_factory=list)


class StagingService:

    def __init__(self, conn=None):
        self._conn_externo = conn is not None
        self.conn = conn or psycopg2.connect(DATABASE_URL)

    # =========================================================================
    # Verificación de período existente (un solo mes)
    # =========================================================================

    def verificar_periodo(self, empresa: str, anio: int, mes: int) -> PeriodoInfo:
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
                WHERE empresa = %s AND periodo_anio = %s AND periodo_mes = %s
            """, (empresa, anio, mes))
            row = cur.fetchone()
            if row and row[0] > 0:
                return PeriodoInfo(
                    existe=True, empresa=empresa,
                    periodo_anio=anio, periodo_mes=mes,
                    total_registros=row[0], total_debe=float(row[1]),
                    total_haber=float(row[2]), fecha_carga=row[3],
                    archivo_origen=row[4],
                )
            return PeriodoInfo(existe=False, empresa=empresa, periodo_anio=anio, periodo_mes=mes)
        finally:
            cur.close()

    # =========================================================================
    # Verificación de múltiples períodos de un DataFrame
    # =========================================================================

    def verificar_periodos_df(self, df: pd.DataFrame, empresa: str) -> List[PeriodoInfo]:
        """
        Dado un DataFrame con múltiples períodos, devuelve PeriodoInfo para cada uno.
        Los períodos se devuelven ordenados cronológicamente.
        """
        periodos = sorted(
            df[['periodo_anio', 'periodo_mes']].drop_duplicates().values.tolist()
        )
        return [self.verificar_periodo(empresa, anio, mes) for anio, mes in periodos]

    # =========================================================================
    # Carga de UN solo período (uso interno)
    # =========================================================================

    def _cargar_periodo(self, cur, df_mes: pd.DataFrame, empresa: str,
                        anio: int, mes: int, archivo_nombre: str,
                        reemplazar: bool) -> tuple:
        """
        Carga un período específico. Devuelve (staging_id, registros_cargados, accion).
        """
        info = self.verificar_periodo(empresa, anio, mes)

        if info.existe and not reemplazar:
            raise ValueError(
                f"El período {anio}/{mes:02d} ya existe y no se indicó reemplazar."
            )

        accion = 'reemplazo' if info.existe else 'carga_nueva'

        # Registrar en staging
        cur.execute("""
            INSERT INTO input_staging
                (empresa, periodo_anio, periodo_mes, archivo_nombre,
                 estado, total_registros, total_debe, total_haber, periodo_existia)
            VALUES (%s, %s, %s, %s, 'pendiente', %s, %s, %s, %s)
            RETURNING id
        """, (
            empresa, anio, mes, archivo_nombre,
            len(df_mes),
            float(df_mes['debe'].sum()),
            float(df_mes['haber'].sum()),
            info.existe,
        ))
        staging_id = cur.fetchone()[0]

        # Eliminar si es reemplazo
        if info.existe:
            cur.execute("""
                DELETE FROM libro_diario
                WHERE empresa = %s AND periodo_anio = %s AND periodo_mes = %s
            """, (empresa, anio, mes))

        # Insertar
        registros = self._bulk_insert(cur, df_mes, archivo_nombre)

        # Actualizar staging
        cur.execute("""
            UPDATE input_staging SET estado = 'procesado', procesado_en = NOW()
            WHERE id = %s
        """, (staging_id,))

        return staging_id, registros, accion

    # =========================================================================
    # Carga principal — soporta uno o múltiples períodos
    # =========================================================================

    def ejecutar_carga(
        self,
        df: pd.DataFrame,
        empresa: str,
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
            empresa=empresa,
            archivo_nombre=archivo_nombre,
            periodos_reemplazar={(periodo_anio, periodo_mes): reemplazar},
        )

    def ejecutar_carga_multiperiodo(
        self,
        df: pd.DataFrame,
        empresa: str,
        archivo_nombre: str,
        periodos_reemplazar: dict,  # {(anio, mes): bool} — True = reemplazar
    ) -> CargaResult:
        """
        Carga múltiples períodos de un DataFrame.

        periodos_reemplazar: dict donde la clave es (anio, mes) y el valor
        indica si se debe reemplazar ese período si ya existe.

        El recálculo del mayor se hace UNA sola vez desde el período más viejo,
        para que el saldo_acumulado se arrastre correctamente mes a mes.
        """
        inicio = time.time()
        resultado = CargaResult(ok=False)
        cur = self.conn.cursor()

        try:
            # Períodos ordenados cronológicamente
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
                    cur, df_mes, empresa, anio, mes, archivo_nombre, reemplazar
                )

                total_cargados += registros
                periodos_cargados.append((anio, mes, registros))
                if accion == 'reemplazo':
                    periodos_reemplazados.append((anio, mes))

                log.info(f"  ✅ {anio}/{mes:02d} — {registros} registros ({accion})")

            resultado.registros_cargados = total_cargados
            resultado.periodos_cargados  = periodos_cargados
            resultado.periodos_reemplazados = periodos_reemplazados

            # Determinar acción global
            n_reemplazos = len(periodos_reemplazados)
            if n_reemplazos == 0:
                resultado.accion = 'carga_nueva'
            elif n_reemplazos == len(periodos_cargados):
                resultado.accion = 'reemplazo'
            else:
                resultado.accion = 'mixto'

            # Commit de todos los inserts
            self.conn.commit()
            log.info(f"✅ Todos los períodos cargados — {total_cargados} registros")

            # Recálculo del mayor UNA sola vez desde el período más viejo
            # Así el saldo_acumulado se arrastra correctamente mes a mes
            anio_desde, mes_desde = periodos[0]
            motivo = f"carga_multiperiodo_{len(periodos)}_meses"

            calc = MayorCalculator(self.conn)
            registros_mayor = calc.recalcular(
                empresa=empresa,
                desde_anio=anio_desde,
                desde_mes=mes_desde,
                motivo=motivo,
            )
            resultado.registros_mayor = registros_mayor

            resultado.duracion_ms = int((time.time() - inicio) * 1000)
            resultado.ok = True
            log.info(f"✅ Mayor recalculado desde {anio_desde}/{mes_desde:02d} "
                     f"— {registros_mayor} registros — {resultado.duracion_ms}ms")

        except Exception as e:
            self.conn.rollback()
            resultado.errores.append(f"Error en la carga: {str(e)}")
            log.error(f"❌ Error: {e}")

            # Marcar staging como rechazado si aplica
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

    # =========================================================================
    # Bulk insert
    # =========================================================================

    def _bulk_insert(self, cur, df: pd.DataFrame, archivo_nombre: str) -> int:
        ahora = datetime.now()
        registros = []
        for _, row in df.iterrows():
            registros.append((
                str(row['empresa']),
                row['fecha'].date() if hasattr(row['fecha'], 'date') else row['fecha'],
                int(row['periodo_anio']),
                int(row['periodo_mes']),
                str(row['nro_asiento']) if pd.notna(row.get('nro_asiento')) else None,
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
                empresa, fecha, periodo_anio, periodo_mes,
                nro_asiento, cuenta_codigo, debe, haber,
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