"""
staging_service.py
==================
Servicio de staging y carga del Libro Diario para ReporteApp v2.

Responsabilidades:
  1. Detectar si el período ya existe en libro_diario
  2. Registrar el intento de carga en input_staging
  3. Ejecutar la carga (INSERT masivo con COPY)
  4. En caso de reemplazo: DELETE del período existente + INSERT nuevo
  5. Disparar el recálculo del libro_mayor desde el período afectado

Flujo:
    staging = StagingService(conn)

    # Verificar si el período existe
    info = staging.verificar_periodo(empresa, anio, mes)
    if info.existe:
        # Mostrar popup de confirmación en UI con info.resumen
        ...

    # Ejecutar carga (nueva o reemplazo)
    resultado = staging.ejecutar_carga(df, empresa, anio, mes, archivo_nombre, reemplazar=True/False)
"""

import io
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

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
    """Información sobre un período existente en libro_diario."""
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
    """Resultado de una operación de carga."""
    ok: bool
    accion: str = ""           # 'carga_nueva' | 'reemplazo'
    registros_cargados: int = 0
    registros_mayor: int = 0
    errores: list = field(default_factory=list)
    staging_id: Optional[int] = None
    duracion_ms: int = 0


class StagingService:

    def __init__(self, conn=None):
        self._conn_externo = conn is not None
        self.conn = conn or psycopg2.connect(DATABASE_URL)

    # =========================================================================
    # Verificación de período existente
    # =========================================================================

    def verificar_periodo(self, empresa: str, anio: int, mes: int) -> PeriodoInfo:
        """
        Verifica si ya existe un período cargado en libro_diario.
        Devuelve PeriodoInfo con el resumen del período existente para mostrar en UI.
        """
        cur = self.conn.cursor()
        try:
            cur.execute("""
                SELECT
                    COUNT(*)                    AS total_registros,
                    COALESCE(SUM(debe), 0)      AS total_debe,
                    COALESCE(SUM(haber), 0)     AS total_haber,
                    MIN(cargado_en)             AS fecha_carga,
                    MIN(archivo_origen)         AS archivo_origen
                FROM libro_diario
                WHERE empresa = %s
                  AND periodo_anio = %s
                  AND periodo_mes = %s
            """, (empresa, anio, mes))
            row = cur.fetchone()

            if row and row[0] > 0:
                return PeriodoInfo(
                    existe=True,
                    empresa=empresa,
                    periodo_anio=anio,
                    periodo_mes=mes,
                    total_registros=row[0],
                    total_debe=float(row[1]),
                    total_haber=float(row[2]),
                    fecha_carga=row[3],
                    archivo_origen=row[4],
                )
            else:
                return PeriodoInfo(
                    existe=False,
                    empresa=empresa,
                    periodo_anio=anio,
                    periodo_mes=mes,
                )
        finally:
            cur.close()

    # =========================================================================
    # Carga principal
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
        """
        Ejecuta la carga del DataFrame en libro_diario y recalcula el mayor.

        Args:
            df:             DataFrame limpio y validado del FileParser
            empresa:        código de empresa
            periodo_anio:   año del período
            periodo_mes:    mes del período
            archivo_nombre: nombre del archivo original (para auditoría)
            reemplazar:     True = eliminar período existente y recargar
                            False = carga nueva (falla si el período ya existe)

        Returns:
            CargaResult con el resultado de la operación
        """
        import time
        inicio = time.time()
        resultado = CargaResult(ok=False)
        cur = self.conn.cursor()

        try:
            # 1. Verificar si el período existe
            info = self.verificar_periodo(empresa, periodo_anio, periodo_mes)

            if info.existe and not reemplazar:
                resultado.errores.append(
                    f"El período {periodo_anio}/{periodo_mes:02d} de {empresa} ya existe "
                    f"({info.total_registros} registros). "
                    f"Confirmá el reemplazo para continuar."
                )
                return resultado

            accion = 'reemplazo' if info.existe else 'carga_nueva'
            resultado.accion = accion

            # 2. Registrar en input_staging
            cur.execute("""
                INSERT INTO input_staging
                    (empresa, periodo_anio, periodo_mes, archivo_nombre,
                     estado, total_registros, total_debe, total_haber, periodo_existia)
                VALUES (%s, %s, %s, %s, 'pendiente', %s, %s, %s, %s)
                RETURNING id
            """, (
                empresa, periodo_anio, periodo_mes, archivo_nombre,
                len(df),
                float(df['debe'].sum()),
                float(df['haber'].sum()),
                info.existe,
            ))
            staging_id = cur.fetchone()[0]
            resultado.staging_id = staging_id
            log.info(f"Staging registrado — id={staging_id} — acción={accion}")

            # 3. Si es reemplazo, eliminar período existente
            if info.existe:
                cur.execute("""
                    DELETE FROM libro_diario
                    WHERE empresa = %s
                      AND periodo_anio = %s
                      AND periodo_mes = %s
                """, (empresa, periodo_anio, periodo_mes))
                log.info(f"Período eliminado: {empresa} {periodo_anio}/{periodo_mes:02d} "
                         f"({info.total_registros} registros)")

            # 4. Cargar nuevo período con COPY (bulk insert)
            registros_cargados = self._bulk_insert(cur, df, archivo_nombre)
            resultado.registros_cargados = registros_cargados

            # 5. Actualizar staging a 'procesado'
            cur.execute("""
                UPDATE input_staging
                SET estado = 'procesado', procesado_en = NOW()
                WHERE id = %s
            """, (staging_id,))

            # 6. Commit antes de recalcular mayor
            self.conn.commit()
            log.info(f"✅ Carga completada — {registros_cargados} registros")

            # 7. Recalcular libro_mayor desde este período en adelante
            motivo = 'reemplazo_periodo' if accion == 'reemplazo' else 'carga_nueva'
            calc = MayorCalculator(self.conn)
            registros_mayor = calc.recalcular(
                empresa=empresa,
                desde_anio=periodo_anio,
                desde_mes=periodo_mes,
                motivo=motivo
            )
            resultado.registros_mayor = registros_mayor

            resultado.duracion_ms = int((time.time() - inicio) * 1000)
            resultado.ok = True
            log.info(f"✅ Proceso completo — mayor: {registros_mayor} registros — "
                     f"{resultado.duracion_ms}ms")

        except Exception as e:
            self.conn.rollback()
            resultado.errores.append(f"Error en la carga: {str(e)}")
            log.error(f"❌ Error en carga: {e}")

            # Actualizar staging a 'rechazado'
            try:
                cur2 = self.conn.cursor()
                if resultado.staging_id:
                    cur2.execute("""
                        UPDATE input_staging
                        SET estado = 'rechazado',
                            errores_json = %s::jsonb
                        WHERE id = %s
                    """, (
                        f'{{"error": "{str(e)}"}}',
                        resultado.staging_id
                    ))
                    self.conn.commit()
                cur2.close()
            except Exception:
                pass

        finally:
            cur.close()

        return resultado

    # =========================================================================
    # Bulk insert con COPY
    # =========================================================================

    def _bulk_insert(self, cur, df: pd.DataFrame, archivo_nombre: str) -> int:
        """
        Carga masiva usando execute_values para máxima performance.
        Agrega archivo_origen y cargado_en a cada fila.
        """
        ahora = datetime.now()

        # Preparar registros
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