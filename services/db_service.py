"""
Servicio para operaciones de base de datos
"""
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()


class DBService:
    """Servicio para operaciones de base de datos"""
    
    def __init__(self):
        database_url = os.getenv('DATABASE_URL')
        self.engine = create_engine(database_url)
    
    def insertar_libro_diario(self, df: pd.DataFrame, nombre_archivo: str) -> tuple:
        """
        Insertar datos del libro diario en la tabla libro_diario_abierto
        
        Args:
            df: DataFrame normalizado con los datos
            nombre_archivo: Nombre del archivo original
            
        Returns:
            Tupla (éxito, mensaje, registros_insertados)
        """
        try:
            # Registrar inicio en log_cargas
            id_empresa = df['id_empresa'].iloc[0]
            periodo_anio = df['periodo_anio'].iloc[0]
            periodo_mes = df['periodo_mes'].iloc[0]
            total_registros = len(df)
            
            log_id = self._crear_log_carga(
                id_empresa, nombre_archivo, periodo_anio, periodo_mes, total_registros
            )
            
            # Insertar en batch
            registros_insertados = 0
            
            with self.engine.connect() as conn:
                # Insertar registros
                for _, row in df.iterrows():
                    try:
                        conn.execute(
                            text("""
                                INSERT INTO libro_diario_abierto (
                                    id_empresa, fecha_asiento, tipo_asiento, nro_asiento, nro_renglon,
                                    codigo_cuenta, descripcion_cuenta, descripcion_movimiento,
                                    tipo_subcta, nro_subcuenta, tipo_comprobante, sucursal, nro_comprobante,
                                    nombre_tercero, debe, haber, periodo_anio, periodo_mes,
                                    fecha_carga_original, descripcion_asiento, referencia
                                ) VALUES (
                                    :id_empresa, :fecha_asiento, :tipo_asiento, :nro_asiento, :nro_renglon,
                                    :codigo_cuenta, :descripcion_cuenta, :descripcion_movimiento,
                                    :tipo_subcta, :nro_subcuenta, :tipo_comprobante, :sucursal, :nro_comprobante,
                                    :nombre_tercero, :debe, :haber, :periodo_anio, :periodo_mes,
                                    :fecha_carga_original, :descripcion_asiento, :referencia
                                )
                            """),
                            row.to_dict()
                        )
                        registros_insertados += 1
                        
                    except Exception as e:
                        print(f"Error en registro {registros_insertados + 1}: {e}")
                        continue
                
                conn.commit()
            
            # Actualizar log
            self._actualizar_log_carga(log_id, 'completado', registros_insertados, 0)
            
            return True, f"Insertados {registros_insertados} de {total_registros} registros", registros_insertados
            
        except Exception as e:
            # Actualizar log con error
            if 'log_id' in locals():
                self._actualizar_log_carga(log_id, 'error', 0, 0, str(e))
            
            return False, f"Error al insertar: {str(e)}", 0
    
    def calcular_libro_mayor(self, id_empresa: int, anio: int, mes: int) -> tuple:
        """
        Calcular el libro mayor a partir del libro diario
        
        Args:
            id_empresa: ID de la empresa
            anio: Año
            mes: Mes
            
        Returns:
            Tupla (éxito, mensaje)
        """
        try:
            with self.engine.connect() as conn:
                # Obtener saldos anteriores (del mes anterior)
                mes_anterior = mes - 1 if mes > 1 else 12
                anio_anterior = anio if mes > 1 else anio - 1
                
                # Calcular mayor del mes actual
                result = conn.execute(
                    text("""
                        SELECT 
                            codigo_cuenta,
                            SUM(debe) as total_debe,
                            SUM(haber) as total_haber
                        FROM libro_diario_abierto
                        WHERE id_empresa = :id_empresa
                        AND periodo_anio = :anio
                        AND periodo_mes = :mes
                        GROUP BY codigo_cuenta
                    """),
                    {'id_empresa': id_empresa, 'anio': anio, 'mes': mes}
                )
                
                movimientos = result.fetchall()
                
                # Insertar o actualizar en libro_mayor_abierto
                for mov in movimientos:
                    codigo_cuenta = mov[0]
                    total_debe = mov[1]
                    total_haber = mov[2]
                    
                    # Obtener saldo anterior
                    saldo_anterior = self._obtener_saldo_anterior(
                        conn, id_empresa, codigo_cuenta, anio_anterior, mes_anterior
                    )
                    
                    saldo_final = saldo_anterior + total_debe - total_haber
                    
                    # Verificar si ya existe
                    existe = conn.execute(
                        text("""
                            SELECT id FROM libro_mayor_abierto
                            WHERE id_empresa = :id_empresa
                            AND codigo_cuenta = :codigo_cuenta
                            AND periodo_anio = :anio
                            AND periodo_mes = :mes
                        """),
                        {
                            'id_empresa': id_empresa,
                            'codigo_cuenta': codigo_cuenta,
                            'anio': anio,
                            'mes': mes
                        }
                    ).fetchone()
                    
                    if existe:
                        # Actualizar
                        conn.execute(
                            text("""
                                UPDATE libro_mayor_abierto
                                SET saldo_inicial = :saldo_inicial,
                                    total_debe = :total_debe,
                                    total_haber = :total_haber,
                                    saldo_final = :saldo_final,
                                    fecha_calculo = NOW()
                                WHERE id_empresa = :id_empresa
                                AND codigo_cuenta = :codigo_cuenta
                                AND periodo_anio = :anio
                                AND periodo_mes = :mes
                            """),
                            {
                                'id_empresa': id_empresa,
                                'codigo_cuenta': codigo_cuenta,
                                'anio': anio,
                                'mes': mes,
                                'saldo_inicial': saldo_anterior,
                                'total_debe': total_debe,
                                'total_haber': total_haber,
                                'saldo_final': saldo_final
                            }
                        )
                    else:
                        # Insertar
                        conn.execute(
                            text("""
                                INSERT INTO libro_mayor_abierto (
                                    id_empresa, codigo_cuenta, periodo_anio, periodo_mes,
                                    saldo_inicial, total_debe, total_haber, saldo_final
                                ) VALUES (
                                    :id_empresa, :codigo_cuenta, :anio, :mes,
                                    :saldo_inicial, :total_debe, :total_haber, :saldo_final
                                )
                            """),
                            {
                                'id_empresa': id_empresa,
                                'codigo_cuenta': codigo_cuenta,
                                'anio': anio,
                                'mes': mes,
                                'saldo_inicial': saldo_anterior,
                                'total_debe': total_debe,
                                'total_haber': total_haber,
                                'saldo_final': saldo_final
                            }
                        )
                
                conn.commit()
            
            return True, f"Libro mayor calculado: {len(movimientos)} cuentas procesadas"
            
        except Exception as e:
            return False, f"Error al calcular libro mayor: {str(e)}"
    
    def _obtener_saldo_anterior(self, conn, id_empresa: int, codigo_cuenta: str, anio: int, mes: int) -> float:
        """Obtener el saldo final del mes anterior para una cuenta"""
        result = conn.execute(
            text("""
                SELECT saldo_final 
                FROM libro_mayor_abierto
                WHERE id_empresa = :id_empresa
                AND codigo_cuenta = :codigo_cuenta
                AND periodo_anio = :anio
                AND periodo_mes = :mes
            """),
            {
                'id_empresa': id_empresa,
                'codigo_cuenta': codigo_cuenta,
                'anio': anio,
                'mes': mes
            }
        ).fetchone()
        
        return result[0] if result else 0.0
    
    def _crear_log_carga(self, id_empresa: int, nombre_archivo: str, anio: int, mes: int, total_registros: int) -> int:
        """Crear registro en log_cargas"""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO log_cargas (
                        id_empresa, nombre_archivo, periodo_anio, periodo_mes,
                        total_registros, estado
                    ) VALUES (
                        :id_empresa, :nombre_archivo, :anio, :mes,
                        :total_registros, 'procesando'
                    )
                    RETURNING id
                """),
                {
                    'id_empresa': id_empresa,
                    'nombre_archivo': nombre_archivo,
                    'anio': anio,
                    'mes': mes,
                    'total_registros': total_registros
                }
            )
            conn.commit()
            return result.fetchone()[0]
    
    def _actualizar_log_carga(self, log_id: int, estado: str, insertados: int, errores: int, mensaje_error: str = None):
        """Actualizar registro en log_cargas"""
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE log_cargas
                    SET estado = :estado,
                        registros_insertados = :insertados,
                        registros_error = :errores,
                        mensaje_error = :mensaje_error,
                        fecha_fin = NOW()
                    WHERE id = :id
                """),
                {
                    'id': log_id,
                    'estado': estado,
                    'insertados': insertados,
                    'errores': errores,
                    'mensaje_error': mensaje_error
                }
            )
            conn.commit()
    
    def obtener_estadisticas_periodo(self, id_empresa: int, anio: int, mes: int) -> dict:
        """Obtener estadísticas del período"""
        try:
            with self.engine.connect() as conn:
                # Total de movimientos
                result = conn.execute(
                    text("""
                        SELECT COUNT(*), SUM(debe), SUM(haber)
                        FROM libro_diario_abierto
                        WHERE id_empresa = :id_empresa
                        AND periodo_anio = :anio
                        AND periodo_mes = :mes
                    """),
                    {'id_empresa': id_empresa, 'anio': anio, 'mes': mes}
                ).fetchone()
                
                return {
                    'total_movimientos': result[0] or 0,
                    'total_debe': float(result[1] or 0),
                    'total_haber': float(result[2] or 0)
                }
                
        except Exception as e:
            return {
                'total_movimientos': 0,
                'total_debe': 0,
                'total_haber': 0,
                'error': str(e)
            }