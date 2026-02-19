"""
Servicio para operaciones de base de datos
OPTIMIZADO con Batch INSERT para máxima velocidad
"""
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import time
from dotenv import load_dotenv
from psycopg2.extensions import register_adapter, AsIs

# Registrar adaptador para numpy types
register_adapter(np.int64, lambda x: AsIs(int(x)))
register_adapter(np.int32, lambda x: AsIs(int(x)))
register_adapter(np.float64, lambda x: AsIs(float(x)))
register_adapter(np.float32, lambda x: AsIs(float(x)))

load_dotenv()


class DBService:
    """Servicio para operaciones de base de datos"""
    
    def __init__(self):
        database_url = os.getenv('DATABASE_URL')
        
        # Configurar engine con reconexión automática
        self.engine = create_engine(
            database_url,
            pool_pre_ping=True,  # Verifica conexión antes de usar
            pool_recycle=3600,    # Recicla conexiones cada hora
            connect_args={
                'connect_timeout': 10
            }
        )
    
    def insertar_libro_diario(self, df: pd.DataFrame, nombre_archivo: str, callback_progreso=None) -> tuple:
        """
        Insertar datos del libro diario en la tabla libro_diario_abierto
        OPTIMIZADO con Batch INSERT (10x más rápido)
        
        Args:
            df: DataFrame normalizado con los datos
            nombre_archivo: Nombre del archivo original
            callback_progreso: Función opcional para reportar progreso (actual, total)
            
        Returns:
            Tupla (éxito, mensaje, registros_insertados)
        """
        try:
            # Registrar inicio en log_cargas
            id_empresa = int(df['id_empresa'].iloc[0])
            periodo_anio = int(df['periodo_anio'].iloc[0])
            periodo_mes = int(df['periodo_mes'].iloc[0])
            total_registros = int(len(df))
            
            log_id = self._crear_log_carga(
                id_empresa, nombre_archivo, periodo_anio, periodo_mes, total_registros
            )
            
            # Preparar datos en batch
            registros_insertados = 0
            batch_size = 500  # Insertar de a 500 registros (óptimo para Neon)
            
            # Convertir todo el DataFrame a lista de diccionarios
            print(f"📦 Preparando {total_registros} registros para inserción en batches...")
            registros = []
            for _, row in df.iterrows():
                row_dict = row.to_dict()
                
                # Convertir numpy types a Python types
                for key, value in row_dict.items():
                    if pd.isna(value):
                        row_dict[key] = None
                    elif hasattr(value, 'item'):
                        row_dict[key] = value.item()
                
                registros.append(row_dict)
            
            print(f"✅ Registros preparados. Iniciando inserción por batches...")
            
            # SQL de inserción
            sql = text("""
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
            """)
            
            with self.engine.connect() as conn:
                # Insertar en batches
                num_batches = (len(registros) + batch_size - 1) // batch_size
                
                for batch_num, i in enumerate(range(0, len(registros), batch_size), 1):
                    batch = registros[i:i + batch_size]
                    
                    try:
                        # executemany - inserta todo el batch de una vez
                        conn.execute(sql, batch)
                        registros_insertados += len(batch)
                        
                        print(f"✅ Batch {batch_num}/{num_batches}: {len(batch)} registros insertados")
                        
                        # Reportar progreso
                        if callback_progreso:
                            callback_progreso(registros_insertados, total_registros)
                        
                    except Exception as e:
                        print(f"❌ Error en batch {batch_num}: {e}")
                        continue
                
                # Commit al final de todos los batches
                conn.commit()
                print(f"💾 Commit realizado: {registros_insertados} registros guardados")
            
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
        Calcular el libro mayor ABIERTO a partir del libro diario ABIERTO
        
        IMPORTANTE: Solo calcula desde libro_diario_abierto → libro_mayor_abierto
        El libro mayor histórico se calcula por separado durante el cierre semestral
        
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
                
                print(f"📊 Calculando libro mayor ABIERTO para {mes}/{anio}...")
                
                # Calcular mayor del mes actual desde libro_diario_abierto
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
                print(f"✅ {len(movimientos)} cuentas encontradas en el libro diario abierto")
                
                # Insertar o actualizar en libro_mayor_abierto
                cuentas_procesadas = 0
                for mov in movimientos:
                    codigo_cuenta = mov[0]
                    total_debe = float(mov[1]) if mov[1] else 0.0
                    total_haber = float(mov[2]) if mov[2] else 0.0
                    
                    # Obtener saldo anterior (del mes anterior en libro_mayor_abierto)
                    saldo_anterior = self._obtener_saldo_anterior(
                        conn, id_empresa, codigo_cuenta, anio_anterior, mes_anterior
                    )
                    
                    # Convertir saldo_anterior a float también
                    saldo_anterior = float(saldo_anterior)
                    
                    saldo_final = saldo_anterior + total_debe - total_haber
                    
                    # Verificar si ya existe en libro_mayor_abierto
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
                        # Actualizar en libro_mayor_abierto
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
                        # Insertar en libro_mayor_abierto
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
                    
                    cuentas_procesadas += 1
                
                conn.commit()
                print(f"💾 Libro mayor abierto guardado: {cuentas_procesadas} cuentas")
            
            return True, f"Libro mayor calculado: {len(movimientos)} cuentas procesadas"
            
        except Exception as e:
            return False, f"Error al calcular libro mayor: {str(e)}"
    
    def obtener_libro_mayor(self, id_empresa: int, anio: int, mes: int) -> pd.DataFrame:
        """
        Obtener el libro mayor ABIERTO calculado para un período
        
        IMPORTANTE: Solo consulta libro_mayor_abierto (datos recientes)
        Para ver todo (abierto + histórico) usar obtener_libro_mayor_completo()
        
        Args:
            id_empresa: ID de la empresa
            anio: Año
            mes: Mes
            
        Returns:
            DataFrame con el libro mayor abierto
        """
        try:
            # Consultar SOLO libro_mayor_abierto
            query = text("""
                SELECT 
                    lm.codigo_cuenta,
                    dc.nombre as nombre_cuenta,
                    lm.saldo_inicial,
                    lm.total_debe,
                    lm.total_haber,
                    lm.saldo_final,
                    lm.fecha_calculo
                FROM libro_mayor_abierto lm
                LEFT JOIN dim_cuenta dc ON lm.codigo_cuenta = dc.codigo
                WHERE lm.id_empresa = :id_empresa
                AND lm.periodo_anio = :anio
                AND lm.periodo_mes = :mes
                ORDER BY lm.codigo_cuenta
            """)
            
            df = pd.read_sql(
                query,
                self.engine,
                params={'id_empresa': id_empresa, 'anio': anio, 'mes': mes}
            )
            
            return df
            
        except Exception as e:
            print(f"Error al obtener libro mayor: {e}")
            return pd.DataFrame()
    
    def obtener_libro_mayor_completo(self, id_empresa: int, anio: int, mes: int) -> pd.DataFrame:
        """
        Obtener el libro mayor COMPLETO (abierto + histórico) para un período
        
        Usa la vista consolidada v_libro_mayor_completo
        
        Args:
            id_empresa: ID de la empresa
            anio: Año
            mes: Mes
            
        Returns:
            DataFrame con el libro mayor completo (abierto + histórico)
        """
        try:
            # Consultar vista consolidada
            query = text("""
                SELECT 
                    lm.codigo_cuenta,
                    dc.nombre as nombre_cuenta,
                    lm.saldo_inicial,
                    lm.total_debe,
                    lm.total_haber,
                    lm.saldo_final,
                    lm.fecha_calculo
                FROM v_libro_mayor_completo lm
                LEFT JOIN dim_cuenta dc ON lm.codigo_cuenta = dc.codigo
                WHERE lm.id_empresa = :id_empresa
                AND lm.periodo_anio = :anio
                AND lm.periodo_mes = :mes
                ORDER BY lm.codigo_cuenta
            """)
            
            df = pd.read_sql(
                query,
                self.engine,
                params={'id_empresa': id_empresa, 'anio': anio, 'mes': mes}
            )
            
            return df
            
        except Exception as e:
            print(f"Error al obtener libro mayor completo: {e}")
            return pd.DataFrame()
    
    def _obtener_saldo_anterior(self, conn, id_empresa: int, codigo_cuenta: str, anio: int, mes: int) -> float:
        """
        Obtener el saldo final del mes anterior para una cuenta
        Consulta SOLO libro_mayor_abierto
        """
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
                        :id_empresa, :nombre_archivo, :periodo_anio, :periodo_mes,
                        :total_registros, 'procesando'
                    )
                    RETURNING id
                """),
                {
                    'id_empresa': int(id_empresa),
                    'nombre_archivo': str(nombre_archivo),
                    'periodo_anio': int(anio),
                    'periodo_mes': int(mes),
                    'total_registros': int(total_registros)
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
                    WHERE id = :log_id
                """),
                {
                    'log_id': log_id,
                    'estado': estado,
                    'insertados': insertados,
                    'errores': errores,
                    'mensaje_error': mensaje_error
                }
            )
            conn.commit()
    
    def obtener_estadisticas_periodo(self, id_empresa: int, anio: int, mes: int) -> dict:
        """
        Obtener estadísticas del período
        Consulta SOLO libro_diario_abierto
        """
        try:
            with self.engine.connect() as conn:
                # Total de movimientos desde libro_diario_abierto
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