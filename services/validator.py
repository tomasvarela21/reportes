"""
Servicio para validar datos del libro diario
"""
import pandas as pd
from typing import Tuple, List
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()


class Validator:
    """Clase para validar datos del libro diario"""
    
    def __init__(self):
        database_url = os.getenv('DATABASE_URL')
        self.engine = create_engine(database_url)
    
    def validar_empresa_existe(self, codigo_empresa: str) -> Tuple[bool, str, int]:
        """
        Validar que la empresa exista en la base de datos
        
        Args:
            codigo_empresa: Código de la empresa
            
        Returns:
            Tupla (existe, mensaje, id_empresa)
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT id, nombre FROM dim_empresa WHERE codigo = :codigo AND activa = TRUE"),
                    {'codigo': codigo_empresa}
                ).fetchone()
                
                if result:
                    return True, f"Empresa encontrada: {result[1]}", result[0]
                else:
                    return False, f"Empresa '{codigo_empresa}' no existe en la base de datos", 0
                    
        except Exception as e:
            return False, f"Error al validar empresa: {str(e)}", 0
    
    def validar_balance(self, df: pd.DataFrame) -> Tuple[bool, str]:
        """
        Validar que el debe y haber estén balanceados
        
        Args:
            df: DataFrame con los datos
            
        Returns:
            Tupla (es_valido, mensaje)
        """
        total_debe = df['debe'].sum()
        total_haber = df['haber'].sum()
        diferencia = abs(total_debe - total_haber)
        
        # Tolerancia de 0.01 por redondeos
        if diferencia > 0.01:
            return False, f"El libro diario no está balanceado. Debe: {total_debe:,.2f} | Haber: {total_haber:,.2f} | Diferencia: {diferencia:,.2f}"
        
        return True, "Debe y Haber balanceados correctamente"
    
    def validar_fechas(self, df: pd.DataFrame, mes: int, anio: int) -> Tuple[bool, str]:
        """
        Validar que las fechas correspondan al período
        
        Args:
            df: DataFrame con los datos
            mes: Mes esperado
            anio: Año esperado
            
        Returns:
            Tupla (es_valido, mensaje)
        """
        # Verificar que no haya fechas nulas
        if df['fecha_asiento'].isna().any():
            return False, "Hay fechas faltantes en el archivo"
        
        # Verificar que las fechas estén en el mes/año correcto
        fechas_incorrectas = df[
            (df['fecha_asiento'].dt.month != mes) | 
            (df['fecha_asiento'].dt.year != anio)
        ]
        
        if len(fechas_incorrectas) > 0:
            return False, f"Hay {len(fechas_incorrectas)} registros con fechas fuera del período {mes}/{anio}"
        
        return True, f"Todas las fechas corresponden al período {mes}/{anio}"
    
    def validar_cuentas_existen(self, df: pd.DataFrame) -> Tuple[bool, str, List[str]]:
        """
        Validar que las cuentas existan en el plan de cuentas
        
        Args:
            df: DataFrame con los datos
            
        Returns:
            Tupla (todas_existen, mensaje, cuentas_faltantes)
        """
        try:
            # Obtener cuentas únicas del archivo
            cuentas_archivo = df['codigo_cuenta'].unique().tolist()
            
            # Obtener cuentas de la base de datos
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT codigo FROM dim_cuenta WHERE activa = TRUE")
                )
                cuentas_db = [row[0] for row in result]
            
            # Verificar cuáles faltan
            cuentas_faltantes = [c for c in cuentas_archivo if c not in cuentas_db]
            
            if cuentas_faltantes:
                return False, f"Hay {len(cuentas_faltantes)} cuentas que no existen en el plan de cuentas", cuentas_faltantes
            
            return True, "Todas las cuentas existen en el plan de cuentas", []
            
        except Exception as e:
            return False, f"Error al validar cuentas: {str(e)}", []
    
    def validar_duplicados(self, df: pd.DataFrame, id_empresa: int, mes: int, anio: int) -> Tuple[bool, str]:
        """
        Validar que no haya datos duplicados en la base de datos
        
        Args:
            df: DataFrame con los datos
            id_empresa: ID de la empresa
            mes: Mes
            anio: Año
            
        Returns:
            Tupla (no_hay_duplicados, mensaje)
        """
        try:
            with self.engine.connect() as conn:
                # Verificar si ya existe data para esta empresa/período
                result = conn.execute(
                    text("""
                        SELECT COUNT(*) 
                        FROM libro_diario_abierto 
                        WHERE id_empresa = :id_empresa 
                        AND periodo_anio = :anio 
                        AND periodo_mes = :mes
                    """),
                    {'id_empresa': id_empresa, 'anio': anio, 'mes': mes}
                ).fetchone()
                
                registros_existentes = result[0]
                
                if registros_existentes > 0:
                    return False, f"Ya existen {registros_existentes} registros para este período. Eliminalos primero si querés recargar."
                
                return True, "No hay datos duplicados"
                
        except Exception as e:
            return False, f"Error al validar duplicados: {str(e)}"
    
    def validar_todo(self, df: pd.DataFrame, codigo_empresa: str, mes: int, anio: int) -> Tuple[bool, str, int]:
        """
        Ejecutar todas las validaciones
        
        Args:
            df: DataFrame con los datos
            codigo_empresa: Código de la empresa
            mes: Mes
            anio: Año
            
        Returns:
            Tupla (es_valido, mensaje, id_empresa)
        """
        errores = []
        
        # 1. Validar empresa
        existe, msg, id_empresa = self.validar_empresa_existe(codigo_empresa)
        if not existe:
            return False, msg, 0
        
        # 2. Validar balance
        es_valido, msg = self.validar_balance(df)
        if not es_valido:
            errores.append(msg)
        
        # 3. Validar fechas
        es_valido, msg = self.validar_fechas(df, mes, anio)
        if not es_valido:
            errores.append(msg)
        
        # 4. Validar cuentas
        todas_existen, msg, faltantes = self.validar_cuentas_existen(df)
        if not todas_existen:
            errores.append(msg)
            if len(faltantes) <= 10:
                errores.append(f"Cuentas faltantes: {', '.join(faltantes)}")
        
        # 5. Validar duplicados
        no_duplicados, msg = self.validar_duplicados(df, id_empresa, mes, anio)
        if not no_duplicados:
            errores.append(msg)
        
        if errores:
            return False, "\n".join(errores), id_empresa
        
        return True, "Todas las validaciones pasaron correctamente ✅", id_empresa