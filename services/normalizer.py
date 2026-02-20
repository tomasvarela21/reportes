"""
Servicio para normalizar datos del libro diario
"""
import pandas as pd
from typing import Dict


class Normalizer:
    """Clase para normalizar datos antes de insertar en la DB"""
    
    @staticmethod
    def normalizar_para_db(df: pd.DataFrame, id_empresa: int) -> pd.DataFrame:
        """
        Normalizar DataFrame para inserción en la base de datos
        
        Args:
            df: DataFrame con datos limpios
            id_empresa: ID de la empresa
            
        Returns:
            DataFrame normalizado para inserción
        """
        df = df.copy()
        
        # Agregar ID de empresa
        df['id_empresa'] = id_empresa
        
        # Asegurar que los campos numéricos sean del tipo correcto
        df['debe'] = pd.to_numeric(df['debe'], errors='coerce').fillna(0)
        df['haber'] = pd.to_numeric(df['haber'], errors='coerce').fillna(0)
        
        # Convertir campos opcionales a None si están vacíos
        df['nombre_tercero'] = df['nombre_tercero'].replace('', None)
        df['descripcion_movimiento'] = df['descripcion_movimiento'].replace('', None)
        df['descripcion_asiento'] = df['descripcion_asiento'].replace('', None)
        
        # Convertir 0 a None en campos numéricos opcionales (sin tipo_subcta)
        df['nro_subcuenta'] = df['nro_subcuenta'].replace(0, None)
        df['tipo_comprobante'] = df['tipo_comprobante'].replace(0, None)
        df['sucursal'] = df['sucursal'].replace(0, None)
        df['nro_comprobante'] = df['nro_comprobante'].replace(0, None)
        
        # Reordenar columnas en el orden de la tabla
        columnas_db = [
            'id_empresa',
            'fecha_asiento',
            'tipo_asiento',
            'nro_asiento',
            'nro_renglon',
            'codigo_cuenta',
            'descripcion_cuenta',
            'descripcion_movimiento',
            'nro_subcuenta',
            'tipo_comprobante',
            'sucursal',
            'nro_comprobante',
            'nombre_tercero',
            'debe',
            'haber',
            'periodo_anio',
            'periodo_mes',
            'fecha_carga_original',
            'descripcion_asiento',
            'referencia'
        ]
        
        return df[columnas_db]
    
    @staticmethod
    def preparar_batch(df: pd.DataFrame, batch_size: int = 1000) -> list:
        """Dividir DataFrame en lotes para inserción"""
        batches = []
        total_rows = len(df)
        
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i + batch_size]
            batches.append(batch)
        
        return batches
    
    @staticmethod
    def convertir_a_dict(df: pd.DataFrame) -> list:
        """Convertir DataFrame a lista de diccionarios para inserción"""
        df = df.where(pd.notna(df), None)
        return df.to_dict('records')