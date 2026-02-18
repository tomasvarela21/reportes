"""
Servicio para parsear archivos CSV del libro diario
"""
import pandas as pd
from typing import Tuple, Optional
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import (
    parse_filename, 
    convert_decimal_string, 
    parse_date_string,
    validar_estructura_csv
)


class FileParser:
    """Clase para parsear archivos del libro diario"""
    
    def __init__(self):
        self.df = None
        self.empresa = None
        self.mes = None
        self.anio = None
    
    def parse_csv(self, filepath: str, filename: str) -> Tuple[bool, str, Optional[pd.DataFrame]]:
        """
        Parsear archivo CSV del libro diario
        
        Args:
            filepath: Ruta completa al archivo
            filename: Nombre del archivo (para extraer empresa y período)
            
        Returns:
            Tupla (éxito, mensaje, dataframe)
        """
        try:
            # Parsear nombre de archivo
            empresa, mes, anio = parse_filename(filename)
            
            if not empresa or not mes or not anio:
                return False, "Formato de nombre de archivo incorrecto. Debe ser: diario_EmpresaA_01-2025.csv", None
            
            self.empresa = empresa
            self.mes = mes
            self.anio = anio
            
            # Leer CSV
            # El archivo usa separador ; y encoding latin1
            df = pd.read_csv(filepath, sep=';', encoding='latin1')
            
            # Validar estructura
            es_valido, mensaje = validar_estructura_csv(df)
            if not es_valido:
                return False, mensaje, None
            
            # Limpiar datos
            df = self._limpiar_datos(df)
            
            self.df = df
            
            return True, f"Archivo parseado correctamente: {len(df)} registros", df
            
        except Exception as e:
            return False, f"Error al parsear archivo: {str(e)}", None
    
    def _limpiar_datos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpiar y transformar los datos del CSV
        
        Args:
            df: DataFrame con datos crudos
            
        Returns:
            DataFrame limpio
        """
        # Crear copia para no modificar el original
        df = df.copy()
        
        # Convertir fecha
        df['fecha_asiento'] = df['Fecasi'].apply(
            lambda x: parse_date_string(x) if pd.notna(x) else None
        )
        
        # Convertir debe y haber (formato argentino con coma)
        df['debe_num'] = df['debe'].apply(convert_decimal_string)
        df['haber_num'] = df['haber'].apply(
            lambda x: abs(convert_decimal_string(x))  # Convertir haber a positivo
        )
        
        # Convertir fecha de carga original
        df['fecha_carga_original'] = df['feccar'].apply(
            lambda x: parse_date_string(x, '%Y/%m/%d %H:%M:%S') if pd.notna(x) else None
        )
        
        # Limpiar campos de texto
        df['descripcion_cuenta'] = df['desc_pdc'].str.strip()
        df['descripcion_movimiento'] = df['descrip_movs'].fillna('').str.strip()
        df['descripcion_asiento'] = df['descrip_asis'].fillna('').str.strip()
        df['nombre_tercero'] = df['nombre'].fillna('').str.strip()
        
        # Código de cuenta como string
        df['codigo_cuenta'] = df['nro_cta'].astype(str)
        
        # Campos numéricos opcionales
        df['tipo_subcta'] = df['tipo_subcta'].fillna(0).astype(int)
        df['nro_subcuenta'] = df['nro_subcuenta'].fillna(0)
        df['tipo_comprobante'] = df['tipo_comp'].fillna(0).astype(int)
        df['sucursal'] = df['sucursal'].fillna(0).astype(int)
        df['nro_comprobante'] = df['nrocomp'].fillna(0)
        df['referencia'] = df['referencia'].fillna(0).astype(str)
        
        # Agregar período
        df['periodo_anio'] = self.anio
        df['periodo_mes'] = self.mes
        
        return df
    
    def get_resumen(self) -> dict:
        """
        Obtener resumen de los datos parseados
        
        Returns:
            Diccionario con resumen
        """
        if self.df is None:
            return {}
        
        return {
            'empresa': self.empresa,
            'mes': self.mes,
            'anio': self.anio,
            'total_registros': len(self.df),
            'total_debe': self.df['debe_num'].sum(),
            'total_haber': self.df['haber_num'].sum(),
            'diferencia': abs(self.df['debe_num'].sum() - self.df['haber_num'].sum()),
            'fecha_min': self.df['fecha_asiento'].min(),
            'fecha_max': self.df['fecha_asiento'].max(),
            'asientos_unicos': self.df['nro_asiento'].nunique(),
            'cuentas_unicas': self.df['codigo_cuenta'].nunique()
        }
    
    def get_dataframe_limpio(self) -> Optional[pd.DataFrame]:
        """
        Obtener DataFrame con solo las columnas necesarias para la DB
        
        Returns:
            DataFrame con columnas para inserción
        """
        if self.df is None:
            return None
        
        columnas = [
            'fecha_asiento',
            'tipo_asiento',
            'nro_asiento',
            'Nro_renglon',  # nro_renglon
            'codigo_cuenta',
            'descripcion_cuenta',
            'descripcion_movimiento',
            'tipo_subcta',
            'nro_subcuenta',
            'tipo_comprobante',
            'sucursal',
            'nro_comprobante',
            'nombre_tercero',
            'debe_num',
            'haber_num',
            'periodo_anio',
            'periodo_mes',
            'fecha_carga_original',
            'descripcion_asiento',
            'referencia'
        ]
        
        df_limpio = self.df[columnas].copy()
        
        # Renombrar columnas para que coincidan con la DB
        df_limpio = df_limpio.rename(columns={
            'Nro_renglon': 'nro_renglon',
            'debe_num': 'debe',
            'haber_num': 'haber'
        })
        
        return df_limpio