"""
Funciones auxiliares para el sistema de reportes
"""
import re
from datetime import datetime
from typing import Tuple, Optional

def parse_filename(filename: str) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    """
    Parsear el nombre del archivo para extraer empresa, mes y año
    
    Formato esperado: diario_EmpresaA_01-2025.csv
    
    Args:
        filename: Nombre del archivo
        
    Returns:
        Tupla (empresa, mes, año) o (None, None, None) si no se puede parsear
    """
    try:
        # Patrón: diario_EMPRESA_MM-YYYY.csv
        pattern = r'diario_(.+?)_(\d{2})-(\d{4})\.csv'
        match = re.match(pattern, filename.lower())
        
        if match:
            empresa = match.group(1).upper()
            mes = int(match.group(2))
            anio = int(match.group(3))
            
            # Validar mes
            if mes < 1 or mes > 12:
                return None, None, None
            
            return empresa, mes, anio
        
        return None, None, None
    except Exception as e:
        print(f"Error parseando nombre de archivo: {e}")
        return None, None, None


def convert_decimal_string(value: str) -> float:
    """
    Convertir string con formato argentino (coma decimal) a float
    
    Args:
        value: String con formato "1.234,56" o "1234,56"
        
    Returns:
        Float con el valor convertido
    """
    if not value or value.strip() == '':
        return 0.0
    
    try:
        # Remover espacios
        value = str(value).strip()
        
        # Reemplazar coma por punto
        value = value.replace(',', '.')
        
        # Convertir a float
        return float(value)
    except Exception as e:
        print(f"Error convirtiendo '{value}' a decimal: {e}")
        return 0.0


def parse_date_string(date_str: str, format: str = None) -> Optional[datetime]:
    """
    Convertir string de fecha a datetime detectando automáticamente el formato
    
    Args:
        date_str: String con la fecha
        format: Formato de la fecha (opcional, se detecta automáticamente)
        
    Returns:
        datetime o None si no se puede parsear
    """
    try:
        if not date_str or date_str.strip() == '':
            return None
        
        date_str = date_str.strip()
        
        # Lista de formatos posibles
        formatos = [
            '%d/%m/%Y',      # 16/01/2025
            '%Y-%m-%d',      # 2025-01-16
            '%Y/%m/%d %H:%M:%S',  # 2025/01/16 10:30:00
            '%d-%m-%Y',      # 16-01-2025
        ]
        
        # Si se especificó un formato, usarlo primero
        if format:
            formatos.insert(0, format)
        
        # Intentar con cada formato
        for fmt in formatos:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Si ningún formato funcionó
        return None
        
    except Exception as e:
        print(f"Error parseando fecha '{date_str}': {e}")
        return None


def format_currency(value: float) -> str:
    """
    Formatear un valor numérico como moneda argentina
    
    Args:
        value: Valor numérico
        
    Returns:
        String formateado (ej: "1.234,56")
    """
    try:
        # Formatear con separador de miles y 2 decimales
        formatted = f"{value:,.2f}"
        
        # Reemplazar punto por coma y coma por punto (formato argentino)
        formatted = formatted.replace(',', 'X').replace('.', ',').replace('X', '.')
        
        return formatted
    except Exception as e:
        print(f"Error formateando moneda: {e}")
        return str(value)


def validar_estructura_csv(df) -> Tuple[bool, str]:
    """
    Validar que el CSV tenga la estructura esperada del libro diario
    
    Args:
        df: DataFrame de pandas con los datos
        
    Returns:
        Tupla (es_valido, mensaje_error)
    """
    columnas_requeridas = [
        'Fecasi', 'tipo_asiento', 'nro_asiento', 'Nro_renglon',
        'nro_cta', 'desc_pdc', 'debe', 'haber'
    ]
    
    # Verificar que existan las columnas requeridas
    columnas_faltantes = [col for col in columnas_requeridas if col not in df.columns]
    
    if columnas_faltantes:
        return False, f"Faltan columnas requeridas: {', '.join(columnas_faltantes)}"
    
    # Verificar que haya datos
    if len(df) == 0:
        return False, "El archivo está vacío"
    
    return True, "Estructura válida"


def get_nombre_mes(mes: int) -> str:
    """
    Obtener el nombre del mes en español
    
    Args:
        mes: Número del mes (1-12)
        
    Returns:
        Nombre del mes
    """
    meses = {
        1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
        5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
        9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
    }
    return meses.get(mes, 'Desconocido')


def calcular_semestre(mes: int) -> int:
    """
    Calcular el semestre basado en el mes
    
    Args:
        mes: Número del mes (1-12)
        
    Returns:
        1 o 2 (primer o segundo semestre)
    """
    return 1 if mes <= 6 else 2