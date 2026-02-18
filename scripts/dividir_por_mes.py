"""
Script para dividir el archivo anual del libro diario en archivos mensuales
"""
import pandas as pd
import os
from datetime import datetime


def dividir_por_mes(archivo_anual: str, codigo_empresa: str = 'EMPRESA_A'):
    """
    Dividir archivo anual en archivos mensuales
    
    Args:
        archivo_anual: Ruta al archivo CSV anual
        codigo_empresa: Código de la empresa para nombrar los archivos
    """
    
    print("📖 Leyendo archivo anual...")
    
    # Leer el CSV
    df = pd.read_csv(archivo_anual, sep=';', encoding='latin1')
    
    print(f"✅ Leídos {len(df)} registros\n")
    
    # Convertir la columna de fecha
    df['Fecasi'] = pd.to_datetime(df['Fecasi'], format='%d/%m/%Y')
    
    # Extraer mes y año
    df['mes'] = df['Fecasi'].dt.month
    df['anio'] = df['Fecasi'].dt.year
    
    # Obtener el año (debería ser 2025 según tu archivo)
    anio = df['anio'].iloc[0]
    
    # Crear carpeta para los archivos
    carpeta_salida = 'archivos_mensuales'
    os.makedirs(carpeta_salida, exist_ok=True)
    
    print(f"📊 Dividiendo por mes...\n")
    
    # Dividir por mes
    for mes in range(1, 13):
        df_mes = df[df['mes'] == mes].copy()
        
        if len(df_mes) == 0:
            print(f"   ⚠️  Mes {mes:02d}: Sin datos")
            continue
        
        # Eliminar las columnas auxiliares
        df_mes = df_mes.drop(['mes', 'anio'], axis=1)
        
        # Nombre del archivo de salida
        nombre_archivo = f"diario_{codigo_empresa}_{mes:02d}-{anio}.csv"
        ruta_salida = os.path.join(carpeta_salida, nombre_archivo)
        
        # Guardar CSV
        df_mes.to_csv(ruta_salida, sep=';', encoding='latin1', index=False)
        
        print(f"   ✅ Mes {mes:02d}/{anio}: {len(df_mes):,} registros → {nombre_archivo}")
    
    print(f"\n{'='*60}")
    print(f"✅ COMPLETADO")
    print(f"{'='*60}")
    print(f"📁 Archivos guardados en: {os.path.abspath(carpeta_salida)}")
    print(f"📊 Total de archivos generados: {len([f for f in os.listdir(carpeta_salida) if f.endswith('.csv')])}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Uso: python dividir_por_mes.py <archivo_anual.csv> [CODIGO_EMPRESA]")
        print("\nEjemplo:")
        print("  python scripts/dividir_por_mes.py reporte_diario_operativo__1_.CSV EMPRESA_A")
        print("\nSi no especificas CODIGO_EMPRESA, se usará 'EMPRESA_A' por defecto")
        sys.exit(1)
    
    archivo = sys.argv[1]
    empresa = sys.argv[2] if len(sys.argv) > 2 else 'EMPRESA_A'
    
    if not os.path.exists(archivo):
        print(f"❌ Error: El archivo {archivo} no existe")
        sys.exit(1)
    
    print("="*60)
    print("DIVISIÓN DE ARCHIVO ANUAL POR MESES")
    print("="*60)
    print(f"📁 Archivo: {archivo}")
    print(f"🏢 Empresa: {empresa}")
    print("="*60 + "\n")
    
    dividir_por_mes(archivo, empresa)