"""
Script para dividir el archivo anual del libro diario en archivos mensuales.
Maneja correctamente archivos que contienen datos de múltiples años (ej: Norfork).
"""
import pandas as pd
import os
import sys


def dividir_por_mes(archivo_anual: str, codigo_empresa: str = 'EMPRESA'):
    """
    Dividir un archivo CSV anual en archivos mensuales.

    El nombre de salida sigue el formato esperado por el sistema:
        diario_EMPRESA_MM-YYYY.csv

    Si el archivo contiene datos de múltiples años (ej: Norfork 2023+2024),
    genera archivos separados por cada combinación mes/año.
    """
    if not os.path.exists(archivo_anual):
        print(f"❌ Error: El archivo '{archivo_anual}' no existe")
        sys.exit(1)

    print(f"{'='*60}")
    print(f"DIVISIÓN DE ARCHIVO POR MES")
    print(f"{'='*60}")
    print(f"📁 Archivo : {archivo_anual}")
    print(f"🏢 Empresa : {codigo_empresa}")
    print(f"{'='*60}\n")

    print("📖 Leyendo archivo...")
    df = pd.read_csv(archivo_anual, sep=';', encoding='latin1')
    print(f"✅ {len(df):,} registros leídos\n")

    # Parsear fecha — formato D/M/YYYY
    df['_fecha'] = pd.to_datetime(df['Fecasi'], format='%d/%m/%Y', dayfirst=True, errors='coerce')

    nulas = df['_fecha'].isna().sum()
    if nulas > 0:
        print(f"⚠️  {nulas} registros con fecha inválida — se omitirán\n")

    df = df.dropna(subset=['_fecha'])
    df['_mes']  = df['_fecha'].dt.month
    df['_anio'] = df['_fecha'].dt.year

    # Resumen de distribución
    periodos = df.groupby(['_anio', '_mes']).size().reset_index(name='registros')
    print(f"📊 Distribución de períodos encontrados:")
    for _, row in periodos.iterrows():
        print(f"   {row['_mes']:02d}/{row['_anio']}: {row['registros']:,} registros")
    print()

    # Crear carpeta de salida
    carpeta_salida = 'archivos_mensuales'
    os.makedirs(carpeta_salida, exist_ok=True)

    archivos_generados = 0

    for _, periodo in periodos.iterrows():
        anio = int(periodo['_anio'])
        mes  = int(periodo['_mes'])

        df_mes = df[(df['_mes'] == mes) & (df['_anio'] == anio)].copy()
        df_mes = df_mes.drop(columns=['_fecha', '_mes', '_anio'])

        nombre_archivo = f"diario_{codigo_empresa}_{mes:02d}-{anio}.csv"
        ruta_salida    = os.path.join(carpeta_salida, nombre_archivo)

        df_mes.to_csv(ruta_salida, sep=';', encoding='latin1', index=False)

        print(f"✅ {nombre_archivo}: {len(df_mes):,} registros")
        archivos_generados += 1

    print(f"\n{'='*60}")
    print(f"✅ COMPLETADO — {archivos_generados} archivos generados")
    print(f"📁 Carpeta: {os.path.abspath(carpeta_salida)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python dividir_por_mes.py <archivo.csv> <CODIGO_EMPRESA>")
        print("\nEjemplo:")
        print("  python dividir_por_mes.py Norfork_2024.CSV NORFORK")
        print("  python dividir_por_mes.py Guare_2024.CSV GUARE")
        sys.exit(1)

    dividir_por_mes(sys.argv[1], sys.argv[2].upper())