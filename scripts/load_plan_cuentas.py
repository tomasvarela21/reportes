"""
Script para cargar el plan de cuentas desde Excel
"""
import sys
import os

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()


def cargar_plan_cuentas(excel_path: str):
    """Cargar plan de cuentas desde Excel a dim_cuenta"""
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL no está configurado")
    
    engine = create_engine(database_url)
    
    print("📖 Leyendo Excel...")
    df = pd.read_excel(excel_path)
    
    print(f"✅ Leídas {len(df)} cuentas\n")
    
    # Limpiar columnas
    df.columns = df.columns.str.strip()
    
    print("🔄 Procesando datos...")
    
    cuentas = []
    
    for idx, row in df.iterrows():
        codigo = str(row['Codigo']).strip()
        es_resultado = row['Resultado?'] == 'S' if pd.notna(row['Resultado?']) else False
        
        cuenta = {
            'codigo': codigo,
            'nombre': row['Nombre de cuenta'],
            'imput': int(row['Imput']) if pd.notna(row['Imput']) else None,
            'es_resultado': es_resultado,
            'tipo_subcta': int(row['Tipo Sub Cta']) if pd.notna(row['Tipo Sub Cta']) else None,
            'moneda': int(row['Moneda']) if pd.notna(row['Moneda']) else None,
            'nivel': int(row['Nivel']),
            'activa': True
        }
        
        cuentas.append(cuenta)
    
    print(f"✅ {len(cuentas)} cuentas procesadas\n")
    print("💾 Insertando en DB...")
    
    insertadas = 0
    actualizadas = 0
    
    with engine.connect() as conn:
        for cuenta in cuentas:
            try:
                result = conn.execute(
                    text("SELECT id FROM dim_cuenta WHERE codigo = :codigo"),
                    {'codigo': cuenta['codigo']}
                ).fetchone()
                
                if result:
                    conn.execute(
                        text("""
                            UPDATE dim_cuenta 
                            SET nombre = :nombre, imput = :imput,
                                es_resultado = :es_resultado,
                                tipo_subcta = :tipo_subcta,
                                moneda = :moneda, nivel = :nivel
                            WHERE codigo = :codigo
                        """),
                        cuenta
                    )
                    actualizadas += 1
                else:
                    conn.execute(
                        text("""
                            INSERT INTO dim_cuenta 
                            (codigo, nombre, imput, es_resultado, 
                             tipo_subcta, moneda, nivel, activa)
                            VALUES 
                            (:codigo, :nombre, :imput, :es_resultado,
                             :tipo_subcta, :moneda, :nivel, :activa)
                        """),
                        cuenta
                    )
                    insertadas += 1
                
                conn.commit()
                
            except Exception as e:
                print(f"❌ Error en {cuenta['codigo']}: {e}")
                continue
    
    print("\n" + "="*50)
    print(f"✅ Insertadas: {insertadas}")
    print(f"🔄 Actualizadas: {actualizadas}")
    print("="*50)
    
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM dim_cuenta")).fetchone()[0]
        print(f"\n🎯 Total en DB: {total}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python load_plan_cuentas.py <ruta_excel>")
        print("Ejemplo: python scripts/load_plan_cuentas.py PLAN_DE_CUENTAS_NUEVOOO.xlsx")
        exit(1)
    
    excel_path = sys.argv[1]
    
    if not os.path.exists(excel_path):
        print(f"❌ No existe: {excel_path}")
        exit(1)
    
    print("🚀 Cargando plan de cuentas...")
    print(f"📁 Archivo: {excel_path}\n")
    
    cargar_plan_cuentas(excel_path)
    print("\n✅ Completado!")