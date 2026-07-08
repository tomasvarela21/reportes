"""
Script para insertar las empresas del grupo
"""
import sys
import os

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ============================================
# CONFIGURAR TUS EMPRESAS AQUÍ
# ============================================

EMPRESAS = [
    {
        'codigo': 'BATIA',
        'nombre': 'BATIA S.A.',
        'cuit': '30-12345678-9'
    },
    {
        'codigo': 'GUARE',
        'nombre': 'GUARE S.A.',
        'cuit': '30-87654321-0'
    },
    {
        'codigo': 'NORFORK',
        'nombre': 'NORFORK S.A.',
        'cuit': '30-11223344-5'
    },
]

# ============================================


def insertar_empresas():
    """Insertar empresas en dim_empresa"""
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL no está configurado")
    
    engine = create_engine(database_url)
    
    print("🚀 Iniciando inserción de empresas...")
    print(f"📊 Total: {len(EMPRESAS)}\n")
    
    insertadas = 0
    actualizadas = 0
    
    with engine.connect() as conn:
        for empresa in EMPRESAS:
            try:
                result = conn.execute(
                    text("SELECT id FROM dim_empresa WHERE codigo = :codigo"),
                    {'codigo': empresa['codigo']}
                ).fetchone()
                
                if result:
                    conn.execute(
                        text("""
                            UPDATE dim_empresa 
                            SET nombre = :nombre, cuit = :cuit, activa = TRUE
                            WHERE codigo = :codigo
                        """),
                        empresa
                    )
                    print(f"🔄 Actualizada: {empresa['codigo']}")
                    actualizadas += 1
                else:
                    conn.execute(
                        text("""
                            INSERT INTO dim_empresa (codigo, nombre, cuit, activa)
                            VALUES (:codigo, :nombre, :cuit, TRUE)
                        """),
                        empresa
                    )
                    print(f"✅ Insertada: {empresa['codigo']}")
                    insertadas += 1
                
                conn.commit()
                
            except Exception as e:
                print(f"❌ Error con {empresa['codigo']}: {e}")
                continue
    
    print("\n" + "="*50)
    print(f"✅ Insertadas: {insertadas}")
    print(f"🔄 Actualizadas: {actualizadas}")
    print("="*50)


if __name__ == "__main__":
    print("="*50)
    print("INSERCIÓN DE EMPRESAS")
    print("="*50 + "\n")
    ##print("⚠️  Edita este archivo primero con tus empresas reales\n")
    
    respuesta = input("¿Continuar? (s/n): ")
    
    if respuesta.lower() != 's':
        print("\n❌ Cancelado")
        exit(0)
    
    print()
    insertar_empresas()
    print("\n✅ Completado!")