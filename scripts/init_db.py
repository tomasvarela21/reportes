"""
Script para inicializar la base de datos ejecutando el archivo schemas.sql
"""
import sys
import os

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()


def ejecutar_sql_file(filepath: str):
    """
    Ejecutar un archivo SQL en la base de datos
    """
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL no está configurado en .env")
    
    engine = create_engine(database_url)
    
    print(f"📖 Leyendo archivo SQL: {filepath}")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    print("🔄 Ejecutando SQL...")
    
    # Dividir en statements
    statements = []
    current_statement = []
    
    for line in sql_content.split('\n'):
        line = line.strip()
        if not line or line.startswith('--'):
            continue
        
        current_statement.append(line)
        
        if line.endswith(';'):
            statement = ' '.join(current_statement)
            if statement.strip():
                statements.append(statement)
            current_statement = []
    
    print(f"📝 Total de statements: {len(statements)}")
    
    ejecutados = 0
    errores = 0
    
    with engine.connect() as conn:
        for idx, statement in enumerate(statements, 1):
            try:
                if idx % 10 == 0:
                    print(f"   Ejecutando {idx}/{len(statements)}...")
                
                conn.execute(text(statement))
                conn.commit()
                ejecutados += 1
                
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"⚠️  Statement {idx}: Ya existe")
                else:
                    print(f"❌ Error en {idx}: {e}")
                errores += 1
                continue
    
    print("\n" + "="*50)
    print(f"✅ Ejecutados: {ejecutados}")
    print(f"❌ Errores: {errores}")
    print("="*50)


def verificar_tablas():
    """Verificar tablas creadas"""
    database_url = os.getenv('DATABASE_URL')
    engine = create_engine(database_url)
    
    print("\n🔍 Verificando tablas...")
    
    tablas_esperadas = [
        'dim_empresa', 'dim_cuenta',
        'libro_diario_abierto', 'libro_diario_historico',
        'libro_mayor_abierto', 'libro_mayor_historico',
        'control_periodos', 'log_cargas'
    ]
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """))
        
        tablas_existentes = [row[0] for row in result]
    
    print(f"\n📋 Tablas encontradas: {len(tablas_existentes)}\n")
    
    for tabla in tablas_esperadas:
        if tabla in tablas_existentes:
            print(f"   ✅ {tabla}")
        else:
            print(f"   ❌ {tabla}")


if __name__ == "__main__":
    print("="*50)
    print("INICIALIZACIÓN DE BASE DE DATOS")
    print("="*50 + "\n")
    
    sql_file = 'models/schemas.sql'
    if not os.path.exists(sql_file):
        print(f"❌ Error: No se encuentra {sql_file}")
        exit(1)
    
    respuesta = input("¿Continuar? (s/n): ")
    
    if respuesta.lower() != 's':
        print("\n❌ Cancelado")
        exit(0)
    
    print()
    
    try:
        ejecutar_sql_file(sql_file)
        verificar_tablas()
        print("\n✅ Inicialización completada!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        exit(1)