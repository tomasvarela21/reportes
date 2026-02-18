"""
Script de verificación del sistema
"""
import sys
import os

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()


def verificar_env():
    """Verificar variables de entorno"""
    print("🔍 Verificando .env...")
    
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("   ❌ DATABASE_URL no configurado")
        return False
    
    if 'neon.tech' in database_url:
        print("   ✅ DATABASE_URL OK (Neon)")
    else:
        print("   ⚠️  DATABASE_URL configurado (no Neon)")
    
    return True


def verificar_conexion():
    """Verificar conexión"""
    print("\n🔍 Verificando conexión...")
    
    try:
        database_url = os.getenv('DATABASE_URL')
        engine = create_engine(database_url)
        
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        print("   ✅ Conexión exitosa")
        return True
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def verificar_tablas():
    """Verificar tablas"""
    print("\n🔍 Verificando tablas...")
    
    tablas_requeridas = [
        'dim_empresa', 'dim_cuenta',
        'libro_diario_abierto', 'libro_diario_historico',
        'libro_mayor_abierto', 'libro_mayor_historico',
        'control_periodos', 'log_cargas'
    ]
    
    try:
        database_url = os.getenv('DATABASE_URL')
        engine = create_engine(database_url)
        
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """))
            
            tablas_existentes = [row[0] for row in result]
        
        todas_ok = True
        for tabla in tablas_requeridas:
            if tabla in tablas_existentes:
                print(f"   ✅ {tabla}")
            else:
                print(f"   ❌ {tabla}")
                todas_ok = False
        
        return todas_ok
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def verificar_datos():
    """Verificar datos maestros"""
    print("\n🔍 Verificando datos...")
    
    try:
        database_url = os.getenv('DATABASE_URL')
        engine = create_engine(database_url)
        
        with engine.connect() as conn:
            # Empresas
            result = conn.execute(text("SELECT COUNT(*) FROM dim_empresa"))
            count_emp = result.fetchone()[0]
            
            if count_emp > 0:
                print(f"   ✅ dim_empresa: {count_emp} empresas")
            else:
                print(f"   ⚠️  dim_empresa: vacía")
            
            # Cuentas
            result = conn.execute(text("SELECT COUNT(*) FROM dim_cuenta"))
            count_cta = result.fetchone()[0]
            
            if count_cta > 0:
                print(f"   ✅ dim_cuenta: {count_cta} cuentas")
            else:
                print(f"   ⚠️  dim_cuenta: vacía")
            
            return count_emp > 0 and count_cta > 0
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


if __name__ == "__main__":
    print("="*50)
    print("VERIFICACIÓN DEL SISTEMA")
    print("="*50 + "\n")
    
    env_ok = verificar_env()
    
    if not env_ok:
        print("\n❌ Revisar .env")
        exit(1)
    
    conn_ok = verificar_conexion()
    
    if not conn_ok:
        print("\n❌ No hay conexión")
        exit(1)
    
    tablas_ok = verificar_tablas()
    datos_ok = verificar_datos()
    
    print("\n" + "="*50)
    if tablas_ok and datos_ok:
        print("✅ SISTEMA LISTO")
    elif tablas_ok:
        print("⚠️  FALTAN DATOS")
        print("\nEjecutar:")
        print("  python scripts/insert_empresas.py")
        print("  python scripts/load_plan_cuentas.py <excel>")
    else:
        print("❌ FALTAN TABLAS")
        print("\nEjecutar:")
        print("  python scripts/init_db.py")
    
    print("="*50 + "\n")