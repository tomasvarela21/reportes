"""
Script de verificación del sistema — nuevo schema
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

TABLAS_REQUERIDAS = [
    'dim_empresa', 'dim_rubro', 'dim_cuenta',
    'libro_diario', 'libro_mayor',
    'saldos_apertura', 'log_cargas'
]


def verificar_env():
    print("🔍 Verificando .env...")
    url = os.getenv('DATABASE_URL')
    if not url:
        print("   ❌ DATABASE_URL no configurado")
        return False
    print(f"   ✅ DATABASE_URL configurado ({'Neon' if 'neon.tech' in url else 'otro'})")
    return True


def verificar_conexion():
    print("\n🔍 Verificando conexión...")
    try:
        engine = create_engine(os.getenv('DATABASE_URL'))
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("   ✅ Conexión exitosa")
        return True
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def verificar_tablas():
    print("\n🔍 Verificando tablas...")
    try:
        engine = create_engine(os.getenv('DATABASE_URL'))
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
            """))
            existentes = {row[0] for row in result}

        todas_ok = True
        for tabla in TABLAS_REQUERIDAS:
            if tabla in existentes:
                print(f"   ✅ {tabla}")
            else:
                print(f"   ❌ {tabla} — FALTA")
                todas_ok = False
        return todas_ok
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def verificar_datos():
    print("\n🔍 Verificando datos maestros...")
    try:
        engine = create_engine(os.getenv('DATABASE_URL'))
        with engine.connect() as conn:

            emp = conn.execute(text("SELECT COUNT(*) FROM dim_empresa")).fetchone()[0]
            cta = conn.execute(text("SELECT COUNT(*) FROM dim_cuenta")).fetchone()[0]
            rub = conn.execute(text("SELECT COUNT(*) FROM dim_rubro")).fetchone()[0]
            dia = conn.execute(text("SELECT COUNT(*) FROM libro_diario")).fetchone()[0]
            may = conn.execute(text("SELECT COUNT(*) FROM libro_mayor")).fetchone()[0]

            print(f"   {'✅' if emp > 0 else '⚠️ '} dim_empresa:   {emp} registros")
            print(f"   {'✅' if rub > 0 else '⚠️ '} dim_rubro:     {rub} registros")
            print(f"   {'✅' if cta > 0 else '⚠️ '} dim_cuenta:    {cta} registros")
            print(f"   {'✅' if dia > 0 else '⚠️ '} libro_diario:  {dia:,} registros")
            print(f"   {'✅' if may > 0 else '⚠️ '} libro_mayor:   {may:,} registros")

            # Empresas con datos
            if dia > 0:
                print("\n   📊 Datos por empresa:")
                rows = conn.execute(text("""
                    SELECT e.codigo, e.nombre,
                           COUNT(*)            AS movimientos,
                           MIN(ld.periodo_anio || '-' || LPAD(ld.periodo_mes::text,2,'0')) AS desde,
                           MAX(ld.periodo_anio || '-' || LPAD(ld.periodo_mes::text,2,'0')) AS hasta
                    FROM libro_diario ld
                    JOIN dim_empresa e ON e.id = ld.id_empresa
                    GROUP BY e.id, e.codigo, e.nombre
                    ORDER BY e.codigo
                """)).fetchall()
                for r in rows:
                    print(f"      {r[0]}: {r[2]:,} movimientos ({r[3]} → {r[4]})")

            return emp > 0 and cta > 0

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


if __name__ == "__main__":
    print("=" * 55)
    print("VERIFICACIÓN DEL SISTEMA CONTABLE")
    print("=" * 55)

    if not verificar_env():
        sys.exit(1)

    if not verificar_conexion():
        sys.exit(1)

    tablas_ok = verificar_tablas()
    datos_ok  = verificar_datos()

    print("\n" + "=" * 55)
    if tablas_ok and datos_ok:
        print("✅ SISTEMA LISTO")
    elif tablas_ok:
        print("⚠️  FALTAN DATOS MAESTROS")
        print("\nEjecutar el schema SQL en Neon y luego:")
        print("  python scripts/load_plan_cuentas.py <excel>")
    else:
        print("❌ FALTAN TABLAS — ejecutar schema_contable.sql en Neon")
    print("=" * 55)