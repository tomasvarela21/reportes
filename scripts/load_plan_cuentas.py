"""
Script para cargar el plan de cuentas desde Excel a dim_cuenta.

Regla: solo se cargan las filas que tienen valor en la columna 'Imput'.
El campo 'Imput' (entero) se usa como codigo principal, que coincide
con nro_cta en los archivos de libro diario.
"""
import sys
import os

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
    df.columns = df.columns.str.strip()

    print(f"✅ Leídas {len(df)} filas totales\n")

    # Solo filas con código de imputación asignado
    df_imput = df[df['Imput'].notna()].copy()
    print(f"🔢 Cuentas con código de imputación: {len(df_imput)}")
    print(f"⏭️  Cuentas resumen (sin imput, ignoradas): {len(df) - len(df_imput)}\n")

    cuentas = []
    for _, row in df_imput.iterrows():
        cuenta = {
            'codigo': int(row['Imput']),
            'nombre': str(row['Nombre de cuenta']).strip(),
            'codigo_jerarquico': str(row['Codigo']).strip(),
            'es_resultado': row['Resultado?'] == 'S' if pd.notna(row['Resultado?']) else False,
            'tipo_subcta': int(row['Tipo Sub Cta']) if pd.notna(row['Tipo Sub Cta']) else None,
            'moneda': int(row['Moneda']) if pd.notna(row['Moneda']) else None,
            'nivel': int(row['Nivel']),
            'activa': True
        }
        cuentas.append(cuenta)

    print(f"✅ {len(cuentas)} cuentas preparadas\n")
    print("💾 Insertando en DB...")

    insertadas = 0
    actualizadas = 0
    errores = 0

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
                            SET nombre = :nombre,
                                codigo_jerarquico = :codigo_jerarquico,
                                es_resultado = :es_resultado,
                                tipo_subcta = :tipo_subcta,
                                moneda = :moneda,
                                nivel = :nivel
                            WHERE codigo = :codigo
                        """),
                        cuenta
                    )
                    actualizadas += 1
                else:
                    conn.execute(
                        text("""
                            INSERT INTO dim_cuenta
                            (codigo, nombre, codigo_jerarquico, es_resultado,
                             tipo_subcta, moneda, nivel, activa)
                            VALUES
                            (:codigo, :nombre, :codigo_jerarquico, :es_resultado,
                             :tipo_subcta, :moneda, :nivel, :activa)
                        """),
                        cuenta
                    )
                    insertadas += 1

                conn.commit()

            except Exception as e:
                print(f"❌ Error en codigo {cuenta['codigo']}: {e}")
                errores += 1
                continue

    print("\n" + "=" * 50)
    print(f"✅ Insertadas:  {insertadas}")
    print(f"🔄 Actualizadas: {actualizadas}")
    print(f"❌ Errores:     {errores}")
    print("=" * 50)

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

    print("=" * 50)
    print("CARGA DE PLAN DE CUENTAS")
    print("=" * 50)
    print(f"📁 Archivo: {excel_path}\n")

    cargar_plan_cuentas(excel_path)
    print("\n✅ Completado!")
