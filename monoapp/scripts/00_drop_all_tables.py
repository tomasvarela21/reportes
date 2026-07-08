"""
00_drop_all_tables.py
=====================
Elimina TODAS las tablas del schema public en Neon.
Ejecutar SOLO para comenzar desde cero.

Uso:
    python 00_drop_all_tables.py
"""

import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ No se encontró DATABASE_URL en el archivo .env")


def drop_all_tables():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # Obtener todas las tablas del schema public EXCEPTO dim_cuenta
        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
              AND tablename != 'dim_cuenta'
            ORDER BY tablename;
        """)
        tablas = [row[0] for row in cur.fetchall()]

        if not tablas:
            print("✅ No hay tablas en el schema public. Ya está limpio.")
            return

        print(f"⚠️  Se encontraron {len(tablas)} tabla(s) para eliminar:")
        for t in tablas:
            print(f"   - {t}")

        confirmacion = input("\n¿Confirmas que querés eliminar TODAS estas tablas? (escribe 'CONFIRMAR'): ")
        if confirmacion.strip() != "CONFIRMAR":
            print("❌ Operación cancelada.")
            return

        # DROP CASCADE para evitar problemas de dependencias
        cur.execute(
            "DROP TABLE IF EXISTS {} CASCADE;".format(
                ", ".join(f'"{t}"' for t in tablas)
            )
        )
        conn.commit()

        print(f"\n✅ {len(tablas)} tabla(s) eliminadas correctamente.")

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    drop_all_tables()