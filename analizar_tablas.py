"""
Analiza la estructura completa de las tablas 'proyectos' y 'proy_presupuestos'
en la DB externa (EXTERNAL_DB_URL) para poder replicarlas en ReporteApp.

Uso:
    pip install psycopg2-binary
    python analizar_tablas.py
"""
import os
import sys

import psycopg2
from dotenv import load_dotenv

load_dotenv()

CONN_TEST = os.environ.get("EXTERNAL_DB_URL")
if not CONN_TEST:
    print("EXTERNAL_DB_URL no está configurada. Agregala al archivo .env antes de ejecutar este script.")
    sys.exit(1)

TABLAS = ["proyectos", "proy_presupuestos"]


def analizar_columnas(cur, tabla):
    print(f"\n{'─'*70}")
    print(f"  COLUMNAS — {tabla}")
    print(f"{'─'*70}")
    cur.execute("""
        SELECT column_name, data_type, character_maximum_length,
               numeric_precision, numeric_scale,
               is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position;
    """, (tabla,))
    rows = cur.fetchall()
    for col, dtype, maxlen, prec, scale, nullable, default in rows:
        tipo = dtype
        if maxlen:
            tipo += f"({maxlen})"
        elif prec and scale is not None:
            tipo += f"({prec},{scale})"
        print(f"  {col:<30} {tipo:<25} NULL={nullable:<5} DEFAULT={default}")
    return rows


def analizar_pk_fk(cur, tabla):
    print(f"\n  CLAVES — {tabla}")
    cur.execute("""
        SELECT tc.constraint_type, kcu.column_name,
               ccu.table_name AS foreign_table, ccu.column_name AS foreign_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        LEFT JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name AND tc.constraint_type = 'FOREIGN KEY'
        WHERE tc.table_schema = 'public' AND tc.table_name = %s
        ORDER BY tc.constraint_type;
    """, (tabla,))
    for tipo, col, ftable, fcol in cur.fetchall():
        if tipo == "FOREIGN KEY":
            print(f"    {tipo}: {col} -> {ftable}.{fcol}")
        else:
            print(f"    {tipo}: {col}")


def analizar_indices(cur, tabla):
    print(f"\n  ÍNDICES — {tabla}")
    cur.execute("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = %s;
    """, (tabla,))
    for name, definicion in cur.fetchall():
        print(f"    {name}: {definicion}")


def muestra_datos(cur, tabla, n=5):
    print(f"\n  MUESTRA DE DATOS — {tabla} (primeras {n} filas)")
    cur.execute(f"SELECT * FROM {tabla} LIMIT {n};")
    cols = [desc[0] for desc in cur.description]
    print(f"    Columnas: {cols}")
    for row in cur.fetchall():
        print(f"    {row}")

    cur.execute(f"SELECT COUNT(*) FROM {tabla};")
    total = cur.fetchone()[0]
    print(f"\n  TOTAL DE FILAS en {tabla}: {total}")


def ddl_create_table(cur, tabla):
    """Genera un CREATE TABLE aproximado basado en information_schema."""
    print(f"\n  DDL SUGERIDO — {tabla}")
    cur.execute("""
        SELECT column_name, data_type, character_maximum_length,
               numeric_precision, numeric_scale, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position;
    """, (tabla,))
    cols_def = []
    for col, dtype, maxlen, prec, scale, nullable, default in cur.fetchall():
        tipo_sql = dtype.upper()
        if dtype == "character varying":
            tipo_sql = f"VARCHAR({maxlen})" if maxlen else "VARCHAR"
        elif dtype == "numeric" and prec:
            tipo_sql = f"NUMERIC({prec},{scale or 0})"
        elif dtype == "timestamp without time zone":
            tipo_sql = "TIMESTAMP"
        elif dtype == "timestamp with time zone":
            tipo_sql = "TIMESTAMPTZ"
        elif dtype == "integer":
            tipo_sql = "INTEGER"
        elif dtype == "bigint":
            tipo_sql = "BIGINT"
        elif dtype == "boolean":
            tipo_sql = "BOOLEAN"
        elif dtype == "text":
            tipo_sql = "TEXT"
        elif dtype == "date":
            tipo_sql = "DATE"

        linea = f"    {col} {tipo_sql}"
        if nullable == "NO":
            linea += " NOT NULL"
        if default:
            linea += f" DEFAULT {default}"
        cols_def.append(linea)

    print(f"CREATE TABLE {tabla} (")
    print(",\n".join(cols_def))
    print(");")


if __name__ == "__main__":
    conn = psycopg2.connect(CONN_TEST)
    cur = conn.cursor()

    for tabla in TABLAS:
        print(f"\n{'='*70}")
        print(f"  TABLA: {tabla}")
        print(f"{'='*70}")
        analizar_columnas(cur, tabla)
        analizar_pk_fk(cur, tabla)
        analizar_indices(cur, tabla)
        muestra_datos(cur, tabla)
        ddl_create_table(cur, tabla)

    cur.close()
    conn.close()