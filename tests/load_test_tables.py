"""
load_test_tables.py
===================
Carga libro_mayor_test y saldo_apertura_test en Neon via COPY.

Estructura esperada:
    ReporteApp/
    ├── .env                      ← DATABASE_URL aquí
    └── tests/
        ├── load_test_tables.py   ← este script
        ├── libro_mayor_test.csv
        └── saldo_apertura_test.csv

Uso:
    cd ReporteApp
    python tests/load_test_tables.py
"""

import os
import time
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# .env en la raíz del proyecto (un nivel arriba de /tests)
ROOT_DIR  = Path(__file__).resolve().parent.parent
TESTS_DIR = Path(__file__).resolve().parent

load_dotenv(ROOT_DIR / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("No se encontró DATABASE_URL en el .env")

COLS = (
    "empresa_id,periodo_anio,periodo_mes,nivel,cuenta_codigo,"
    "tipo_subcuenta,nro_subcuenta,centro_costo,"
    "total_debe,total_haber,saldo_periodo,saldo_acumulado,saldo_anterior,fecha_periodo"
)

COPY_SQL = """
    COPY {tabla} ({cols})
    FROM STDIN
    WITH (FORMAT CSV, HEADER TRUE, DELIMITER '|', NULL '\\N')
"""

tablas = [
    ("libro_mayor_test",    TESTS_DIR / "libro_mayor_test.csv"),
    ("saldo_apertura_test", TESTS_DIR / "saldo_apertura_test.csv"),
]

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = False

try:
    with conn.cursor() as cur:
        for tabla, archivo in tablas:
            print(f"Cargando {archivo.name} → {tabla}...")
            t0 = time.time()
            with open(archivo, "r", encoding="utf-8") as f:
                cur.copy_expert(COPY_SQL.format(tabla=tabla, cols=COLS), f)
            elapsed = time.time() - t0
            cur.execute(f"SELECT COUNT(*) FROM {tabla}")
            n = cur.fetchone()[0]
            print(f"  ✅ {n:,} filas en {elapsed:.1f}s")

    conn.commit()
    print("\nCarga completa.")

except Exception as e:
    conn.rollback()
    print(f"❌ Error: {e}")
    raise
finally:
    conn.close()