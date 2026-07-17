"""
ultimos_cambios.py
===================
Muestra los últimos registros modificados en la base de datos EXTERNA
(origen) para 'proyectos' y 'proy_presupuestos', ordenados por updated_at
descendente, junto con sus valores actuales. Es de SOLO LECTURA.

Útil para responder: "¿qué datos cambiaron últimamente en el origen y
cuándo?" — por ejemplo para cruzar con números raros detectados en PowerBI.

Uso:
    python ultimos_cambios.py               # últimos 15 de cada tabla
    python ultimos_cambios.py --limit 30
    python ultimos_cambios.py --desde 2026-07-10   # solo cambios desde esa fecha
"""
import argparse
import os
import sys

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

CONN_EXTERNA = os.environ.get("EXTERNAL_DB_URL")
if not CONN_EXTERNA:
    print("EXTERNAL_DB_URL no está configurada en .env")
    sys.exit(1)


def mostrar_proyectos(conn, limit, desde):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    where = "WHERE updated_at >= %s" if desde else ""
    params = (desde,) if desde else ()
    cur.execute(f"""
        SELECT id, nombre, centro_costo, estado, importe_mat, importe_mo,
               terceros, herramientas, superficie, ingresos, updated_at
        FROM proyectos
        {where}
        ORDER BY updated_at DESC NULLS LAST
        LIMIT %s
    """, params + (limit,))
    rows = cur.fetchall()
    cur.close()

    print(f"\n--- Últimos {len(rows)} cambios en PROYECTOS (origen) ---")
    for r in rows:
        print(
            f"[{r['updated_at']}] id={r['id']} ccosto={r['centro_costo']} "
            f"nombre={r['nombre']!r} estado={r['estado']} "
            f"mat={r['importe_mat']} mo={r['importe_mo']} terceros={r['terceros']} "
            f"herr={r['herramientas']} superficie={r['superficie']} ingresos={r['ingresos']}"
        )


def mostrar_presupuestos(conn, limit, desde):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    where = "WHERE updated_at >= %s" if desde else ""
    params = (desde,) if desde else ()
    cur.execute(f"""
        SELECT id, proyecto_id, fecha, mo_propia, mo_terceros, materiales,
               herramientas, horas, metros, importe, descripcion, updated_at
        FROM proy_presupuestos
        {where}
        ORDER BY updated_at DESC NULLS LAST
        LIMIT %s
    """, params + (limit,))
    rows = cur.fetchall()
    cur.close()

    print(f"\n--- Últimos {len(rows)} cambios en PROY_PRESUPUESTOS (origen) ---")
    for r in rows:
        print(
            f"[{r['updated_at']}] id={r['id']} proyecto_id={r['proyecto_id']} "
            f"fecha={r['fecha']} mo_propia={r['mo_propia']} mo_terceros={r['mo_terceros']} "
            f"materiales={r['materiales']} herr={r['herramientas']} horas={r['horas']} "
            f"metros={r['metros']} importe={r['importe']} desc={r['descripcion']!r}"
        )


def main():
    parser = argparse.ArgumentParser(description="Lista los últimos cambios en la DB externa.")
    parser.add_argument("--limit", type=int, default=15, help="Cantidad de filas a mostrar por tabla.")
    parser.add_argument("--desde", type=str, default=None, help="Fecha (YYYY-MM-DD) desde la cual filtrar cambios.")
    args = parser.parse_args()

    conn = psycopg2.connect(CONN_EXTERNA)
    try:
        mostrar_proyectos(conn, args.limit, args.desde)
        mostrar_presupuestos(conn, args.limit, args.desde)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
