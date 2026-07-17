"""
verificar_sync.py
==================
Verifica que la sincronización de 'proyectos' y 'proy_presupuestos' entre la
base de datos externa (origen) y la base de datos de ReporteApp (Neon) esté
al día y sea consistente. Es de SOLO LECTURA: no modifica ninguna de las dos bases.

Chequea:
    1. Conteo de filas en origen vs destino.
    2. Última modificación real en el origen (MAX(updated_at)) vs lo que
       quedó registrado en Neon (actualizado_en / última corrida del sync).
    3. Registros huérfanos: existen en un lado y no en el otro.
    4. Para proyectos: filas donde el origen tiene updated_at más nuevo que
       lo que quedó en Neon (indica que faltó correr el sync).
    5. Para presupuestos: diferencias de valores entre filas con el mismo id.

Uso:
    python verificar_sync.py
"""
import os
import sys

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

CONN_REPORTEAPP = os.environ["DATABASE_URL"]
CONN_EXTERNA = os.environ.get("EXTERNAL_DB_URL")
if not CONN_EXTERNA:
    print("EXTERNAL_DB_URL no está configurada en .env")
    sys.exit(1)

PRESUPUESTO_CAMPOS_COMPARAR = [
    ("mo_propia", "mo_propia"),
    ("mo_terceros", "mo_terceros"),
    ("materiales", "materiales"),
    ("herramientas", "herramientas"),
    ("horas", "horas"),
    ("metros", "metros"),
    ("importe", "importe"),
    ("fecha", "fecha"),
]


def linea(txt=""):
    print(txt)


def encontrar_columna_timestamp(conn, tabla, candidatos):
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = %s AND data_type LIKE 'timestamp%%'
    """, (tabla,))
    cols = {r[0] for r in cur.fetchall()}
    cur.close()
    for c in candidatos:
        if c in cols:
            return c
    return next(iter(cols), None)


def main():
    conn_ext = psycopg2.connect(CONN_EXTERNA)
    conn_app = psycopg2.connect(CONN_REPORTEAPP)
    cur_ext = conn_ext.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur_app = conn_app.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    linea("=" * 70)
    linea("VERIFICACIÓN DE SINCRONIZACIÓN: DB externa -> Neon (ReporteApp)")
    linea("=" * 70)

    # ── 1. PROYECTOS ─────────────────────────────────────────────────────
    linea("\n--- PROYECTOS ---")

    cur_ext.execute("SELECT id, updated_at, deleted_at FROM proyectos")
    ext_proyectos = {r["id"]: r for r in cur_ext.fetchall()}

    cur_app.execute("""
        SELECT id_origen, actualizado_en, activo
        FROM proyectos WHERE id_origen IS NOT NULL
    """)
    app_proyectos = {r["id_origen"]: r for r in cur_app.fetchall()}

    linea(f"Filas en origen: {len(ext_proyectos)}")
    linea(f"Filas en Neon (con id_origen): {len(app_proyectos)}")

    max_ext = max((r["updated_at"] for r in ext_proyectos.values() if r["updated_at"]), default=None)
    max_app = max((r["actualizado_en"] for r in app_proyectos.values() if r["actualizado_en"]), default=None)
    linea(f"Última modificación en origen (MAX updated_at):  {max_ext}")
    linea(f"Última actualización reflejada en Neon:          {max_app}")
    if max_ext and max_app and max_ext > max_app:
        linea("⚠️  El origen tiene cambios más nuevos que los reflejados en Neon. Falta correr el sync.")
    elif max_ext and max_app:
        linea("✅ Neon está al día respecto al origen (por fecha).")

    faltan_en_neon = set(ext_proyectos) - set(app_proyectos)
    faltan_en_origen = set(app_proyectos) - set(ext_proyectos)
    if faltan_en_neon:
        linea(f"⚠️  {len(faltan_en_neon)} proyectos existen en el origen pero no en Neon: {sorted(faltan_en_neon)[:20]}")
    else:
        linea("✅ No hay proyectos del origen ausentes en Neon.")
    if faltan_en_origen:
        linea(f"⚠️  {len(faltan_en_origen)} proyectos existen en Neon (id_origen) pero ya no están en el origen: {sorted(faltan_en_origen)[:20]}")
    else:
        linea("✅ No hay proyectos huérfanos en Neon.")

    desfasados = []
    for pid, ext_row in ext_proyectos.items():
        app_row = app_proyectos.get(pid)
        if app_row and ext_row["updated_at"] and app_row["actualizado_en"]:
            if ext_row["updated_at"] > app_row["actualizado_en"]:
                desfasados.append(pid)
    if desfasados:
        linea(f"⚠️  {len(desfasados)} proyectos con cambios en origen no reflejados aún en Neon: {sorted(desfasados)[:20]}")
    else:
        linea("✅ Ningún proyecto individual está desfasado.")

    # ── 2. PROY_PRESUPUESTOS ────────────────────────────────────────────
    linea("\n--- PROY_PRESUPUESTOS ---")

    cur_ext.execute(f"""
        SELECT id, proyecto_id, updated_at, {', '.join(c for c, _ in PRESUPUESTO_CAMPOS_COMPARAR)}
        FROM proy_presupuestos
    """)
    ext_presu = {r["id"]: r for r in cur_ext.fetchall()}

    cur_app.execute(f"""
        SELECT id, {', '.join(c for _, c in PRESUPUESTO_CAMPOS_COMPARAR)}
        FROM proy_presupuestos
    """)
    app_presu = {r["id"]: r for r in cur_app.fetchall()}

    linea(f"Filas en origen: {len(ext_presu)}")
    linea(f"Filas en Neon: {len(app_presu)}")

    faltan_en_neon_p = set(ext_presu) - set(app_presu)
    faltan_en_origen_p = set(app_presu) - set(ext_presu)
    if faltan_en_neon_p:
        linea(f"⚠️  {len(faltan_en_neon_p)} presupuestos existen en el origen pero no en Neon: {sorted(faltan_en_neon_p)[:20]}")
    else:
        linea("✅ No hay presupuestos del origen ausentes en Neon.")
    if faltan_en_origen_p:
        linea(f"⚠️  {len(faltan_en_origen_p)} presupuestos existen en Neon pero ya no están en el origen: {sorted(faltan_en_origen_p)[:20]}")
    else:
        linea("✅ No hay presupuestos huérfanos en Neon.")

    diferencias = []
    for pid in set(ext_presu) & set(app_presu):
        ext_row, app_row = ext_presu[pid], app_presu[pid]
        for col_ext, col_app in PRESUPUESTO_CAMPOS_COMPARAR:
            if ext_row.get(col_ext) != app_row.get(col_app):
                diferencias.append((pid, col_ext, ext_row.get(col_ext), app_row.get(col_app)))
    if diferencias:
        linea(f"⚠️  {len(diferencias)} diferencias de valores encontradas entre origen y Neon. Ejemplos:")
        for pid, col, v_ext, v_app in diferencias[:15]:
            linea(f"    id={pid} campo={col}: origen={v_ext!r} vs neon={v_app!r}")
    else:
        linea("✅ Los valores coinciden entre origen y Neon para las filas presentes en ambos.")

    max_ext_p = max((r["updated_at"] for r in ext_presu.values() if r["updated_at"]), default=None)
    linea(f"\nÚltima modificación en origen (proy_presupuestos, MAX updated_at): {max_ext_p}")

    # ── 3. ÚLTIMA CORRIDA DEL SYNC (sync_proyectos_log) ─────────────────
    linea("\n--- ÚLTIMA CORRIDA DEL SYNC ---")
    cur_app.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables WHERE table_name = 'sync_proyectos_log'
        )
    """)
    existe_log = cur_app.fetchone()["exists"]
    if not existe_log:
        linea("❌ La tabla sync_proyectos_log no existe en Neon. No se puede saber cuándo corrió el sync por última vez.")
    else:
        ts_col = encontrar_columna_timestamp(conn_app, "sync_proyectos_log", ["creado_en", "created_at", "fecha", "ejecutado_en", "timestamp"])
        if not ts_col:
            linea("⚠️  sync_proyectos_log existe pero no se encontró una columna de fecha/hora.")
        else:
            cur_app.execute(f"""
                SELECT origen, proyectos_ok, proyectos_error, presupuestos_ok, presupuestos_error, {ts_col}
                FROM sync_proyectos_log ORDER BY {ts_col} DESC LIMIT 1
            """)
            ultimo = cur_app.fetchone()
            if ultimo:
                linea(f"Última corrida: {ultimo[ts_col]} (origen={ultimo['origen']})")
                linea(f"  Proyectos:    {ultimo['proyectos_ok']} OK / {ultimo['proyectos_error']} error")
                linea(f"  Presupuestos: {ultimo['presupuestos_ok']} OK / {ultimo['presupuestos_error']} error")
                if max_ext_p and ultimo[ts_col] and max_ext_p > ultimo[ts_col]:
                    linea("  ⚠️  Hay cambios en el origen (proy_presupuestos) posteriores a la última corrida del sync.")
                if ultimo['proyectos_error'] or ultimo['presupuestos_error']:
                    linea("  ⚠️  La última corrida tuvo errores. Revisar detalle_errores en sync_proyectos_log.")
            else:
                linea("⚠️  sync_proyectos_log existe pero no tiene registros (el sync nunca corrió).")

    linea("\n" + "=" * 70)

    cur_ext.close()
    cur_app.close()
    conn_ext.close()
    conn_app.close()


if __name__ == "__main__":
    main()
