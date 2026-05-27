"""
Acceso a datos para stg_mayor_csv_cuenta (chequeo de consistencia).
Solo contiene SQL — sin lógica de negocio ni excepciones HTTP.
"""
import logging

from psycopg2.extras import RealDictCursor

from core.db_utils import execute_batch

log = logging.getLogger(__name__)

_TOL = 1.0   # tolerancia en pesos para considerar una diferencia


def get_estado(conn) -> list[dict]:
    """
    Resumen de empresas/períodos cargados en stg_mayor_csv_cuenta.
    Una fila por (empresa_id, periodo_anio, periodo_mes).
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                empresa_id,
                periodo_anio,
                periodo_mes,
                COUNT(*)            AS total_cuentas,
                archivo_origen      AS archivo,
                MAX(cargado_en)     AS ultima_carga
            FROM stg_mayor_csv_cuenta
            GROUP BY empresa_id, periodo_anio, periodo_mes, archivo_origen
            ORDER BY empresa_id
            """
        )
        return cur.fetchall()


def truncate_staging(conn) -> None:
    """TRUNCATE TABLE stg_mayor_csv_cuenta. No hace commit."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE stg_mayor_csv_cuenta")


def insert_batch(conn, rows: list[tuple]) -> int:
    """
    INSERT batch en stg_mayor_csv_cuenta.
    Cada tupla: (archivo_origen, empresa_id, periodo_anio, periodo_mes,
                 cuenta_codigo, descripcion, saldo_no_ajustado)
    No hace commit — el service es responsable.
    """
    sql = """
        INSERT INTO stg_mayor_csv_cuenta
            (archivo_origen, empresa_id, periodo_anio, periodo_mes,
             cuenta_codigo, descripcion, saldo_no_ajustado)
        VALUES %s
    """
    return execute_batch(conn, sql, rows, page_size=500)


def comparar(
    conn,
    empresa_ids: list[int],
    anio: int,
    mes: int,
) -> list[dict]:
    """
    FULL OUTER JOIN stg_mayor_csv_cuenta vs libro_mayor para el período y empresas dados.
    Clasifica cada fila en "Diferencia", "Solo en CSV" o "Solo en DB".
    Solo devuelve filas donde |diferencia| > TOL.
    Copiado de pages/6_Chequeo_Consistencia.py → run_comparacion().
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                COALESCE(csv.empresa_id, lm.empresa_id)                       AS empresa_id,
                de.empresa_nombre,
                COALESCE(csv.cuenta_codigo, lm.cuenta_codigo)                 AS cuenta_codigo,
                COALESCE(csv.descripcion, dc.nombre, lm.cuenta_codigo::text)  AS descripcion,
                csv.saldo_no_ajustado                                          AS saldo_csv,
                lm.saldo_acumulado                                             AS saldo_db,
                ROUND(
                    (COALESCE(csv.saldo_no_ajustado, 0)
                     - COALESCE(lm.saldo_acumulado, 0))::numeric, 2
                )                                                              AS diferencia,
                CASE
                    WHEN csv.cuenta_codigo IS NULL THEN 'Solo en DB'
                    WHEN lm.cuenta_codigo  IS NULL THEN 'Solo en CSV'
                    ELSE 'Diferencia'
                END AS tipo
            FROM stg_mayor_csv_cuenta csv
            FULL OUTER JOIN libro_mayor lm
                ON  lm.empresa_id    = csv.empresa_id
                AND lm.cuenta_codigo = csv.cuenta_codigo
                AND lm.periodo_anio  = csv.periodo_anio
                AND lm.periodo_mes   = csv.periodo_mes
                AND lm.nivel         = 'cuenta'
            LEFT JOIN dim_cuenta dc
                ON  dc.nro_cta = COALESCE(csv.cuenta_codigo, lm.cuenta_codigo)
            LEFT JOIN dim_empresa de
                ON  de.empresa_id = COALESCE(csv.empresa_id, lm.empresa_id)
            WHERE
                (csv.empresa_id = ANY(%s)
                 AND csv.periodo_anio = %s AND csv.periodo_mes = %s)
                OR
                (lm.empresa_id = ANY(%s)
                 AND lm.periodo_anio = %s AND lm.periodo_mes = %s
                 AND lm.nivel = 'cuenta')
            ORDER BY
                COALESCE(csv.empresa_id, lm.empresa_id),
                ABS(COALESCE(csv.saldo_no_ajustado, 0)
                    - COALESCE(lm.saldo_acumulado, 0)) DESC
            """,
            (empresa_ids, anio, mes, empresa_ids, anio, mes),
        )
        return cur.fetchall()
