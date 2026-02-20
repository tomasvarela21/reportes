"""
Script para recalcular el libro mayor en orden cronológico
Usa los datos ya existentes en libro_diario_abierto
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()


def recalcular_libro_mayor(codigo_empresa: str):
    database_url = os.getenv('DATABASE_URL')
    engine = create_engine(database_url)

    with engine.connect() as conn:
        # Obtener id_empresa
        result = conn.execute(
            text("SELECT id, nombre FROM dim_empresa WHERE codigo = :codigo"),
            {'codigo': codigo_empresa}
        ).fetchone()

        if not result:
            print(f"❌ Empresa '{codigo_empresa}' no encontrada")
            return

        id_empresa, nombre_empresa = result
        print(f"✅ Empresa: {nombre_empresa} (id={id_empresa})")

        # Obtener todos los períodos disponibles en orden cronológico
        periodos = conn.execute(
            text("""
                SELECT DISTINCT periodo_anio, periodo_mes
                FROM libro_diario_abierto
                WHERE id_empresa = :id_empresa
                ORDER BY periodo_anio, periodo_mes
            """),
            {'id_empresa': id_empresa}
        ).fetchall()

        print(f"📅 Períodos encontrados: {len(periodos)}")
        print(f"   Desde: {periodos[0][0]}/{periodos[0][1]:02d}")
        print(f"   Hasta: {periodos[-1][0]}/{periodos[-1][1]:02d}")
        print()

        # Recalcular mes a mes en orden
        for anio, mes in periodos:
            mes_anterior = mes - 1 if mes > 1 else 12
            anio_anterior = anio if mes > 1 else anio - 1

            # Obtener movimientos del mes
            movimientos = conn.execute(
                text("""
                    SELECT codigo_cuenta,
                           SUM(debe) as total_debe,
                           SUM(haber) as total_haber
                    FROM libro_diario_abierto
                    WHERE id_empresa = :id_empresa
                    AND periodo_anio = :anio
                    AND periodo_mes = :mes
                    GROUP BY codigo_cuenta
                """),
                {'id_empresa': id_empresa, 'anio': anio, 'mes': mes}
            ).fetchall()

            cuentas_procesadas = 0
            for mov in movimientos:
                codigo_cuenta = mov[0]
                total_debe = float(mov[1]) if mov[1] else 0.0
                total_haber = float(mov[2]) if mov[2] else 0.0

                # Obtener saldo del mes anterior
                saldo_ant = conn.execute(
                    text("""
                        SELECT saldo_final FROM libro_mayor_abierto
                        WHERE id_empresa = :id_empresa
                        AND codigo_cuenta = :codigo
                        AND periodo_anio = :anio
                        AND periodo_mes = :mes
                    """),
                    {'id_empresa': id_empresa, 'codigo': codigo_cuenta,
                     'anio': anio_anterior, 'mes': mes_anterior}
                ).fetchone()

                saldo_inicial = float(saldo_ant[0]) if saldo_ant else 0.0
                saldo_final = saldo_inicial + total_debe - total_haber

                # Verificar si ya existe
                existe = conn.execute(
                    text("""
                        SELECT id FROM libro_mayor_abierto
                        WHERE id_empresa = :id_empresa
                        AND codigo_cuenta = :codigo
                        AND periodo_anio = :anio
                        AND periodo_mes = :mes
                    """),
                    {'id_empresa': id_empresa, 'codigo': codigo_cuenta,
                     'anio': anio, 'mes': mes}
                ).fetchone()

                if existe:
                    conn.execute(
                        text("""
                            UPDATE libro_mayor_abierto
                            SET saldo_inicial = :saldo_inicial,
                                total_debe = :total_debe,
                                total_haber = :total_haber,
                                saldo_final = :saldo_final,
                                fecha_calculo = NOW()
                            WHERE id_empresa = :id_empresa
                            AND codigo_cuenta = :codigo
                            AND periodo_anio = :anio
                            AND periodo_mes = :mes
                        """),
                        {'id_empresa': id_empresa, 'codigo': codigo_cuenta,
                         'anio': anio, 'mes': mes,
                         'saldo_inicial': saldo_inicial,
                         'total_debe': total_debe,
                         'total_haber': total_haber,
                         'saldo_final': saldo_final}
                    )
                else:
                    conn.execute(
                        text("""
                            INSERT INTO libro_mayor_abierto
                            (id_empresa, codigo_cuenta, periodo_anio, periodo_mes,
                             saldo_inicial, total_debe, total_haber, saldo_final)
                            VALUES
                            (:id_empresa, :codigo, :anio, :mes,
                             :saldo_inicial, :total_debe, :total_haber, :saldo_final)
                        """),
                        {'id_empresa': id_empresa, 'codigo': codigo_cuenta,
                         'anio': anio, 'mes': mes,
                         'saldo_inicial': saldo_inicial,
                         'total_debe': total_debe,
                         'total_haber': total_haber,
                         'saldo_final': saldo_final}
                    )

                cuentas_procesadas += 1

            conn.commit()
            print(f"✅ {anio}/{mes:02d}: {cuentas_procesadas} cuentas procesadas")

    print()
    print("="*50)
    print("✅ Recálculo completado")
    print("="*50)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python recalcular_mayor.py CODIGO_EMPRESA")
        print("Ejemplo: python recalcular_mayor.py BATIA")
        sys.exit(1)

    codigo = sys.argv[1].upper()
    print("="*50)
    print(f"RECALCULANDO LIBRO MAYOR: {codigo}")
    print("="*50)
    print()
    recalcular_libro_mayor(codigo)