"""
Servicio para cargar y gestionar saldos de apertura (cierre del ejercicio anterior).
Se usa para inicializar el libro mayor sin tener el histórico completo.
"""
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()


class CierreService:
    """Gestiona la carga de saldos de apertura desde archivo Excel o CSV"""

    def __init__(self):
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            raise ValueError("DATABASE_URL no configurado en .env")
        self.engine = create_engine(database_url, pool_pre_ping=True)

    # ------------------------------------------------------------------
    # CARGA DESDE ARCHIVO
    # ------------------------------------------------------------------

    def cargar_desde_excel(
        self, filepath: str, codigo_empresa: str, anio_apertura: int
    ) -> tuple:
        """
        Cargar saldos de apertura desde un Excel.

        El Excel debe tener al menos dos columnas:
          - nro_cuenta (o codigo): código imput de la cuenta
          - saldo: saldo de cierre del ejercicio anterior

        Args:
            filepath: Ruta al archivo Excel
            codigo_empresa: Código de empresa (ej: 'NORFORK')
            anio_apertura: Año al que corresponde el saldo (ej: 2023 si es cierre 2023)
        """
        try:
            df = pd.read_excel(filepath)
            df.columns = [c.strip().lower() for c in df.columns]

            # Mapear columnas flexibles
            col_cuenta = self._detectar_columna(df, ['nro_cuenta', 'codigo', 'cuenta', 'imput', 'nro cta'])
            col_saldo  = self._detectar_columna(df, ['saldo', 'saldo_final', 'saldo final', 'cierre'])

            if not col_cuenta:
                return False, "No se encontró columna de cuenta (esperado: nro_cuenta, codigo, imput)"
            if not col_saldo:
                return False, "No se encontró columna de saldo (esperado: saldo, saldo_final)"

            df = df[[col_cuenta, col_saldo]].copy()
            df.columns = ['codigo_cuenta', 'saldo']

            # Limpiar
            df['codigo_cuenta'] = pd.to_numeric(df['codigo_cuenta'], errors='coerce').astype('Int64')
            df['saldo']         = pd.to_numeric(df['saldo'], errors='coerce').fillna(0)
            df = df.dropna(subset=['codigo_cuenta'])
            df = df[df['codigo_cuenta'] != 0]

            ok, msg, id_empresa = self._get_id_empresa(codigo_empresa)
            if not ok:
                return False, msg

            # Validar cuentas
            cuentas_invalidas = self._validar_cuentas(df['codigo_cuenta'].tolist())
            if cuentas_invalidas:
                return False, f"Cuentas no encontradas en plan de cuentas: {cuentas_invalidas[:10]}"

            # Insertar
            insertados, actualizados = self._insertar_saldos(df, id_empresa, anio_apertura)

            return True, (
                f"Saldos de apertura cargados: {insertados} nuevos, {actualizados} actualizados "
                f"para {codigo_empresa} - Año {anio_apertura}"
            )

        except Exception as e:
            return False, f"Error al cargar saldos: {str(e)}"

    def cargar_desde_df(
        self, df: pd.DataFrame, codigo_empresa: str, anio_apertura: int
    ) -> tuple:
        """
        Cargar saldos desde un DataFrame ya preparado.
        El DataFrame debe tener columnas: codigo_cuenta (int), saldo (float)
        """
        try:
            ok, msg, id_empresa = self._get_id_empresa(codigo_empresa)
            if not ok:
                return False, msg

            insertados, actualizados = self._insertar_saldos(df, id_empresa, anio_apertura)
            return True, f"Cargados: {insertados} nuevos, {actualizados} actualizados"

        except Exception as e:
            return False, f"Error: {str(e)}"

    # ------------------------------------------------------------------
    # CONSULTAS
    # ------------------------------------------------------------------

    def obtener_saldos(self, codigo_empresa: str, anio: int) -> pd.DataFrame:
        """Obtener saldos de apertura de una empresa/año"""
        ok, msg, id_empresa = self._get_id_empresa(codigo_empresa)
        if not ok:
            return pd.DataFrame()

        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT sa.codigo_cuenta, dc.nombre, sa.saldo, sa.fecha_carga
                FROM saldos_apertura sa
                JOIN dim_cuenta dc ON dc.codigo = sa.codigo_cuenta
                WHERE sa.id_empresa = :id_empresa
                  AND sa.anio       = :anio
                ORDER BY sa.codigo_cuenta
            """), {'id_empresa': id_empresa, 'anio': anio})

            rows = result.fetchall()
            return pd.DataFrame(rows, columns=['codigo', 'nombre', 'saldo', 'fecha_carga'])

    def verificar_saldos(self, codigo_empresa: str, anio: int) -> dict:
        """Verificar integridad de saldos cargados"""
        df = self.obtener_saldos(codigo_empresa, anio)
        if df.empty:
            return {'ok': False, 'mensaje': f'No hay saldos de apertura para {codigo_empresa}/{anio}'}

        total_activo    = df[df['codigo'].astype(str).str.startswith('1')]['saldo'].sum() if len(df) else 0

        return {
            'ok':            True,
            'empresa':       codigo_empresa,
            'anio':          anio,
            'total_cuentas': len(df),
            'suma_saldos':   float(df['saldo'].sum()),
            'cuentas_con_saldo': int((df['saldo'] != 0).sum()),
        }

    def eliminar_saldos(self, codigo_empresa: str, anio: int) -> tuple:
        """Eliminar saldos de apertura de una empresa/año (para recargar)"""
        try:
            ok, msg, id_empresa = self._get_id_empresa(codigo_empresa)
            if not ok:
                return False, msg

            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    DELETE FROM saldos_apertura
                    WHERE id_empresa = :id_empresa AND anio = :anio
                """), {'id_empresa': id_empresa, 'anio': anio})
                conn.commit()
                return True, f"Eliminados {result.rowcount} saldos de {codigo_empresa}/{anio}"

        except Exception as e:
            return False, f"Error al eliminar: {str(e)}"

    # ------------------------------------------------------------------
    # HELPERS PRIVADOS
    # ------------------------------------------------------------------

    def _insertar_saldos(self, df: pd.DataFrame, id_empresa: int, anio: int) -> tuple:
        """Insertar o actualizar saldos usando ON CONFLICT"""
        registros = [
            {
                'id_empresa':    id_empresa,
                'codigo_cuenta': int(row['codigo_cuenta']),
                'anio':          anio,
                'saldo':         float(row['saldo']),
            }
            for _, row in df.iterrows()
        ]

        insertados   = 0
        actualizados = 0

        with self.engine.connect() as conn:
            for rec in registros:
                result = conn.execute(text("""
                    INSERT INTO saldos_apertura (id_empresa, codigo_cuenta, anio, saldo)
                    VALUES (:id_empresa, :codigo_cuenta, :anio, :saldo)
                    ON CONFLICT (id_empresa, codigo_cuenta, anio)
                    DO UPDATE SET saldo = EXCLUDED.saldo, fecha_carga = NOW()
                    RETURNING (xmax = 0) AS inserted
                """), rec)
                row = result.fetchone()
                if row and row[0]:
                    insertados += 1
                else:
                    actualizados += 1

            conn.commit()

        return insertados, actualizados

    def _get_id_empresa(self, codigo: str) -> tuple:
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT id FROM dim_empresa WHERE codigo = :codigo AND activa = TRUE"),
                {'codigo': codigo}
            ).fetchone()
            if result:
                return True, "OK", result[0]
            return False, f"Empresa '{codigo}' no encontrada", 0

    def _validar_cuentas(self, codigos: list) -> list:
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT codigo FROM dim_cuenta WHERE activa = TRUE")
            )
            validas = {int(r[0]) for r in result}
        return [c for c in codigos if int(c) not in validas]

    @staticmethod
    def _detectar_columna(df: pd.DataFrame, candidatas: list) -> str:
        """Detectar cuál columna del DataFrame coincide con las candidatas"""
        for c in candidatas:
            if c in df.columns:
                return c
        return None