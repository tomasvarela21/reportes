"""
01_init_db_v2.py
================
Inicializa el schema v2 completo de ReporteApp en Neon.
Crea todas las tablas, índices y constraints necesarios.

Ejecutar DESPUÉS de 00_drop_all_tables.py

Uso:
    python 01_init_db_v2.py
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ No se encontró DATABASE_URL en el archivo .env")

# =============================================================================
# DDL — Tablas dimensionales (maestros)
# =============================================================================

DDL_DIM_EMPRESA = """
CREATE TABLE IF NOT EXISTS dim_empresa (
    codigo          VARCHAR(20)  PRIMARY KEY,
    nombre          VARCHAR(100) NOT NULL,
    razon_social    VARCHAR(200),
    cuit            VARCHAR(20),
    activa          BOOLEAN      NOT NULL DEFAULT TRUE,
    creado_en       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE dim_empresa IS 'Maestro de empresas del grupo corporativo';
"""

DDL_DIM_CUENTA = """
CREATE TABLE IF NOT EXISTS dim_cuenta (
    codigo          INTEGER      PRIMARY KEY,
    nombre          VARCHAR(200) NOT NULL,
    tipo            VARCHAR(50),       -- Activo, Pasivo, Patrimonio, Ingreso, Egreso
    es_resultado    BOOLEAN      NOT NULL DEFAULT FALSE,
    nivel           INTEGER,           -- 1=rubro, 2=cuenta, 3=subcuenta
    cuenta_padre    INTEGER      REFERENCES dim_cuenta(codigo),
    moneda          VARCHAR(10)  NOT NULL DEFAULT 'ARS',
    activa          BOOLEAN      NOT NULL DEFAULT TRUE,
    creado_en       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE dim_cuenta IS 'Plan de cuentas unificado para todas las empresas';
"""

DDL_DIM_CENTRO_COSTO = """
CREATE TABLE IF NOT EXISTS dim_centro_costo (
    codigo          VARCHAR(20)  PRIMARY KEY,
    descripcion     VARCHAR(200) NOT NULL,
    empresa         VARCHAR(20)  REFERENCES dim_empresa(codigo),  -- NULL = compartido
    activo          BOOLEAN      NOT NULL DEFAULT TRUE,
    creado_en       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE dim_centro_costo IS 'Maestro de centros de costo. empresa=NULL indica centro compartido';
"""

# =============================================================================
# DDL — Tablas transaccionales
# =============================================================================

DDL_LIBRO_DIARIO = """
CREATE TABLE IF NOT EXISTS libro_diario (
    id              BIGSERIAL    PRIMARY KEY,
    empresa         VARCHAR(20)  NOT NULL REFERENCES dim_empresa(codigo),
    fecha           DATE         NOT NULL,
    periodo_anio    SMALLINT     NOT NULL,
    periodo_mes     SMALLINT     NOT NULL CHECK (periodo_mes BETWEEN 1 AND 12),
    nro_asiento     VARCHAR(50),
    cuenta_codigo   INTEGER      NOT NULL REFERENCES dim_cuenta(codigo),
    debe            NUMERIC(18,2) NOT NULL DEFAULT 0,
    haber           NUMERIC(18,2) NOT NULL DEFAULT 0,
    descripcion     TEXT,
    tipo_subcuenta  VARCHAR(50),
    nro_subcuenta   VARCHAR(50),
    centro_costo    VARCHAR(20)  REFERENCES dim_centro_costo(codigo),  -- nullable
    cargado_en      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    archivo_origen  VARCHAR(255)
);
COMMENT ON TABLE libro_diario IS 'Asientos contables. Tabla única (sin hot/cold). Centro de costo opcional.';
"""

DDL_LIBRO_MAYOR = """
CREATE TABLE IF NOT EXISTS libro_mayor (
    id              BIGSERIAL    PRIMARY KEY,
    empresa         VARCHAR(20)  NOT NULL REFERENCES dim_empresa(codigo),
    periodo_anio    SMALLINT     NOT NULL,
    periodo_mes     SMALLINT     NOT NULL CHECK (periodo_mes BETWEEN 1 AND 12),
    cuenta_codigo   INTEGER      NOT NULL REFERENCES dim_cuenta(codigo),
    tipo_subcuenta  VARCHAR(50),
    nro_subcuenta   VARCHAR(50),
    centro_costo    VARCHAR(20)  REFERENCES dim_centro_costo(codigo),  -- nullable
    total_debe      NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_haber     NUMERIC(18,2) NOT NULL DEFAULT 0,
    saldo_periodo   NUMERIC(18,2) NOT NULL DEFAULT 0,  -- debe + haber del período (haber viene negativo)
    saldo_acumulado NUMERIC(18,2) NOT NULL DEFAULT 0,  -- acumulado desde apertura
    recalculado_en  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (empresa, periodo_anio, periodo_mes, cuenta_codigo, 
            tipo_subcuenta, nro_subcuenta, centro_costo)
);
COMMENT ON TABLE libro_mayor IS 'Mayor calculado y persistido. Nunca editar manualmente. Siempre recalcular.';
"""

DDL_SALDOS_APERTURA = """
CREATE TABLE IF NOT EXISTS saldos_apertura (
    id              BIGSERIAL    PRIMARY KEY,
    empresa         VARCHAR(20)  NOT NULL REFERENCES dim_empresa(codigo),
    anio_fiscal     SMALLINT     NOT NULL,
    cuenta_codigo   INTEGER      NOT NULL REFERENCES dim_cuenta(codigo),
    tipo_subcuenta  VARCHAR(50),
    nro_subcuenta   VARCHAR(50),
    centro_costo    VARCHAR(20)  REFERENCES dim_centro_costo(codigo),  -- nullable
    saldo           NUMERIC(18,2) NOT NULL DEFAULT 0,
    cargado_en      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    archivo_origen  VARCHAR(255),
    UNIQUE (empresa, anio_fiscal, cuenta_codigo, tipo_subcuenta, nro_subcuenta, centro_costo)
);
COMMENT ON TABLE saldos_apertura IS 'Saldos iniciales por año fiscal. Se usan como punto de partida del acumulado.';
"""

# =============================================================================
# DDL — Tabla de staging / input
# =============================================================================

DDL_INPUT_STAGING = """
CREATE TABLE IF NOT EXISTS input_staging (
    id              BIGSERIAL    PRIMARY KEY,
    empresa         VARCHAR(20)  NOT NULL REFERENCES dim_empresa(codigo),
    periodo_anio    SMALLINT     NOT NULL,
    periodo_mes     SMALLINT     NOT NULL CHECK (periodo_mes BETWEEN 1 AND 12),
    archivo_nombre  VARCHAR(255) NOT NULL,
    estado          VARCHAR(20)  NOT NULL DEFAULT 'pendiente'
                        CHECK (estado IN ('pendiente', 'validado', 'procesado', 'rechazado')),
    total_registros INTEGER,
    total_debe      NUMERIC(18,2),
    total_haber     NUMERIC(18,2),
    errores_json    JSONB,        -- detalle de errores de validación
    periodo_existia BOOLEAN      NOT NULL DEFAULT FALSE,
    subido_en       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    procesado_en    TIMESTAMPTZ
);
COMMENT ON TABLE input_staging IS 'Control de cargas. Registra cada intento de carga con su estado y errores.';
"""

# =============================================================================
# DDL — Log de recálculos del mayor
# =============================================================================

DDL_MAYOR_LOG = """
CREATE TABLE IF NOT EXISTS mayor_recalculo_log (
    id              BIGSERIAL    PRIMARY KEY,
    empresa         VARCHAR(20)  NOT NULL REFERENCES dim_empresa(codigo),
    desde_anio      SMALLINT     NOT NULL,
    desde_mes       SMALLINT     NOT NULL,
    hasta_anio      SMALLINT,
    hasta_mes       SMALLINT,
    motivo          VARCHAR(100),  -- 'carga_nueva', 'reemplazo_periodo', 'recarga_apertura'
    registros_afectados INTEGER,
    duracion_ms     INTEGER,
    ejecutado_en    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE mayor_recalculo_log IS 'Auditoría de cada recálculo del libro mayor. Útil para debugging.';
"""

# =============================================================================
# DDL — Índices
# =============================================================================

DDL_INDICES = """
-- libro_diario
CREATE INDEX IF NOT EXISTS idx_diario_empresa_periodo 
    ON libro_diario (empresa, periodo_anio, periodo_mes);

CREATE INDEX IF NOT EXISTS idx_diario_cuenta 
    ON libro_diario (cuenta_codigo);

CREATE INDEX IF NOT EXISTS idx_diario_centro_costo 
    ON libro_diario (centro_costo) WHERE centro_costo IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_diario_fecha 
    ON libro_diario (fecha);

-- libro_mayor
CREATE INDEX IF NOT EXISTS idx_mayor_empresa_periodo 
    ON libro_mayor (empresa, periodo_anio, periodo_mes);

CREATE INDEX IF NOT EXISTS idx_mayor_cuenta 
    ON libro_mayor (cuenta_codigo);

CREATE INDEX IF NOT EXISTS idx_mayor_centro_costo 
    ON libro_mayor (centro_costo) WHERE centro_costo IS NOT NULL;

-- saldos_apertura
CREATE INDEX IF NOT EXISTS idx_apertura_empresa_anio 
    ON saldos_apertura (empresa, anio_fiscal);

-- input_staging
CREATE INDEX IF NOT EXISTS idx_staging_empresa_periodo 
    ON input_staging (empresa, periodo_anio, periodo_mes);
"""

# =============================================================================
# DDL — Datos iniciales (empresas)
# =============================================================================

DDL_INSERT_EMPRESAS = """
INSERT INTO dim_empresa (codigo, nombre) VALUES
    ('BATIA',     'Batia S.A.'),
    ('GUARE',     'Guare S.A.'),
    ('NORFORK',   'Norfork S.A.'),
    ('TORRES',    'Torres S.A.'),
    ('WERCOLICH', 'Wercolich S.A.')
ON CONFLICT (codigo) DO NOTHING;
"""

# =============================================================================
# Runner
# =============================================================================

PASOS = [
    ("dim_empresa",           DDL_DIM_EMPRESA),
    ("dim_cuenta (preservada, IF NOT EXISTS)", DDL_DIM_CUENTA),
    ("dim_centro_costo",      DDL_DIM_CENTRO_COSTO),
    ("libro_diario",          DDL_LIBRO_DIARIO),
    ("libro_mayor",           DDL_LIBRO_MAYOR),
    ("saldos_apertura",       DDL_SALDOS_APERTURA),
    ("input_staging",         DDL_INPUT_STAGING),
    ("mayor_recalculo_log",   DDL_MAYOR_LOG),
    ("índices",               DDL_INDICES),
    ("datos iniciales",       DDL_INSERT_EMPRESAS),
]


def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    print("🚀 Inicializando schema v2 de ReporteApp en Neon...\n")

    try:
        for nombre, ddl in PASOS:
            cur.execute(ddl)
            print(f"  ✅ {nombre}")

        conn.commit()
        print("\n✅ Schema v2 creado correctamente.")
        print("\nTablas creadas:")
        cur.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename;
        """)
        for row in cur.fetchall():
            print(f"   📋 {row[0]}")

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error en paso '{nombre}': {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    init_db()