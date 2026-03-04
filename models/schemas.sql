-- ============================================
-- SISTEMA DE REPORTES CONTABLES
-- Base de datos: Neon PostgreSQL
-- Versión 2 - Código de imputación real (INT)
-- ============================================

-- ============================================
-- TABLAS DE DIMENSIONES (MAESTROS)
-- ============================================

-- Empresas del grupo
CREATE TABLE IF NOT EXISTS dim_empresa (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(10) UNIQUE NOT NULL,
    nombre VARCHAR(200) NOT NULL,
    cuit VARCHAR(20),
    activa BOOLEAN DEFAULT TRUE,
    fecha_creacion TIMESTAMP DEFAULT NOW()
);

-- Plan de cuentas
-- codigo = código de imputación (columna Imput del Excel / nro_cta del diario)
-- Solo se cargan cuentas con código de imputación asignado (cuentas de movimiento)
CREATE TABLE IF NOT EXISTS dim_cuenta (
    id SERIAL PRIMARY KEY,
    codigo INT UNIQUE NOT NULL,           -- Código de imputación real (ej: 38, 3817)
    nombre VARCHAR(200) NOT NULL,
    codigo_jerarquico VARCHAR(30),         -- Código jerárquico de referencia (ej: 1.01.01.001)
    es_resultado BOOLEAN,
    tipo_subcta INT,
    moneda INT,
    nivel INT NOT NULL,
    activa BOOLEAN DEFAULT TRUE,
    fecha_creacion TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_cuenta_codigo ON dim_cuenta(codigo);
CREATE INDEX IF NOT EXISTS idx_dim_cuenta_activa ON dim_cuenta(activa);

-- ============================================
-- HOT STORAGE (últimos 6 meses - datos activos)
-- ============================================

CREATE TABLE IF NOT EXISTS libro_diario_abierto (
    id BIGSERIAL PRIMARY KEY,
    id_empresa INT NOT NULL REFERENCES dim_empresa(id),

    -- Datos del asiento
    fecha_asiento DATE NOT NULL,
    tipo_asiento INT,
    nro_asiento INT NOT NULL,
    nro_renglon INT NOT NULL,

    -- Cuenta y descripción
    -- codigo_cuenta = nro_cta del CSV, coincide con dim_cuenta.codigo
    codigo_cuenta INT NOT NULL,
    descripcion_cuenta VARCHAR(200),
    descripcion_movimiento TEXT,

    -- Subcuenta y comprobante
    tipo_subcta INT,
    nro_subcuenta NUMERIC,
    tipo_comprobante INT,
    sucursal INT,
    nro_comprobante NUMERIC,

    -- Tercero
    nombre_tercero VARCHAR(200),

    -- Importes
    debe NUMERIC(18,2) DEFAULT 0,
    haber NUMERIC(18,2) DEFAULT 0,

    -- Período
    periodo_anio INT NOT NULL,
    periodo_mes INT NOT NULL,

    -- Metadatos
    fecha_carga TIMESTAMP DEFAULT NOW(),
    fecha_carga_original TIMESTAMP,
    descripcion_asiento TEXT,
    referencia VARCHAR(50),

    CONSTRAINT unique_movimiento_abierto
        UNIQUE (id_empresa, fecha_asiento, tipo_asiento, nro_asiento, nro_renglon)
);

CREATE INDEX IF NOT EXISTS idx_abierto_empresa_periodo ON libro_diario_abierto(id_empresa, periodo_anio, periodo_mes);
CREATE INDEX IF NOT EXISTS idx_abierto_fecha ON libro_diario_abierto(fecha_asiento);
CREATE INDEX IF NOT EXISTS idx_abierto_cuenta ON libro_diario_abierto(codigo_cuenta);
CREATE INDEX IF NOT EXISTS idx_abierto_tercero ON libro_diario_abierto(nombre_tercero);

-- ============================================
-- COLD STORAGE (histórico > 6 meses)
-- ============================================

CREATE TABLE IF NOT EXISTS libro_diario_historico (
    id BIGSERIAL PRIMARY KEY,
    id_empresa INT NOT NULL REFERENCES dim_empresa(id),

    fecha_asiento DATE NOT NULL,
    tipo_asiento INT,
    nro_asiento INT NOT NULL,
    nro_renglon INT NOT NULL,
    codigo_cuenta INT NOT NULL,
    descripcion_cuenta VARCHAR(200),
    descripcion_movimiento TEXT,
    tipo_subcta INT,
    nro_subcuenta NUMERIC,
    tipo_comprobante INT,
    sucursal INT,
    nro_comprobante NUMERIC,
    nombre_tercero VARCHAR(200),
    debe NUMERIC(18,2) DEFAULT 0,
    haber NUMERIC(18,2) DEFAULT 0,
    periodo_anio INT NOT NULL,
    periodo_mes INT NOT NULL,
    fecha_carga TIMESTAMP,
    fecha_carga_original TIMESTAMP,
    descripcion_asiento TEXT,
    referencia VARCHAR(50),

    -- Metadatos del cierre
    fecha_cierre TIMESTAMP,
    semestre_cierre INT,

    CONSTRAINT unique_movimiento_historico
        UNIQUE (id_empresa, fecha_asiento, tipo_asiento, nro_asiento, nro_renglon)
);

CREATE INDEX IF NOT EXISTS idx_historico_empresa_periodo ON libro_diario_historico(id_empresa, periodo_anio, periodo_mes);
CREATE INDEX IF NOT EXISTS idx_historico_fecha ON libro_diario_historico(fecha_asiento);
CREATE INDEX IF NOT EXISTS idx_historico_cuenta ON libro_diario_historico(codigo_cuenta);

-- ============================================
-- LIBRO MAYOR CALCULADO (HOT)
-- ============================================

CREATE TABLE IF NOT EXISTS libro_mayor_abierto (
    id BIGSERIAL PRIMARY KEY,
    id_empresa INT NOT NULL REFERENCES dim_empresa(id),
    codigo_cuenta INT NOT NULL,
    periodo_anio INT NOT NULL,
    periodo_mes INT NOT NULL,

    -- Saldos del mes
    saldo_inicial NUMERIC(18,2) DEFAULT 0,
    total_debe NUMERIC(18,2) DEFAULT 0,
    total_haber NUMERIC(18,2) DEFAULT 0,
    saldo_final NUMERIC(18,2) DEFAULT 0,

    -- Metadatos
    fecha_calculo TIMESTAMP DEFAULT NOW(),

    CONSTRAINT unique_mayor_abierto
        UNIQUE (id_empresa, codigo_cuenta, periodo_anio, periodo_mes)
);

CREATE INDEX IF NOT EXISTS idx_mayor_abierto_empresa_periodo ON libro_mayor_abierto(id_empresa, periodo_anio, periodo_mes);
CREATE INDEX IF NOT EXISTS idx_mayor_abierto_cuenta ON libro_mayor_abierto(codigo_cuenta);

-- ============================================
-- LIBRO MAYOR CALCULADO (COLD)
-- ============================================

CREATE TABLE IF NOT EXISTS libro_mayor_historico (
    id BIGSERIAL PRIMARY KEY,
    id_empresa INT NOT NULL REFERENCES dim_empresa(id),
    codigo_cuenta INT NOT NULL,
    periodo_anio INT NOT NULL,
    periodo_mes INT NOT NULL,
    saldo_inicial NUMERIC(18,2) DEFAULT 0,
    total_debe NUMERIC(18,2) DEFAULT 0,
    total_haber NUMERIC(18,2) DEFAULT 0,
    saldo_final NUMERIC(18,2) DEFAULT 0,
    fecha_calculo TIMESTAMP,
    fecha_cierre TIMESTAMP,
    semestre_cierre INT,

    CONSTRAINT unique_mayor_historico
        UNIQUE (id_empresa, codigo_cuenta, periodo_anio, periodo_mes)
);

CREATE INDEX IF NOT EXISTS idx_mayor_historico_empresa_periodo ON libro_mayor_historico(id_empresa, periodo_anio, periodo_mes);
CREATE INDEX IF NOT EXISTS idx_mayor_historico_cuenta ON libro_mayor_historico(codigo_cuenta);

-- ============================================
-- CONTROL DE PERÍODOS Y CIERRES
-- ============================================

CREATE TABLE IF NOT EXISTS control_periodos (
    id SERIAL PRIMARY KEY,
    id_empresa INT NOT NULL REFERENCES dim_empresa(id),
    periodo_anio INT NOT NULL,
    periodo_mes INT NOT NULL,
    estado VARCHAR(20) NOT NULL,
    tipo VARCHAR(20) DEFAULT 'mensual',
    fecha_apertura TIMESTAMP,
    fecha_cierre TIMESTAMP,
    usuario_cierre VARCHAR(100),
    notas TEXT,

    CONSTRAINT unique_periodo
        UNIQUE (id_empresa, periodo_anio, periodo_mes)
);

CREATE INDEX IF NOT EXISTS idx_control_periodos_empresa ON control_periodos(id_empresa, periodo_anio, periodo_mes);

-- ============================================
-- LOG DE CARGAS (auditoría)
-- ============================================

CREATE TABLE IF NOT EXISTS log_cargas (
    id BIGSERIAL PRIMARY KEY,
    id_empresa INT NOT NULL REFERENCES dim_empresa(id),
    nombre_archivo VARCHAR(255) NOT NULL,
    periodo_anio INT NOT NULL,
    periodo_mes INT NOT NULL,
    total_registros INT NOT NULL,
    registros_insertados INT DEFAULT 0,
    registros_actualizados INT DEFAULT 0,
    registros_error INT DEFAULT 0,
    estado VARCHAR(50),
    mensaje_error TEXT,
    fecha_inicio TIMESTAMP DEFAULT NOW(),
    fecha_fin TIMESTAMP,
    usuario VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_log_cargas_empresa ON log_cargas(id_empresa, periodo_anio, periodo_mes);
CREATE INDEX IF NOT EXISTS idx_log_cargas_fecha ON log_cargas(fecha_inicio);

-- ============================================
-- VISTAS UNIFICADAS
-- ============================================

-- Vista unificada del libro diario (Hot + Cold)
CREATE OR REPLACE VIEW v_libro_diario_completo AS
SELECT
    'abierto' AS origen,
    id, id_empresa, fecha_asiento, tipo_asiento, nro_asiento, nro_renglon,
    codigo_cuenta, descripcion_cuenta, descripcion_movimiento,
    tipo_subcta, nro_subcuenta, tipo_comprobante, sucursal, nro_comprobante,
    nombre_tercero, debe, haber, periodo_anio, periodo_mes,
    fecha_carga, fecha_carga_original, descripcion_asiento, referencia
FROM libro_diario_abierto
UNION ALL
SELECT
    'historico' AS origen,
    id, id_empresa, fecha_asiento, tipo_asiento, nro_asiento, nro_renglon,
    codigo_cuenta, descripcion_cuenta, descripcion_movimiento,
    tipo_subcta, nro_subcuenta, tipo_comprobante, sucursal, nro_comprobante,
    nombre_tercero, debe, haber, periodo_anio, periodo_mes,
    fecha_carga, fecha_carga_original, descripcion_asiento, referencia
FROM libro_diario_historico;

-- Vista unificada del libro mayor (Hot + Cold)
CREATE OR REPLACE VIEW v_libro_mayor_completo AS
SELECT
    'abierto' AS origen,
    id, id_empresa, codigo_cuenta, periodo_anio, periodo_mes,
    saldo_inicial, total_debe, total_haber, saldo_final, fecha_calculo
FROM libro_mayor_abierto
UNION ALL
SELECT
    'historico' AS origen,
    id, id_empresa, codigo_cuenta, periodo_anio, periodo_mes,
    saldo_inicial, total_debe, total_haber, saldo_final, fecha_calculo
FROM libro_mayor_historico;

-- Vista del libro diario con nombre de cuenta (join con dim_cuenta)
CREATE OR REPLACE VIEW v_diario_con_cuenta AS
SELECT
    d.origen,
    d.id_empresa,
    d.fecha_asiento,
    d.tipo_asiento,
    d.nro_asiento,
    d.nro_renglon,
    d.codigo_cuenta,
    COALESCE(c.nombre, d.descripcion_cuenta) AS nombre_cuenta,
    c.codigo_jerarquico,
    d.descripcion_movimiento,
    d.nombre_tercero,
    d.debe,
    d.haber,
    d.periodo_anio,
    d.periodo_mes,
    d.descripcion_asiento,
    d.referencia
FROM v_libro_diario_completo d
LEFT JOIN dim_cuenta c ON c.codigo = d.codigo_cuenta;

-- Vista del libro mayor con nombre de cuenta
CREATE OR REPLACE VIEW v_mayor_con_cuenta AS
SELECT
    m.origen,
    m.id_empresa,
    m.codigo_cuenta,
    COALESCE(c.nombre, '') AS nombre_cuenta,
    c.codigo_jerarquico,
    m.periodo_anio,
    m.periodo_mes,
    m.saldo_inicial,
    m.total_debe,
    m.total_haber,
    m.saldo_final,
    m.fecha_calculo
FROM v_libro_mayor_completo m
LEFT JOIN dim_cuenta c ON c.codigo = m.codigo_cuenta;

-- ============================================
-- COMENTARIOS
-- ============================================

COMMENT ON TABLE dim_empresa IS 'Catálogo de empresas del grupo';
COMMENT ON TABLE dim_cuenta IS 'Plan de cuentas. codigo = código de imputación (nro_cta del libro diario)';
COMMENT ON TABLE libro_diario_abierto IS 'Movimientos contables de los últimos 6 meses (hot storage)';
COMMENT ON TABLE libro_diario_historico IS 'Movimientos contables históricos (cold storage)';
COMMENT ON TABLE libro_mayor_abierto IS 'Saldos mensuales por cuenta - últimos 6 meses';
COMMENT ON TABLE libro_mayor_historico IS 'Saldos mensuales por cuenta - histórico';
COMMENT ON TABLE control_periodos IS 'Control de apertura y cierre de períodos contables';
COMMENT ON TABLE log_cargas IS 'Auditoría de cargas de archivos';
COMMENT ON COLUMN dim_cuenta.codigo IS 'Código de imputación real. Coincide con nro_cta del libro diario CSV';
COMMENT ON COLUMN dim_cuenta.codigo_jerarquico IS 'Código jerárquico del plan (ej: 1.01.01.001). Solo referencial';
