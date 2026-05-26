# Schema de Producción — ReporteApp (Neon PostgreSQL)

Fuente de verdad para el backend. Actualizar este archivo si cambia la DB.

---

## dim_empresa

| Columna        | Tipo     | Constraints      | Notas               |
|----------------|----------|------------------|---------------------|
| empresa_id     | INTEGER  | PK, NOT NULL     | 1=BATIA 2=NORFORK 3=GUARE 4=TORRES 5=WERCOLICH |
| empresa_nombre | VARCHAR  | NOT NULL         |                     |
| grupo          | VARCHAR  | nullable         |                     |
| activa         | BOOLEAN  | default true     |                     |

---

## dim_cuenta

| Columna      | Tipo         | Constraints  | Notas                           |
|--------------|--------------|--------------|----------------------------------|
| nro_cta      | INTEGER      | PK, NOT NULL |                                  |
| extendido    | VARCHAR      | nullable     | ej: 1.05.01.001                  |
| nombre       | VARCHAR      | NOT NULL     |                                  |
| rubro        | VARCHAR      | nullable     |                                  |
| sub_rubro    | VARCHAR      | nullable     |                                  |
| analisis     | VARCHAR      | nullable     |                                  |
| fases        | VARCHAR      | nullable     |                                  |
| tipo         | VARCHAR      | nullable     | Activo/Pasivo/Patrimonio/Resultado |
| moneda       | VARCHAR      | default 'ARS'| ARS/USD/EUR                      |
| activa       | BOOLEAN      | nullable     |                                  |
| es_resultado | VARCHAR(1)   | nullable     | 'S' o 'N'                        |
| nivel_1      | INTEGER      | nullable     |                                  |
| nivel_2      | INTEGER      | nullable     |                                  |
| nivel_3      | INTEGER      | nullable     |                                  |
| *cols_extra* | varias       | nullable     | columnas agregadas dinámicamente |

---

## dim_centro_costo

| Columna    | Tipo    | Constraints  | Notas           |
|------------|---------|--------------|-----------------|
| codigo     | VARCHAR | PK, NOT NULL |                 |
| descripcion| VARCHAR | nullable     |                 |
| empresa_id | INTEGER | FK dim_empresa, nullable |        |
| activo     | BOOLEAN | default true |                 |

---

## proyectos

**PK: ccosto (VARCHAR, NOT NULL)**

| Columna          | Tipo         | Constraints   | Notas                              |
|------------------|--------------|---------------|------------------------------------|
| ccosto           | VARCHAR      | PK, NOT NULL  | código de centro de costo          |
| nombre           | VARCHAR      | NOT NULL      |                                    |
| id_origen        | INTEGER      | nullable      | ID del sistema externo (CRM/ERP)   |
| oportunidad_id   | INTEGER      | nullable      |                                    |
| responsable_id   | INTEGER      | nullable      |                                    |
| version          | INTEGER      | nullable      |                                    |
| fc_inicio        | DATE         | nullable      |                                    |
| fc_fin           | DATE         | nullable      |                                    |
| deleted_at       | DATE         | nullable      | si tiene valor → activo=false      |
| actualizado_en   | TIMESTAMPTZ  | nullable      |                                    |
| estado           | VARCHAR      | nullable      |                                    |
| comentario       | TEXT         | nullable      |                                    |
| activo           | BOOLEAN      | nullable      |                                    |
| ingresos         | NUMERIC      | nullable      |                                    |
| cto_mo_propia    | NUMERIC      | nullable      |                                    |
| cto_mo_terceros  | NUMERIC      | nullable      |                                    |
| cto_materiales   | NUMERIC      | nullable      |                                    |
| cto_herramientas | NUMERIC      | nullable      |                                    |
| cto_diversos     | INTEGER      | nullable      | tipo INTEGER en producción         |
| superficie       | NUMERIC      | nullable      |                                    |
| avance           | NUMERIC      | default 0     |                                    |
| horas            | NUMERIC      | default 0     |                                    |

---

## proy_presupuestos

**PK: id (INTEGER, NOT NULL)**

| Columna     | Tipo        | Constraints  | Notas                         |
|-------------|-------------|--------------|-------------------------------|
| id          | INTEGER     | PK, NOT NULL |                               |
| proyecto_id | INTEGER     | NOT NULL     | FK proyectos.id_origen        |
| fecha       | DATE        | nullable     |                               |
| mo_propia   | NUMERIC     | nullable     |                               |
| mo_terceros | NUMERIC     | nullable     |                               |
| materiales  | NUMERIC     | nullable     |                               |
| herramientas| NUMERIC     | nullable     |                               |
| horas       | NUMERIC     | nullable     |                               |
| metros      | NUMERIC     | nullable     |                               |
| importe     | NUMERIC     | nullable     |                               |
| descripcion | TEXT        | nullable     |                               |
| cargado_en  | TIMESTAMPTZ | nullable     | se setea en INSERT (NOW())    |

---

## libro_diario

**PK: id (BIGINT, autoincrement)**

| Columna       | Tipo        | Constraints  | Notas                    |
|---------------|-------------|--------------|--------------------------|
| id            | BIGINT      | PK, serial   |                          |
| empresa_id    | INTEGER     | NOT NULL     | FK dim_empresa           |
| fecha         | DATE        | NOT NULL     |                          |
| periodo_anio  | INTEGER     | NOT NULL     |                          |
| periodo_mes   | INTEGER     | NOT NULL     |                          |
| tipo_asiento  | VARCHAR     | nullable     |                          |
| nro_asiento   | VARCHAR     | nullable     |                          |
| nro_renglon   | VARCHAR     | nullable     |                          |
| cuenta_codigo | INTEGER     | NOT NULL     | FK dim_cuenta.nro_cta    |
| debe          | NUMERIC     | NOT NULL     | positivo o 0             |
| haber         | NUMERIC     | NOT NULL     | negativo o 0             |
| descripcion   | TEXT        | nullable     |                          |
| tipo_subcuenta| VARCHAR     | nullable     |                          |
| nro_subcuenta | VARCHAR     | nullable     |                          |
| centro_costo  | VARCHAR     | nullable     |                          |
| cargado_en    | TIMESTAMPTZ | nullable     | timestamp de carga       |
| archivo_origen| VARCHAR     | nullable     | nombre del archivo fuente|

---

## libro_mayor

**PK: id (BIGINT, autoincrement)**

| Columna        | Tipo        | Constraints  | Notas                                        |
|----------------|-------------|--------------|----------------------------------------------|
| id             | BIGINT      | PK, serial   |                                              |
| empresa_id     | INTEGER     | NOT NULL     |                                              |
| periodo_anio   | INTEGER     | NOT NULL     |                                              |
| periodo_mes    | INTEGER     | NOT NULL     |                                              |
| fecha_periodo  | DATE        | nullable     |                                              |
| nivel          | VARCHAR     | NOT NULL     | 'cuenta' \| 'subcuenta' \| 'centro_costo'   |
| cuenta_codigo  | INTEGER     | NOT NULL     |                                              |
| tipo_subcuenta | VARCHAR     | nullable     |                                              |
| nro_subcuenta  | VARCHAR     | nullable     |                                              |
| centro_costo   | VARCHAR     | nullable     |                                              |
| total_debe     | NUMERIC     | NOT NULL     |                                              |
| total_haber    | NUMERIC     | NOT NULL     |                                              |
| saldo_anterior | NUMERIC     | NOT NULL     |                                              |
| saldo_periodo  | NUMERIC     | NOT NULL     |                                              |
| saldo_acumulado| NUMERIC     | NOT NULL     |                                              |
| recalculado_en | TIMESTAMPTZ | nullable     | timestamp del último recálculo               |

---

## Otras tablas (referencia)

### input_staging
Auditoría de cargas del libro diario.
Columnas: `id`, `empresa_id`, `periodo_anio`, `periodo_mes`, `archivo_nombre`,
`estado` ('pendiente'|'procesado'|'rechazado'), `total_registros`, `total_debe`,
`total_haber`, `periodo_existia`, `procesado_en`, `errores_json`.

### saldos_apertura
Saldos de inicio de ejercicio fiscal.
Columnas: `empresa_id`, `anio_fiscal`, `cuenta_codigo`, `tipo_subcuenta`,
`nro_subcuenta`, `centro_costo`, `saldo`.

### mayor_recalculo_log
Log de recálculos del libro mayor.
Columnas: `id`, `empresa_id`, `desde_anio`, `desde_mes`, `hasta_anio`, `hasta_mes`,
`motivo`, `registros_afectados`, `duracion_ms`, `ejecutado_en`.

### stg_mayor_csv_cuenta
Staging para chequeo de consistencia (CSV vs DB).
Columnas: `archivo_origen`, `empresa_id`, `periodo_anio`, `periodo_mes`,
`cuenta_codigo`, `descripcion`, `saldo_no_ajustado`, `cargado_en`.
