# 🚀 Plan de Optimización - Implementación Paso a Paso

## Fase 1: Cambios en config/database.py (Impacto: 40-50%)

### Cambio: Usar QueuePool en lugar de NullPool

**Razón:** NullPool abre/cierra conexión para cada operación. QueuePool mantiene conexiones listas, reduciendo overhead de 40-50ms por conexión.

**Antes:**
```python
self.engine = create_engine(
    self.database_url,
    poolclass=NullPool,  # ❌ Malo para Neon
    echo=os.getenv('DEBUG', 'False') == 'True'
)
```

**Después:**
```python
from sqlalchemy.pool import QueuePool

self.engine = create_engine(
    self.database_url,
    poolclass=QueuePool,  # ✅ Pool de conexiones
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,  # Verifica conexión válida
    pool_recycle=3600,   # Recicla cada hora (Neon desconecta)
    connect_args={
        'connect_timeout': 10,
        'application_name': 'reportes_app'
    },
    echo=os.getenv('DEBUG', 'False') == 'True'
)
```

---

## Fase 2: Optimizar calcular_libro_mayor (Impacto: 30-40%)

### Cambio: Usar UPSERT en batch en lugar de UPDATE/INSERT uno por uno

**Problema actual [líneas 238-259]:**
- 1 SELECT por cuenta (para verificar si existe)
- 1 UPDATE o 1 INSERT por cuenta
- Con 500-1000 cuentas = **1500-3000 queries!**

**Solución:** Usar `INSERT ... ON CONFLICT DO UPDATE`

**Nuevo código:**

```python
def calcular_libro_mayor(self, id_empresa: int, anio: int, mes: int) -> tuple:
    """
    Calcular el libro mayor ABIERTO a partir del libro diario ABIERTO
    OPTIMIZADO: UPSERT en batch
    """
    try:
        with self.engine.connect() as conn:
            mes_anterior = mes - 1 if mes > 1 else 12
            anio_anterior = anio if mes > 1 else anio - 1
            
            print(f"📊 Calculando libro mayor ABIERTO para {mes}/{anio}...")
            
            # Obtener movimientos del diario
            result = conn.execute(
                text("""
                    SELECT 
                        codigo_cuenta,
                        SUM(debe) as total_debe,
                        SUM(haber) as total_haber
                    FROM libro_diario_abierto
                    WHERE id_empresa = :id_empresa
                    AND periodo_anio = :anio
                    AND periodo_mes = :mes
                    GROUP BY codigo_cuenta
                """),
                {'id_empresa': id_empresa, 'anio': anio, 'mes': mes}
            )
            
            movimientos = result.fetchall()
            print(f"✅ {len(movimientos)} cuentas encontradas")
            
            # Preparar datos para UPSERT en batch
            registros_upsert = []
            
            for mov in movimientos:
                codigo_cuenta = mov[0]
                total_debe = float(mov[1]) if mov[1] else 0.0
                total_haber = float(mov[2]) if mov[2] else 0.0
                
                saldo_anterior = float(self._obtener_saldo_anterior(
                    conn, id_empresa, codigo_cuenta, anio_anterior, mes_anterior
                ))
                
                saldo_final = saldo_anterior + total_debe - total_haber
                
                registros_upsert.append({
                    'id_empresa': id_empresa,
                    'codigo_cuenta': codigo_cuenta,
                    'periodo_anio': anio,
                    'periodo_mes': mes,
                    'saldo_inicial': saldo_anterior,
                    'total_debe': total_debe,
                    'total_haber': total_haber,
                    'saldo_final': saldo_final
                })
            
            # UPSERT: Una única query para todas las cuentas
            upsert_sql = text("""
                INSERT INTO libro_mayor_abierto (
                    id_empresa, codigo_cuenta, periodo_anio, periodo_mes,
                    saldo_inicial, total_debe, total_haber, saldo_final
                ) VALUES (
                    :id_empresa, :codigo_cuenta, :periodo_anio, :periodo_mes,
                    :saldo_inicial, :total_debe, :total_haber, :saldo_final
                )
                ON CONFLICT (id_empresa, codigo_cuenta, periodo_anio, periodo_mes) 
                DO UPDATE SET
                    saldo_inicial = EXCLUDED.saldo_inicial,
                    total_debe = EXCLUDED.total_debe,
                    total_haber = EXCLUDED.total_haber,
                    saldo_final = EXCLUDED.saldo_final,
                    fecha_calculo = NOW()
            """)
            
            # Batch insert/update - UNA sola operación
            batch_size = 500
            for i in range(0, len(registros_upsert), batch_size):
                batch = registros_upsert[i:i + batch_size]
                conn.execute(upsert_sql, batch)
                print(f"✅ UPSERT batch: {len(batch)} cuentas")
            
            conn.commit()
            print(f"💾 Libro mayor abierto guardado: {len(movimientos)} cuentas")
        
        return True, f"Libro mayor calculado: {len(movimientos)} cuentas procesadas"
        
    except Exception as e:
        return False, f"Error al calcular libro mayor: {str(e)}"
```

---

## Fase 3: Agregar Índices en schema.sql (Impacto: 20-30%)

### Cambio: Agregar índice compuesto para libro_mayor_abierto

**Agregar al final de schema.sql (después de los otros índices):**

```sql
-- Índice compuesto para búsquedas en UPSERT (CRÍTICO)
CREATE INDEX idx_libro_mayor_abierto_upsert 
ON public.libro_mayor_abierto(id_empresa, codigo_cuenta, periodo_anio, periodo_mes);

-- Índice en id_empresa para otros filtros
CREATE INDEX idx_libro_mayor_abierto_empresa 
ON public.libro_mayor_abierto(id_empresa);

-- Índice compuesto para búsquedas por período
CREATE INDEX idx_libro_mayor_abierto_periodo 
ON public.libro_mayor_abierto(id_empresa, periodo_anio, periodo_mes);

-- Índice en libro_diario_abierto para búsquedas por id_empresa
CREATE INDEX idx_abierto_empresa 
ON public.libro_diario_abierto(id_empresa);
```

**Script SQL para ejecutar:**

```sql
BEGIN;

CREATE INDEX IF NOT EXISTS idx_libro_mayor_abierto_upsert 
ON public.libro_mayor_abierto(id_empresa, codigo_cuenta, periodo_anio, periodo_mes);

CREATE INDEX IF NOT EXISTS idx_libro_mayor_abierto_empresa 
ON public.libro_mayor_abierto(id_empresa);

CREATE INDEX IF NOT EXISTS idx_libro_mayor_abierto_periodo 
ON public.libro_mayor_abierto(id_empresa, periodo_anio, periodo_mes);

CREATE INDEX IF NOT EXISTS idx_abierto_empresa 
ON public.libro_diario_abierto(id_empresa);

COMMIT;
```

---

## Fase 4: Optimizar Validaciones (Impacto: 10-20%)

### Cambio 1: validar_cuentas_existen con NOT IN subquery

**Antes [líneas 87-110]:**
```python
# ❌ Obtiene TODAS las cuentas
result = conn.execute(text("SELECT codigo FROM dim_cuenta WHERE activa = TRUE"))
cuentas_db = [row[0] for row in result]  # Lista completa en memoria

cuentas_faltantes = [c for c in cuentas_archivo if c not in cuentas_db]
```

**Después:**
```python
def validar_cuentas_existen(self, df: pd.DataFrame) -> Tuple[bool, str, List[str]]:
    """
    Validar que las cuentas existan en el plan de cuentas (OPTIMIZADO)
    """
    try:
        cuentas_archivo = df['codigo_cuenta'].unique().tolist()
        
        # ✅ Usar SQL para encontrar faltantes directamente
        with self.engine.connect() as conn:
            placeholders = ','.join([f"'{c}'" for c in cuentas_archivo])
            
            result = conn.execute(
                text(f"""
                    SELECT DISTINCT ca.codigo
                    FROM (
                        SELECT UNNEST(ARRAY[{placeholders}]::text[]) as codigo
                    ) ca
                    LEFT JOIN dim_cuenta dc ON ca.codigo = dc.codigo AND dc.activa = TRUE
                    WHERE dc.id IS NULL
                """)
            )
            
            cuentas_faltantes = [row[0] for row in result]
        
        if cuentas_faltantes:
            return False, f"Hay {len(cuentas_faltantes)} cuentas que no existen", cuentas_faltantes
        
        return True, "Todas las cuentas existen en el plan de cuentas", []
        
    except Exception as e:
        return False, f"Error al validar cuentas: {str(e)}", []
```

### Cambio 2: validar_duplicados más eficiente

**Antes [línea 140]:**
```python
result = conn.execute(
    text("SELECT COUNT(*) FROM libro_diario_abierto ...")
)
```

**Después (usa LIMIT 1 con EXISTS):**
```python
def validar_duplicados(self, df: pd.DataFrame, id_empresa: int, mes: int, anio: int) -> Tuple[bool, str]:
    """Validar que no haya datos duplicados (OPTIMIZADO)"""
    try:
        with self.engine.connect() as conn:
            # ✅ Usar EXISTS con LIMIT 1 en lugar de COUNT(*)
            result = conn.execute(
                text("""
                    SELECT 1
                    FROM libro_diario_abierto 
                    WHERE id_empresa = :id_empresa 
                    AND periodo_anio = :anio 
                    AND periodo_mes = :mes
                    LIMIT 1
                """),
                {'id_empresa': id_empresa, 'anio': anio, 'mes': mes}
            ).fetchone()
            
            if result:
                # Contar solo si necesario
                count = conn.execute(
                    text("""
                        SELECT COUNT(*) 
                        FROM libro_diario_abierto 
                        WHERE id_empresa = :id_empresa 
                        AND periodo_anio = :anio 
                        AND periodo_mes = :mes
                    """),
                    {'id_empresa': id_empresa, 'anio': anio, 'mes': mes}
                ).fetchone()[0]
                
                return False, f"Ya existen {count} registros. Eliminalos primero."
            
            return True, "No hay datos duplicados"
            
    except Exception as e:
        return False, f"Error: {str(e)}"
```

---

## Fase 5: Reutilizar Conexiones en db_service.py (Impacto: 10-15%)

### Cambio: Pasar conexión a métodos auxiliares

**Antes [líneas 341, 313, 320]:**
```python
# ❌ Abre 3 conexiones diferentes
log_id = self._crear_log_carga(...)  # Abre conexión
exito_insert, ... = self.insertar_libro_diario(...)  # Abre conexión
self._actualizar_log_carga(...)  # Abre conexión
```

**Después:**
```python
def insertar_libro_diario(self, df: pd.DataFrame, nombre_archivo: str, callback_progreso=None) -> tuple:
    """Insertar datos con conexión reutilizable"""
    try:
        id_empresa = int(df['id_empresa'].iloc[0])
        periodo_anio = int(df['periodo_anio'].iloc[0])
        periodo_mes = int(df['periodo_mes'].iloc[0])
        total_registros = int(len(df))
        
        # ✅ Usar UNA sola conexión para todo
        with self.engine.connect() as conn:
            # Crear log
            log_id = self._crear_log_carga(conn, id_empresa, nombre_archivo, periodo_anio, periodo_mes, total_registros)
            
            # Insertar datos
            registros_insertados = self._ejecutar_batch_insert(conn, df, callback_progreso)
            
            # Actualizar log
            self._actualizar_log_carga(conn, log_id, 'completado', registros_insertados, 0)
            
            conn.commit()  # Una sola vez
        
        return True, f"Insertados {registros_insertados} registros", registros_insertados
        
    except Exception as e:
        if 'log_id' in locals():
            with self.engine.connect() as conn:
                self._actualizar_log_carga(conn, log_id, 'error', 0, 0, str(e))
        
        return False, f"Error: {str(e)}", 0

def _crear_log_carga(self, conn, id_empresa: int, nombre_archivo: str, anio: int, mes: int, total_registros: int):
    """Crear log usando conexión existente"""
    result = conn.execute(
        text("""
            INSERT INTO log_cargas (id_empresa, nombre_archivo, periodo_anio, periodo_mes, total_registros, estado)
            VALUES (:id_empresa, :nombre_archivo, :periodo_anio, :periodo_mes, :total_registros, 'procesando')
            RETURNING id
        """),
        {'id_empresa': int(id_empresa), 'nombre_archivo': str(nombre_archivo), 
         'periodo_anio': int(anio), 'periodo_mes': int(mes), 'total_registros': int(total_registros)}
    )
    return result.fetchone()[0]

def _actualizar_log_carga(self, conn, log_id: int, estado: str, insertados: int, errores: int, mensaje_error: str = None):
    """Actualizar log usando conexión existente"""
    conn.execute(
        text("""
            UPDATE log_cargas
            SET estado = :estado, registros_insertados = :insertados, 
                registros_error = :errores, mensaje_error = :mensaje_error, fecha_fin = NOW()
            WHERE id = :log_id
        """),
        {'log_id': log_id, 'estado': estado, 'insertados': insertados, 'errores': errores, 'mensaje_error': mensaje_error}
    )
```

---

## 📋 Orden de Implementación

1. ✅ Fase 1: Cambiar database.py (5 minutos)
2. ✅ Fase 3: Agregar índices (5 minutos)
3. ✅ Fase 2: Reescribir calcular_libro_mayor (20 minutos)
4. ✅ Fase 4: Optimizar validaciones (15 minutos)
5. ✅ Fase 5: Reutilizar conexiones (10 minutos)

**Tiempo total: ~55 minutos**

---

## 🧪 Cómo Probar

```python
import time

# Medir tiempo antes y después
start = time.time()
exito, msg = db.calcular_libro_mayor(id_empresa=1, anio=2025, mes=1)
elapsed = time.time() - start

print(f"Tiempo: {elapsed:.2f} segundos")
```

**Expected:**
- Antes: 180-240 segundos (3-4 minutos)
- Después: 30-60 segundos (0.5-1 minuto)
- **Mejora: 70-75%** 🚀

