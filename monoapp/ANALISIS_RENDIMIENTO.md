# 📊 Análisis de Cuellos de Botella - Sistema de Reportes Contables

## Resumen Ejecutivo

Se identificaron **7 problemas críticos** que ralentizan la subida de datos a la base de datos. Las soluciones pueden mejorar el rendimiento entre **50-300%** según el tamaño del lote.

---

## 🔴 Problemas Encontrados

### 1. **NullPool en config/database.py - IMPACTO: ALTA**

**Problema:**
```python
poolclass=NullPool  # No mantiene conexiones en pool
```

La configuración de `NullPool` crea una nueva conexión para cada operación y la cierra inmediatamente. Para ~10,000 registros esto significa abrir/cerrar la conexión cientos de veces.

**Impacto Estimado:** 40-50% del tiempo total

**Solución:**
```python
poolclass=QueuePool,
pool_size=5,
max_overflow=10,
pool_pre_ping=True,
pool_recycle=3600
```

**Ganancia esperada:** 40-50% más rápido

---

### 2. **Insert-una-por-una Sin Batch en calcular_libro_mayor - IMPACTO: ALTA**

**Problema en [db_service.py líneas 238-259]:**
```python
for mov in movimientos:  # Itera cuenta por cuenta
    existe = conn.execute(SELECT id FROM libro_mayor_abierto ...)  # Query 1
    if existe:
        conn.execute(UPDATE libro_mayor_abierto ...)  # Query 2
    else:
        conn.execute(INSERT INTO libro_mayor_abierto ...)  # Query 3
```

Para 500-1000 cuentas únicas = 1000-3000 queries separadas.

**Impacto Estimado:** 30-40% del tiempo total

**Solución:** Usar UPSERT (ON CONFLICT) en PostgreSQL con un solo batch command

---

### 3. **Sin Índices Compuestos en libro_mayor_abierto - IMPACTO: MEDIA**

**Problema:**
- Se busca por `(id_empresa, codigo_cuenta, periodo_anio, periodo_mes)` pero NO hay índice compuesto
- Cada UPDATE/INSERT genera table scans

**Índices Faltantes:**
```sql
-- FALTA este:
CREATE INDEX idx_libro_mayor_abierto_lookup 
ON libro_mayor_abierto(id_empresa, codigo_cuenta, periodo_anio, periodo_mes);
```

**Impacto Estimado:** 20-30% más lento en búsquedas

---

### 4. **Validaciones Sin IN() - IMPACTO: MEDIA**

**Problema en [validator.py líneas 87-110]:**
```python
# Obtiene TODAS las cuentas activas
result = conn.execute(
    text("SELECT codigo FROM dim_cuenta WHERE activa = TRUE")
)
cuentas_db = [row[0] for row in result]  # Lista completa

# Luego itera manualmente
cuentas_faltantes = [c for c in cuentas_archivo if c not in cuentas_db]
```

Si hay 10,000+ cuentas en BD, esto es ineficiente.

**Solución:** Usar SQL con NOT IN() o LEFT JOIN

**Impacto Estimado:** 10-20% más lento si hay muchas cuentas

---

### 5. **Múltiples Conexiones sin Reutilización - IMPACTO: MEDIA**

**Problemas en db_service.py:**

- Línea 341: Se abre nueva conexión para `_crear_log_carga()`
- Línea 326: Se abre otra para insertar datos
- Línea 313: Se abre otra para validaciones

**Solución:** Pasar misma conexión entre métodos

**Impacto Estimado:** 10-15% más lento

---

### 6. **Sin Índice en libro_diario_abierto.id_empresa - IMPACTO: BAJA-MEDIA**

**Problema:**
```sql
-- Está definido pero NO hay índice individual
CREATE INDEX idx_abierto_empresa_periodo 
ON libro_diario_abierto(id_empresa, periodo_anio, periodo_mes);
```

Falta índice individual en `id_empresa` para otras queries.

---

### 7. **Validación de Duplicados es Lenta - IMPACTO: BAJA**

**Problema en [validator.py líneas 127-145]:**
```python
# Hace COUNT(*) en tabla que podría tener millones
SELECT COUNT(*) FROM libro_diario_abierto 
WHERE id_empresa = ? AND periodo_anio = ? AND periodo_mes = ?
```

Con tabla grande, COUNT(*) es lento.

**Solución:** Usar `LIMIT 1` con EXISTS

---

## ✅ Soluciones Implementables

### Prioridad 1: CRÍTICA (50% mejora)

1. **Cambiar NullPool a QueuePool** 
2. **Implementar UPSERT batch en calcular_libro_mayor**
3. **Agregar índice compuesto en libro_mayor_abierto**

### Prioridad 2: IMPORTANTE (20-30% mejora)

4. **Optimizar validación de cuentas**
5. **Reutilizar conexiones**

### Prioridad 3: BUENO (10-15% mejora)

6. **Índices adicionales**
7. **Optimizar validación de duplicados**

---

## 📈 Benchmarks Esperados

| Etapa | Actual (est.) | Optimizado | Mejora |
|-------|---------------|-----------|--------|
| Inserción Diario | 2-3 min | 1-1.5 min | 40% |
| Cálculo Mayor | 3-4 min | 0.5-1 min | 75% |
| **TOTAL** | **5-7 min** | **1.5-2.5 min** | **60-70%** |

---

## 🔧 Archivos a Modificar

1. **config/database.py** - Cambiar pool
2. **services/db_service.py** - UPSERT batch + conexiones
3. **services/validator.py** - Optimizar validaciones
4. **schema.sql** - Agregar índices

