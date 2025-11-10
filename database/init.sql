-- =============================================
-- SISTEMA DE GESTION DE LOGS METEOROLOGICOS
-- Esquema de la base de datos
-- =============================================

-- =============================================
-- TABLA PRINCIPAL: registros meteorologicos
-- =============================================
CREATE TABLE IF NOT EXISTS registros_meteorologicos (
    -- Identificador unico
    id SERIAL PRIMARY KEY,
    
    -- Identificacion de la estacion
    id_estacion VARCHAR(50) NOT NULL,
    nombre_estacion VARCHAR(100),
    
    -- Datos meteorologicos principales
    temperatura DECIMAL(4,2),          -- Temperatura en grados centigrados (-99.99 a 99.99)
    humedad INTEGER CHECK (humedad >= 0 AND humedad <= 100),  -- Humedad 0-100%
    presion DECIMAL(6,2),             -- Presion atmosferica en hPa
    velocidad_viento DECIMAL(5,2),    -- Velocidad del viento en km/h
    direccion_viento INTEGER,         -- Direccion del viento en grados (0-360)
    precipitacion DECIMAL(5,2),       -- Precipitacion en mm
    
    -- Mediciones adicionales
    radiacion_solar DECIMAL(6,2),     -- Radiacion solar en W/m²
    indice_uv DECIMAL(3,1),           -- indice UV
    visibilidad DECIMAL(5,2),         -- Visibilidad en km
    cobertura_nubes INTEGER CHECK (cobertura_nubes >= 0 AND cobertura_nubes <= 100), -- 0-100%
    
    -- Condiciones atmosfericas
    condicion_meteorologica VARCHAR(50), -- 'soleado', 'lluvioso', 'nublado', etc.
    
    -- Metadatos del registro
    fecha_medicion TIMESTAMP NOT NULL,    -- Cuando se midieron los datos
    fecha_recepcion TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Cuando llegaron al sistema
    id_mensaje VARCHAR(100),           -- ID del mensaje en RabbitMQ
    
    -- Validacion y calidad de datos
    calidad_dato VARCHAR(20) DEFAULT 'valido' CHECK (calidad_dato IN ('valido', 'invalido', 'fuera_rango')),
    notas_validacion TEXT,             -- Notas sobre validacion
    
    -- Metadata del sistema
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- TABLA: Estaciones meteorologicas
-- =============================================
CREATE TABLE IF NOT EXISTS estaciones_meteorologicas (
    id_estacion VARCHAR(50) PRIMARY KEY,
    nombre_estacion VARCHAR(100) NOT NULL,
    nombre_ubicacion VARCHAR(100),
    latitud DECIMAL(9,6),             -- Latitud geografica
    longitud DECIMAL(9,6),            -- Longitud geografica
    altitud DECIMAL(6,2),             -- Altitud en metros
    tipo_estacion VARCHAR(50),        -- 'automatica', 'manual', 'investigacion'
    esta_activa BOOLEAN DEFAULT TRUE,
    descripcion TEXT,
    
    -- Limites de validacion especificos por estacion
    temperatura_minima DECIMAL(4,2) DEFAULT -50.0,
    temperatura_maxima DECIMAL(4,2) DEFAULT 60.0,
    velocidad_viento_maxima DECIMAL(5,2) DEFAULT 200.0,
    
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- TABLA: Alertas y umbrales
-- =============================================
CREATE TABLE IF NOT EXISTS alertas_meteorologicas (
    id SERIAL PRIMARY KEY,
    id_estacion VARCHAR(50) NOT NULL,
    tipo_alerta VARCHAR(50) NOT NULL,   -- 'temperatura_alta', 'viento_fuerte', etc.
    valor_umbral DECIMAL(8,2) NOT NULL,
    valor_actual DECIMAL(8,2) NOT NULL,
    severidad VARCHAR(20) CHECK (severidad IN ('baja', 'media', 'alta', 'critica')),
    mensaje_alerta TEXT NOT NULL,
    fecha_activacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    esta_confirmada BOOLEAN DEFAULT FALSE,
    fecha_confirmacion TIMESTAMP,
    confirmado_por VARCHAR(50)
);

-- =============================================
-- TABLA: Registros del sistema (errores, procesamiento)
-- =============================================
CREATE TABLE IF NOT EXISTS registros_sistema (
    id SERIAL PRIMARY KEY,
    nivel_log VARCHAR(20) NOT NULL CHECK (nivel_log IN ('DEBUG', 'INFO', 'ADVERTENCIA', 'ERROR', 'CRITICO')),
    componente VARCHAR(50) NOT NULL,    -- 'productor', 'consumidor', 'validador'
    mensaje TEXT NOT NULL,
    detalles JSONB,                     -- Datos adicionales en formato JSON
    traza_error TEXT,                   -- Stack trace en caso de errores
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- INDICES para optimizar consultas
-- =============================================

-- indices para registros_meteorologicos
CREATE INDEX IF NOT EXISTS idx_registros_id_estacion ON registros_meteorologicos(id_estacion);
CREATE INDEX IF NOT EXISTS idx_registros_fecha_medicion ON registros_meteorologicos(fecha_medicion);
CREATE INDEX IF NOT EXISTS idx_registros_temperatura ON registros_meteorologicos(temperatura);
CREATE INDEX IF NOT EXISTS idx_registros_fecha_creacion ON registros_meteorologicos(fecha_creacion);
CREATE INDEX IF NOT EXISTS idx_registros_estacion_medicion ON registros_meteorologicos(id_estacion, fecha_medicion);

-- indices para alertas_meteorologicas
CREATE INDEX IF NOT EXISTS idx_alertas_id_estacion ON alertas_meteorologicas(id_estacion);
CREATE INDEX IF NOT EXISTS idx_alertas_fecha_activacion ON alertas_meteorologicas(fecha_activacion);
CREATE INDEX IF NOT EXISTS idx_alertas_confirmadas ON alertas_meteorologicas(esta_confirmada);

-- indices para registros_sistema
CREATE INDEX IF NOT EXISTS idx_registros_sistema_componente ON registros_sistema(componente);
CREATE INDEX IF NOT EXISTS idx_registros_sistema_fecha_creacion ON registros_sistema(fecha_creacion);
CREATE INDEX IF NOT EXISTS idx_registros_sistema_nivel ON registros_sistema(nivel_log);

-- =============================================
-- VISTAS utiles para reportar
-- =============================================

-- Vista: Resumen diario por estacion
CREATE OR REPLACE VIEW resumen_diario_meteorologico AS
SELECT 
    id_estacion,
    DATE(fecha_medicion) as fecha_medicion,
    COUNT(*) as total_registros,
    ROUND(AVG(temperatura)::numeric, 2) as temperatura_promedio,
    MIN(temperatura) as temperatura_minima,
    MAX(temperatura) as temperatura_maxima,
    ROUND(AVG(humedad)::numeric, 2) as humedad_promedio,
    ROUND(AVG(velocidad_viento)::numeric, 2) as velocidad_viento_promedio,
    MAX(velocidad_viento) as velocidad_viento_maxima,
    SUM(precipitacion) as precipitacion_total
FROM registros_meteorologicos
WHERE calidad_dato = 'valido'
GROUP BY id_estacion, DATE(fecha_medicion);

-- Vista: Alertas pendientes
CREATE OR REPLACE VIEW alertas_pendientes AS
SELECT 
    a.*,
    e.nombre_estacion,
    e.nombre_ubicacion
FROM alertas_meteorologicas a
JOIN estaciones_meteorologicas e ON a.id_estacion = e.id_estacion
WHERE a.esta_confirmada = FALSE
ORDER BY a.fecha_activacion DESC;

-- Vista: Estadisticas de calidad de datos
CREATE OR REPLACE VIEW estadisticas_calidad_datos AS
SELECT 
    id_estacion,
    calidad_dato,
    COUNT(*) as total_registros,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY id_estacion)), 2) as porcentaje
FROM registros_meteorologicos
GROUP BY id_estacion, calidad_dato;

-- =============================================
-- FUNCIONES auxiliares
-- =============================================

-- Funcion: Actualizar automaticamente fecha_actualizacion
CREATE OR REPLACE FUNCTION actualizar_fecha_actualizacion()
RETURNS TRIGGER AS $$
BEGIN
    NEW.fecha_actualizacion = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers para actualizar fecha_actualizacion
CREATE TRIGGER actualizar_registros_fecha 
    BEFORE UPDATE ON registros_meteorologicos 
    FOR EACH ROW EXECUTE FUNCTION actualizar_fecha_actualizacion();

CREATE TRIGGER actualizar_estaciones_fecha 
    BEFORE UPDATE ON estaciones_meteorologicas 
    FOR EACH ROW EXECUTE FUNCTION actualizar_fecha_actualizacion();

-- Funcion: Validar datos meteorologicos
CREATE OR REPLACE FUNCTION validar_datos_meteorologicos()
RETURNS TRIGGER AS $$
BEGIN
    -- Validar temperatura dentro de rangos razonables
    IF NEW.temperatura < -50 OR NEW.temperatura > 60 THEN
        NEW.calidad_dato = 'fuera_rango';
        NEW.notas_validacion = CONCAT('Temperatura fuera de rango: ', NEW.temperatura);
    END IF;
    
    -- Validar humedad
    IF NEW.humedad < 0 OR NEW.humedad > 100 THEN
        NEW.calidad_dato = 'invalido';
        NEW.notas_validacion = CONCAT(COALESCE(NEW.notas_validacion, ''), ' Humedad inválida: ', NEW.humedad);
    END IF;
    
    -- Validar velocidad del viento (no negativa)
    IF NEW.velocidad_viento < 0 THEN
        NEW.calidad_dato = 'invalido';
        NEW.notas_validacion = CONCAT(COALESCE(NEW.notas_validacion, ''), ' Velocidad viento negativa: ', NEW.velocidad_viento);
    END IF;
    
    RETURN NEW;
END;
$$ language plpgsql;

-- Trigger para validacion automotica
CREATE TRIGGER validar_datos_entrada
    BEFORE INSERT ON registros_meteorologicos
    FOR EACH ROW EXECUTE FUNCTION validar_datos_meteorologicos();