# Weather Logs System

sistema de gestión de logs de estaciones meteorológicas en tiempo real desde estaciones de medición, utilizando RabbitMQ como broker de mensajes y PostgreSQL como base de datos.

## Descripción

Este proyecto implementa una arquitectura de microservicios para la recopilación, validación y almacenamiento de datos meteorológicos. Los datos se envían desde productores (estaciones de medición) a través de RabbitMQ, y un consumidor los valida, procesa y persiste en PostgreSQL.

### Características principales

- **Validación robusta** de datos meteorológicos
- **Procesamiento asincrónico** con RabbitMQ
- **Persistencia confiable** en PostgreSQL
- **Manejo de errores** con ACK/NACK manual
- **Logging detallado** para auditoría y debugging
- **Containerización** con Docker y Docker Compose
- **Reintentos automáticos** de conexión

## Arquitectura

```
┌─────────────────────────────────────────┐
│      Productores de Datos               │
│  (Estaciones Meteorológicas)            │
└──────────────────┬──────────────────────┘
                   │
                   ▼
        ┌───────────────────────────┐
        │     RabbitMQ              │
        │  (exchange_meteorologico) │
        │  (cola_meteorologica)     │
        └───────────────────────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │     Consumer         │
        │  • Validación        │
        │  • Procesamiento     │
        │  • Persistencia      │
        └──────────────────────┘
                   │
                   ▼
        ┌───────────────────────────┐
        │    PostgreSQL             │
        │ registros_meteorologico   │
        └───────────────────────────┘
```

## Tecnologías

- **Python 3.9+**
- **RabbitMQ** 3.10+
- **PostgreSQL** 13+
- **Docker & Docker Compose**
- **psycopg2** - Driver PostgreSQL para Python
- **pika** - Cliente AMQP para Python

## Instalación y Configuración

### Requisitos previos

- Docker 20.10+
- Docker Compose 2.0+
- Git

### Pasos de instalación

1. **Clonar el repositorio**
```bash
git clone <https://github.com/Jesus-MMM/weather-logs-system>
cd weather-logs-system
```

2. **Configurar variables de entorno** (crear `.env` si es necesario)
```bash
# RabbitMQ
export RABBITMQ_HOST=rabbitmq
export RABBITMQ_PORT=5672
export RABBITMQ_USER=admin
export RABBITMQ_PASS=password

# PostgreSQL
export POSTGRES_HOST=postgres
export POSTGRES_PORT=5432
export POSTGRES_DB=weather_logs
export POSTGRES_USER=admin
export POSTGRES_PASSWORD=password
```

3. **Levantar los servicios con Docker Compose**
```bash
docker-compose up -d
```

4. **Verificar el estado de los servicios**
```bash
docker-compose ps
docker-compose logs -f consumer
```

## Esquema de Base de Datos

Tabla principal: `registros_meteorologicos`

| Campo | Tipo | Descripción |
|-------|------|-------------|
| id | SERIAL PRIMARY KEY | Identificador único |
| id_estacion | VARCHAR(50) | ID de la estación |
| nombre_estacion | VARCHAR(100) | Nombre de la estación |
| ubicacion | VARCHAR(200) | Ubicación geográfica |
| temperatura | FLOAT | Temperatura en °C (-50 a 60) |
| humedad | FLOAT | Humedad relativa (0-100%) |
| presion | FLOAT | Presión atmosférica (800-1100 hPa) |
| velocidad_viento | FLOAT | Velocidad en km/h (0-200) |
| direccion_viento | VARCHAR(20) | Dirección (N, NE, E, etc.) |
| precipitacion | FLOAT | Precipitación en mm |
| radiacion_solar | FLOAT | Radiación solar |
| indice_uv | FLOAT | Índice UV |
| visibilidad | FLOAT | Visibilidad en km |
| cobertura_nubes | FLOAT | Cobertura de nubes (%) |
| condicion_meteorologica | VARCHAR(50) | Descripción (Soleado, Nublado, etc.) |
| fecha_medicion | TIMESTAMP | Fecha/hora de la medición |
| id_mensaje | VARCHAR(50) | ID único del mensaje |
| calidad_dato | VARCHAR(20) | Estado: 'valido' o 'invalido' |
| notas_validacion | TEXT | Detalles de errores de validación |
| fecha_registro | TIMESTAMP | Fecha de inserción en BD |

## Formato de Mensajes

Los mensajes esperados en RabbitMQ deben ser JSON válido:

```json
{
  "id_mensaje": "msg_001",
  "id_estacion": "estacion_01",
  "nombre_estacion": "Estación Centro",
  "ubicacion": "Centro de la ciudad",
  "temperatura": 25.5,
  "humedad": 65.0,
  "presion": 1013.25,
  "velocidad_viento": 12.5,
  "direccion_viento": "NE",
  "precipitacion": 0.0,
  "radiacion_solar": 800.0,
  "indice_uv": 6.5,
  "visibilidad": 10.0,
  "cobertura_nubes": 20.0,
  "condicion_meteorologica": "Soleado",
  "fecha_medicion": "2024-01-15T14:30:00Z"
}
```

## Validaciones

El sistema valida los siguientes rangos:

- **Temperatura**: -50°C a 60°C
- **Humedad**: 0% a 100%
- **Presión**: 800 hPa a 1100 hPa
- **Velocidad de viento**: 0 km/h a 200 km/h
- **id_estacion**: Campo obligatorio

## Logs

Los logs se almacenan en `/app/logs/consumer.log` dentro del contenedor y también se envían a stdout.

Niveles de log:
- **DEBUG**: Información detallada de validadores y procesamiento
- **INFO**: Eventos importantes (inicialización, mensajes procesados)
- **WARNING**: Avisos de situaciones anómalas
- **ERROR**: Errores que requieren atención

Ejemplo de entrada de log:
```
2024-01-15 14:30:45,123 - weather_consumer - INFO - Procesando mensaje msg_001 de estación estacion_01
2024-01-15 14:30:46,234 - weather_consumer - INFO - Mensaje msg_001 procesado y guardado exitosamente
```

## Flujo de Procesamiento

1. **Recepción**: Consumer recibe mensaje de RabbitMQ
2. **Decodificación**: Parsea JSON del mensaje
3. **Validación**: Valida cada campo según rangos permitidos
4. **Persistencia**: Inserta en PostgreSQL
5. **ACK/NACK**: 
   - ACK si fue exitoso o datos inválidos (para no perder mensajes)
   - NACK si falla la persistencia (datos perdidos)

## Manejo de Errores

### Errores de Conexión
- Reintentos automáticos cada 5 segundos (máximo 10 intentos)
- Logging de intentos fallidos

### Errores de Validación
- Mensaje rechazado pero registrado en BD con estado 'invalido'
- ACK automático para evitar procesamiento repetido
- Detalles de errores guardados en `notas_validacion`

### Errores de Persistencia
- NACK y no reencolar (evita bucles infinitos)
- Logging de error con detalles de la excepción

## Comandos Docker Compose

```bash
# Iniciar servicios
docker-compose up -d

# Ver logs del consumer
docker-compose logs -f consumer

# Ver logs de RabbitMQ
docker-compose logs -f rabbitmq

# Ver logs de PostgreSQL
docker-compose logs -f postgres

# Detener servicios
docker-compose down

# Detener y eliminar volúmenes
docker-compose down -v
```