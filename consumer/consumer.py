"""
Consumidor de Datos Meteorológicos
Recibe datos de RabbitMQ y los persiste en PostgreSQL
"""
import json
import time
import logging
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import pika
from pika.exceptions import AMQPConnectionError
import os
from pathlib import Path

# Configuración de logging
def configurar_logging():
    """Configurar logging para el consumer"""
    try:
        log_dir = Path('/app/logs')
        log_dir.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(log_dir / 'consumer.log', encoding='utf-8')
            ]
        )
        logger = logging.getLogger('weather_consumer')
        logger.info("Logging configurado correctamente para Consumer")
        return logger
    except Exception as e:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        logger = logging.getLogger('weather_consumer')
        logger.warning(f"No se pudo configurar log en archivo: {e}")
        return logger

logger = configurar_logging()

class ValidadorDatosMeteorologicos:
    """Valida los datos meteorológicos antes de persistirlos"""
    
    @staticmethod
    def validar_temperatura(temperatura):
        result = -50 <= temperatura <= 60
        logger.debug(f"Validador: validar_temperatura(temperatura={temperatura}) -> {result}")
        return result
    
    @staticmethod
    def validar_humedad(humedad):
        result = 0 <= humedad <= 100
        logger.debug(f"Validador: validar_humedad(humedad={humedad}) -> {result}")
        return result
    
    @staticmethod
    def validar_presion(presion):
        result = 800 <= presion <= 1100
        logger.debug(f"Validador: validar_presion(presion={presion}) -> {result}")
        return result
    
    @staticmethod
    def validar_velocidad_viento(velocidad):
        result = 0 <= velocidad <= 200
        logger.debug(f"Validador: validar_velocidad_viento(velocidad={velocidad}) -> {result}")
        return result
    
    @staticmethod
    def validar_datos_completos(datos : dict):
        """Validar todos los campos del mensaje"""
        logger.debug(f"Validador: iniciar validar_datos_completos para id_mensaje={datos.get('id_mensaje', 'N/A')} datos_keys={list(datos.keys())}")
        errores = []
        
        # Campos requeridos
        if not datos.get('id_estacion'):
            errores.append("id_estacion es requerido")
        
        if datos.get('temperatura') is not None and not ValidadorDatosMeteorologicos.validar_temperatura(datos['temperatura']):
            errores.append(f"Temperatura fuera de rango: {datos['temperatura']}")
        
        if datos.get('humedad') is not None and not ValidadorDatosMeteorologicos.validar_humedad(datos['humedad']):
            errores.append(f"Humedad fuera de rango: {datos['humedad']}")
        
        if datos.get('presion') is not None and not ValidadorDatosMeteorologicos.validar_presion(datos['presion']):
            errores.append(f"Presión fuera de rango: {datos['presion']}")
        
        if datos.get('velocidad_viento') is not None and not ValidadorDatosMeteorologicos.validar_velocidad_viento(datos['velocidad_viento']):
            errores.append(f"Velocidad viento fuera de rango: {datos['velocidad_viento']}")
        
        ok = len(errores) == 0
        if ok:
            logger.debug(f"Validador: datos válidos para id_mensaje={datos.get('id_mensaje','N/A')}")
        else:
            logger.info(f"Validador: datos inválidos para id_mensaje={datos.get('id_mensaje','N/A')}: {errores}")
        return ok, errores

class ConsumerMeteorologico:
    def __init__(self):
        self.connection_rabbit = None
        self.channel = None
        self.connection_db = None
        
        # Configuración RabbitMQ
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', 5672))
        self.rabbitmq_user = os.getenv('RABBITMQ_USER', 'admin')
        self.rabbitmq_pass = os.getenv('RABBITMQ_PASS', 'password')
        
        # Configuración PostgreSQL
        self.db_host = os.getenv('POSTGRES_HOST', 'postgres')
        self.db_port = int(os.getenv('POSTGRES_PORT', 5432))
        self.db_name = os.getenv('POSTGRES_DB', 'weather_logs')
        self.db_user = os.getenv('POSTGRES_USER', 'admin')
        self.db_pass = os.getenv('POSTGRES_PASSWORD', 'password')
        
        # Configuración RabbitMQ
        self.exchange_name = 'exchange_meteorologico'
        self.exchange_type = 'direct'
        self.queue_name = 'cola_meteorologica'
        self.routing_key = 'datos.meteorologicos'
        
        self.validador = ValidadorDatosMeteorologicos()
        # Nuevo log de inicialización (sin exponer contraseñas)
        try:
            logger.info(f"Inicializando ConsumerMeteorologico - rabbitmq={self.rabbitmq_host}:{self.rabbitmq_port} user={self.rabbitmq_user}, postgres={self.db_host}:{self.db_port} db={self.db_name} user={self.db_user}")
        except Exception:
            logger.debug("Inicializando ConsumerMeteorologico (no se pudieron leer detalles de configuración)")

    def conectar_rabbitmq(self):
        """Establecer conexión con RabbitMQ"""
        intentos = 0
        max_intentos = 10
        
        while intentos < max_intentos:
            try:
                logger.info(f"Conectando a RabbitMQ ({intentos + 1}/{max_intentos})...")
                
                credentials = pika.PlainCredentials(self.rabbitmq_user, self.rabbitmq_pass)
                parameters = pika.ConnectionParameters(
                    host=self.rabbitmq_host,
                    port=self.rabbitmq_port,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
                
                self.connection_rabbit = pika.BlockingConnection(parameters)
                self.channel = self.connection_rabbit.channel()
                
                # Declarar exchange durable
                self.channel.exchange_declare(
                    exchange=self.exchange_name,
                    exchange_type=self.exchange_type,
                    durable=True
                )
                
                # Declarar cola durable
                self.channel.queue_declare(
                    queue=self.queue_name,
                    durable=True
                )
                
                # Binding entre exchange y cola
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                    routing_key=self.routing_key
                )
                
                # Configurar prefetch_count=1 para procesamiento ordenado
                self.channel.basic_qos(prefetch_count=1)
                
                logger.info("Conexión a RabbitMQ establecida exitosamente")
                return True
                
            except AMQPConnectionError as e:
                intentos += 1
                logger.warning(f"Error de conexión RabbitMQ: {e}. Reintentando en 5 segundos...")
                time.sleep(5)
            except Exception as e:
                intentos += 1
                logger.error(f"Error inesperado RabbitMQ: {e}. Reintentando en 5 segundos...")
                time.sleep(5)
        
        logger.error("No se pudo establecer conexión con RabbitMQ")
        return False

    def conectar_postgresql(self):
        """Establecer conexión con PostgreSQL"""
        intentos = 0
        max_intentos = 10
        
        while intentos < max_intentos:
            try:
                logger.info(f"Conectando a PostgreSQL ({intentos + 1}/{max_intentos})...")
                
                self.connection_db = psycopg2.connect(
                    host=self.db_host,
                    port=self.db_port,
                    database=self.db_name,
                    user=self.db_user,
                    password=self.db_pass,
                    cursor_factory=RealDictCursor
                )
                
                logger.info("Conexión a PostgreSQL establecida exitosamente")
                return True
                
            except Exception as e:
                intentos += 1
                logger.error(f"Error conectando a PostgreSQL: {e}. Reintentando en 5 segundos...")
                time.sleep(5)
        
        logger.error("No se pudo establecer conexión con PostgreSQL")
        return False

    def procesar_mensaje(self, ch, method, properties, body):
        """
        Callback para procesar mensajes de RabbitMQ
        Con ACK manual según el resultado
        """
        mensaje_id = None
        try:
            # Decodificar mensaje
            datos = json.loads(body.decode('utf-8'))
            mensaje_id = datos.get('id_mensaje', 'N/A')
            
            logger.info(f"Procesando mensaje {mensaje_id} de estación {datos.get('id_estacion')}")
            
            # Validar datos
            es_valido, errores = self.validador.validar_datos_completos(datos)
            
            if not es_valido:
                logger.warning(f"Datos inválidos en mensaje {mensaje_id}: {errores}")
                # Guardar en tabla de errores o logs de invalidación
                self.guardar_datos_invalidos(datos, errores)
                ch.basic_ack(delivery_tag=method.delivery_tag)  # ACK incluso para inválidos
                return
            
            # Persistir en PostgreSQL
            if self.persistir_datos(datos):
                logger.info(f"Mensaje {mensaje_id} procesado y guardado exitosamente")
                ch.basic_ack(delivery_tag=method.delivery_tag)  # ACK manual
            else:
                logger.error(f"Error al persistir mensaje {mensaje_id}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  # NACK, no reencolar
                
        except json.JSONDecodeError as e:
            logger.error(f"Error decodificando JSON del mensaje: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.error(f"Error inesperado procesando mensaje {mensaje_id}: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def persistir_datos(self, datos):
        """Persistir datos validados en PostgreSQL"""
        logger.debug(f"Persistir: iniciando persistencia para id_mensaje={datos.get('id_mensaje','N/A')} id_estacion={datos.get('id_estacion')}")
        try:
            with self.connection_db.cursor() as cursor:
                # Determinar calidad del dato
                calidad_dato = 'valido'
                notas_validacion = None
                
                # Insertar en la tabla registros_meteorologicos
                query = sql.SQL("""
                    INSERT INTO registros_meteorologicos (
                        id_estacion, nombre_estacion, ubicacion,
                        temperatura, humedad, presion, 
                        velocidad_viento, direccion_viento, precipitacion,
                        radiacion_solar, indice_uv, visibilidad, cobertura_nubes,
                        condicion_meteorologica, fecha_medicion, id_mensaje,
                        calidad_dato, notas_validacion
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """)
                
                cursor.execute(query, [
                    datos.get('id_estacion'),
                    datos.get('nombre_estacion'),
                    datos.get('ubicacion'),
                    datos.get('temperatura'),
                    datos.get('humedad'),
                    datos.get('presion'),
                    datos.get('velocidad_viento'),
                    datos.get('direccion_viento'),
                    datos.get('precipitacion'),
                    datos.get('radiacion_solar'),
                    datos.get('indice_uv'),
                    datos.get('visibilidad'),
                    datos.get('cobertura_nubes'),
                    datos.get('condicion_meteorologica'),
                    datos.get('fecha_medicion'),
                    datos.get('id_mensaje'),
                    calidad_dato,
                    notas_validacion
                ])
                
                self.connection_db.commit()
                logger.debug(f"Persistir: commit exitoso para id_mensaje={datos.get('id_mensaje','N/A')}")
                return True
                
        except Exception as e:
            logger.error(f"Error en persistencia PostgreSQL para id_mensaje={datos.get('id_mensaje','N/A')}: {e}")
            self.connection_db.rollback()
            logger.debug("Persistir: rollback ejecutado")
            return False

    def guardar_datos_invalidos(self, datos, errores):
        """Guardar datos inválidos para análisis"""
        logger.debug(f"GuardarInvalidos: iniciando para id_mensaje={datos.get('id_mensaje','N/A')} errores_count={len(errores)}")
        try:
            with self.connection_db.cursor() as cursor:
                query = sql.SQL("""
                    INSERT INTO registros_meteorologicos (
                        id_estacion, nombre_estacion, ubicacion,
                        temperatura, humedad, presion, 
                        velocidad_viento, direccion_viento, precipitacion,
                        fecha_medicion, id_mensaje,
                        calidad_dato, notas_validacion
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """)
                
                cursor.execute(query, [
                    datos.get('id_estacion'),
                    datos.get('nombre_estacion'),
                    datos.get('ubicacion'),
                    datos.get('temperatura'),
                    datos.get('humedad'),
                    datos.get('presion'),
                    datos.get('velocidad_viento'),
                    datos.get('direccion_viento'),
                    datos.get('precipitacion'),
                    datos.get('fecha_medicion'),
                    datos.get('id_mensaje'),
                    'invalido',
                    '; '.join(errores)
                ])
                
                self.connection_db.commit()
                logger.info("Datos inválidos guardados para análisis")
                
        except Exception as e:
            logger.error(f"Error guardando datos inválidos id_mensaje={datos.get('id_mensaje','N/A')}: {e}")

    def iniciar_consumer(self):
        """Iniciar el consumo de mensajes de RabbitMQ"""
        logger.info("Iniciando Consumer de Datos Meteorológicos")
        
        if not self.conectar_rabbitmq() or not self.conectar_postgresql():
            logger.error("No se pudo iniciar el consumer")
            return

        try:
            # Configurar consumo con ACK manual
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self.procesar_mensaje,
                auto_ack=False  # ACK manual
            )
            
            logger.info("Iniciando consumo de mensajes...")
            self.channel.start_consuming()
            
        except KeyboardInterrupt:
            logger.info("Deteniendo consumer...")
        except Exception as e:
            logger.error(f"Error crítico en el consumer: {e}")
        finally:
            self.cerrar_conexiones()

    def cerrar_conexiones(self):
        """Cerrar todas las conexiones"""
        if self.connection_rabbit and not self.connection_rabbit.is_closed:
            self.connection_rabbit.close()
            logger.info("Conexión RabbitMQ cerrada")
        
        if self.connection_db and not self.connection_db.closed:
            self.connection_db.close()
            logger.info("Conexión PostgreSQL cerrada")

if __name__ == "__main__":
    consumer = ConsumerMeteorologico()
    consumer.iniciar_consumer()