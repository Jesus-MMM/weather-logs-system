"""
Productor de Datos Meteorológicos
Envía datos simulados de estaciones meteorológicas a RabbitMQ
"""
import json
import time
import logging
import random
from datetime import datetime
from faker import Faker
import pika
from pika.exceptions import AMQPConnectionError, AMQPError
import math
from pathlib import Path

def configurar_logging():
    """Configurar logging"""
    try:
        # Crear directorio de logs si no existe
        log_dir = Path('/app/logs')
        log_dir.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(log_dir / 'producer.log', encoding='utf-8')
            ]
        )
        logger = logging.getLogger('weather_producer')
        logger.info("Logging configurado correctamente")

        return logger
    except Exception as e:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        logger = logging.getLogger('weather_producer')
        logger.warning(f"No se pudo configurar log en archivo: {e}.")
        return logger

logger = configurar_logging()

class ProductorMeteorologico:
    def __init__(self):
        self.faker = Faker('es_ES')
        self.connection = None
        self.channel = None
        self.estaciones = [
            {'id': 'EST-001', 'nombre': 'Estación Central', 'ciudad': 'Cartagena', 'altitud': 190},
            {'id': 'EST-002', 'nombre': 'Estación Norte', 'ciudad': 'Bogota', 'altitud': 650},
            {'id': 'EST-003', 'nombre': 'Estación Sur', 'ciudad': 'Medellin', 'altitud': 7},
            {'id': 'EST-004', 'nombre': 'Estación Este', 'ciudad': 'Barranquilla', 'altitud': 12},
            {'id': 'EST-005', 'nombre': 'Estación Oeste', 'ciudad': 'Cali', 'altitud': 100}
        ]
        
        # Configuración RabbitMQ
        self.rabbitmq_host = "rabbitmq"
        self.rabbitmq_port = 5672
        self.rabbitmq_user = "admin"
        self.rabbitmq_pass = "password"
        
        # Configuración del exchange
        self.exchange_name = 'exchange_meteorologico'
        self.exchange_type = 'direct'
        self.routing_key = 'datos.meteorologicos'

    def conectar_rabbitmq(self):
        """Establecer conexión con RabbitMQ con reconexión automática"""
        intentos = 0
        max_intentos = 10
        
        while intentos < max_intentos:
            try:
                logger.info(f"Intentando conectar a RabbitMQ ({intentos + 1}/{max_intentos})...")
                
                # Parámetros de conexión
                credentials = pika.PlainCredentials(self.rabbitmq_user, self.rabbitmq_pass)
                parameters = pika.ConnectionParameters(
                    host=self.rabbitmq_host,
                    port=self.rabbitmq_port,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
                
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                
                # Declarar exchange durable
                self.channel.exchange_declare(
                    exchange=self.exchange_name,
                    exchange_type=self.exchange_type,
                    durable=True
                )
                
                logger.info("Conexión a RabbitMQ establecida exitosamente")
                return True
                
            except AMQPConnectionError as e:
                intentos += 1
                logger.warning(f"Error de conexión: {e}. Reintentando en 5 segundos...")
                time.sleep(5)
            except Exception as e:
                intentos += 1
                logger.error(f"Error inesperado: {e}. Reintentando en 5 segundos...")
                time.sleep(5)
        
        logger.error("No se pudo establecer conexión después de todos los intentos")
        return False

    def generar_datos_meteorologicos(self, estacion):
        """Generar datos meteorológicos simulados para una estación"""
        
        # Base climática según ciudad
        clima_base = {
            'Cartagena': {'temp_base': 30, 'temp_var': 20, 'hum_base': 50},
            'Medellin': {'temp_base': 12, 'temp_var': 15, 'hum_base': 70},
            'Bogota': {'temp_base': 18, 'temp_var': 25, 'hum_base': 45},
            'Choco': {'temp_base': 16, 'temp_var': 18, 'hum_base': 65},
            'Lima': {'temp_base': 16, 'temp_var': 18, 'hum_base': 68}
        }
        
        clima = clima_base.get(estacion['ciudad'], clima_base['Cartagena'])
        
        # Variaciones estacionales y diurnas
        hora_actual = datetime.now().hour
        variacion_diurna = 10 * math.sin((hora_actual - 6) * math.pi / 12)  # Más calor al mediodía
        
        temperatura = round(
            clima['temp_base'] + 
            random.uniform(-5, 5) +  # Variación aleatoria
            variacion_diurna +
            random.uniform(-clima['temp_var']/2, clima['temp_var']/2),
            2
        )
        
        humedad = max(0, min(100, int(
            clima['hum_base'] + 
            random.randint(-20, 20) -
            (temperatura - clima['temp_base']) * 2  # Humedad inversa a temperatura
        )))
        
        precipitacion = round(random.uniform(0, 10), 2) if random.random() > 0.7 else 0.0
        presion = round(1013.25 + random.uniform(-20, 20), 2)
        velocidad_viento = round(random.uniform(0, 30), 2)
        direccion_viento = random.randint(0, 360)
        radiacion_solar = max(0, round(random.uniform(0, 1000) * (hora_actual/12 if hora_actual <= 12 else (24-hora_actual)/12), 2))
        indice_uv = round(random.uniform(0, 11), 1)
        visibilidad = round(random.uniform(5, 20), 2)
        cobertura_nubes = random.randint(0, 100)
        # Generar datos completos
        datos = {
            'id_estacion': estacion['id'],
            'nombre_estacion': estacion['nombre'],
            'ubicacion': estacion['ciudad'],
            'temperatura': temperatura,
            'humedad': humedad,
            'presion': presion,
            'velocidad_viento': velocidad_viento,
            'direccion_viento': direccion_viento,
            'precipitacion': precipitacion,
            'radiacion_solar': radiacion_solar,
            'indice_uv': indice_uv,
            'visibilidad': visibilidad,
            'cobertura_nubes': cobertura_nubes,
            'condicion_meteorologica': self.obtener_condicion_meteorologica(temperatura, humedad, precipitacion),
            'fecha_medicion': datetime.now().isoformat(),
            'id_mensaje': self.faker.uuid4()
        }
        
        return datos

    def obtener_condicion_meteorologica(self, temperatura, humedad, precipitacion):
        """Determinar condición meteorológica basada en los datos"""
        if precipitacion > 5:
            return 'lluvia_intensa'
        elif precipitacion > 1:
            return 'lluvia_moderada'
        elif precipitacion > 0:
            return 'lluvia_ligera'
        elif humedad > 90:
            return 'niebla'
        elif humedad > 80 and temperatura < 5:
            return 'nevando'
        elif humedad < 30:
            return 'soleado'
        elif temperatura > 30:
            return 'caluroso'
        elif temperatura < 0:
            return 'helado'
        else:
            return 'parcialmente_nublado'

    def enviar_datos(self, datos):
        """Enviar datos a RabbitMQ con manejo de errores"""
        try:
            if not self.channel or self.channel.is_closed:
                logger.warning("Canal cerrado, reconectando...")
                if not self.conectar_rabbitmq():
                    return False

            # Convertir a JSON y enviar
            mensaje = json.dumps(datos, ensure_ascii=False)
            
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=self.routing_key,
                body=mensaje,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Mensaje persistente
                    content_type='application/json',
                    timestamp=int(time.time())
                )
            )
            
            logger.info(f"Datos enviados - Estación: {datos['id_estacion']}, "
                       f"Temp: {datos['temperatura']}°C, "
                       f"Humedad: {datos['humedad']}%")
            return True
            
        except AMQPError as e:
            logger.error(f"Error AMQP al enviar datos: {e}")
            return False
        except Exception as e:
            logger.error(f"Error inesperado al enviar datos: {e}")
            return False

    def ejecutar(self):
        """Bucle principal de ejecución del productor"""
        logger.info("Iniciando Productor de Datos Meteorológicos")
        
        if not self.conectar_rabbitmq():
            logger.error("No se pudo iniciar el productor")
            return

        try:
            while True:
                # Seleccionar estación aleatoria
                estacion = random.choice(self.estaciones)
                
                # Generar datos
                datos = self.generar_datos_meteorologicos(estacion)
                
                # Enviar datos
                if self.enviar_datos(datos):
                    logger.debug(f"Mensaje confirmado - ID: {datos['id_mensaje']}")
                else:
                    logger.warning("Falló el envío, reintentando conexión...")
                    if not self.conectar_rabbitmq():
                        time.sleep(10)  # Esperar antes de reintentar conexión
                        continue
                
                # Esperar entre 2 y 8 segundos antes del próximo envío
                tiempo_espera = random.uniform(2, 8)
                time.sleep(tiempo_espera)
                
        except KeyboardInterrupt:
            logger.info("Deteniendo productor...")
        except Exception as e:
            logger.error(f"Error crítico en el productor: {e}")
        finally:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info("Conexión cerrada")

if __name__ == "__main__":
    productor = ProductorMeteorologico()
    productor.ejecutar()