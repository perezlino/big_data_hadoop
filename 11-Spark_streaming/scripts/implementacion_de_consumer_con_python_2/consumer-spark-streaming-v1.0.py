###
 # @section Librerías
 ##

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import datetime

###
 # @section Configuración
 ##

#Servidor KAFKA donde se encuentra el producer
CONF_TOPIC_ZOOKEEPER_SERVER = "10.0.0.4:9092"

#Tópico KAFKA
CONF_TOPIC_KAFKA = "topic_transaccion"

#Segundos en que Spark Streaming extrae los datos del tópicp
CONF_SECONDS_TO_READ_CONSUMER = 5

###
 # @section Funciones
 ##

###
 # Convierte los datos recibidos a un formato procesable
 ##
def formatData(register):
	registerFormatted = []
	#
	try:
		#Cada registro recibido es enviado por una tupla clave/valor
		#Esta tupla es un array de dos registros, el campo "0" es la clave y el campo "1" el registro
		#Por defecto, la "clave" siempre es None, sólo nos interesa el registro, así que leeremos el campo valor
		registerValue = register[1]
		#Convertimos el registro a un array de JSON
		registerFormatted = json.loads(registerValue)
	except Exception as e:
		#Si el string no tuviera un formato JSON, devolvemos un array vacío
		registerFormatted = []
	#
	return registerFormatted

###
 # Realiza el proceso de enriquecimiento
 ##
def enrichementData(register):
	#Le agregamos los campos de fecha y hora
	dateTime = datetime.datetime.now()
	register['FECHA_TRANSACCION'] = str(dateTime.date())
	register['HORA_TRANSACCION'] = str(dateTime.time())
	#
	return register

###
 # @section Inicialización del Stream
 ##

#Creación del Spark Streaming Context
ssc = StreamingContext(sc, CONF_SECONDS_TO_READ_CONSUMER)

#Conexión a Kafka
stream = KafkaUtils.createDirectStream(ssc, [CONF_TOPIC_KAFKA], {"metadata.broker.list": CONF_TOPIC_ZOOKEEPER_SERVER})


###
 # @section Lógica de procesamiento del Stream
 ##

#Procesamiento de los datos
stream.flatMap(lambda register : formatData(register)).\
map(lambda register : enrichementData(register)).pprint()

###
 # @section Ejecución del programa
 ##

#Iniciamos el programa
try:
	ssc.start()
except:
	print("El contexto streaming ya fue inicializado...")


#Esperamos hasta que finalice
ssc.awaitTermination()
