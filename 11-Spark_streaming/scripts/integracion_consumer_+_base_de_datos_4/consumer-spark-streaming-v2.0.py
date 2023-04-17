###
 # @section Librerías
 ##

#pip install happybase

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import happybase
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

#Servidor HBASE
CONF_HBASE_SERVER = "localhost"

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
	#Nos conectamos a HBASE
	connection = happybase.Connection(CONF_HBASE_SERVER)
	#
	#Obtenemos los datos de la empresa donde se realizó la transacción
	rowEmpresa = connection.table('MAIN_SMART_PROYECTO_4:EMPRESA').row(str(register['ID_EMPRESA']).encode())
	#
	#Obtenemos el nombre de la empresa
	nombreEmpresa = rowEmpresa['DATOS:NOMBRE'.encode()].decode('utf-8')
	#
	#Obtenemos los datos de la persona que realizó la transacción
	rowPersona = connection.table('MAIN_SMART_PROYECTO_4:PERSONA').row(str(register['ID_PERSONA']).encode())
	#
	#Obtenemos el nombre de la persona
	nombrePersona = rowPersona['DATOS:NOMBRE'.encode()].decode('utf-8')
	#
	#Enriquecemos el registro agregando nuevos campos
	register['NOMBRE_PERSONA'] = nombrePersona
	register['NOMBRE_EMPRESA'] = nombreEmpresa
	#
	#También le agregamos los campos de fecha y hora
	dateTime = datetime.datetime.now()
	register['FECHA_TRANSACCION'] = str(dateTime.date())
	register['HORA_TRANSACCION'] = str(dateTime.time())
	#
	#Almacenamos el registro en HBASE
	connection.table('MAIN_SMART_PROYECTO_4:TRANSACCION_ENRIQUECIDA').put(getHBaseRowKey(register), getHBaseRow(register))
	#
	return register

###
 # Obtiene la "key" para almacenar sobre HBASE
 ##
def getHBaseRowKey(register):
	#Concatenamos:
	#ID_PERSONA-ID_EMPRESA-FECHA-HORA
	return str(str(register['ID_PERSONA'])+'-'+str(register['ID_EMPRESA'])+'-'+str(register['FECHA_TRANSACCION'])+'-'+str(register['HORA_TRANSACCION'])).encode()

###
 # Obtiene el registro con el formato de almacenamiento de HBASE
 ##
def getHBaseRow(register):
	return {
		"PERSONA:ID".encode(): str(register['ID_PERSONA']).encode(),
		"PERSONA:NOMBRE".encode(): str(register['NOMBRE_PERSONA']).encode(),
		"EMPRESA:ID".encode(): str(register['ID_EMPRESA']).encode(),
		"EMPRESA:NOMBRE".encode(): str(register['NOMBRE_EMPRESA']).encode(),
		"TRANSACCION:FECHA".encode(): str(register['FECHA_TRANSACCION']).encode(),
		"TRANSACCION:HORA".encode(): str(register['HORA_TRANSACCION']).encode(),
		"TRANSACCION:MONTO".encode(): str(register['MONTO']).encode()
	}

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