###
 # @section Librerías
 ##

#pip install kafka-python

import json
import random
from kafka import KafkaProducer

###
 # @section Configuración
 ##

#Ruta donde se localiza el archivo con los datos
CONF_FILE_PATH = "/dataset/transacciones.json"

#Servidor KAFKA donde se encuentra el producer
CONF_TOPIC_KAFKA_SERVER = "10.0.0.4:9092"

#Tópico KAFKA
CONF_TOPIC_KAFKA = "topic_transaccion"

#Cuántos registros son extraídos
CONF_SIZE_READ = 10

###
 # @section Funciones
 ##

###
 # Lee los datos desde la fuente
 ##
def readDataFromSource():
	#Leemos el archivo JSON
	with open(CONF_FILE_PATH) as file:
		jsonDataRead = json.load(file)
	#
	#Extraemos registros al azar
	jsonData = random.sample(jsonDataRead, CONF_SIZE_READ)
	#
	return jsonData

###
 # Convierte los datos obtenidos de la fuente en un formato escribible en el producer (string)
 ##
def convertDataToFormatProducer(dataFromSource):
	#Convertimos los datos a string
	dataFormatted = json.dumps(dataFromSource)
	#
	return dataFormatted

###
 # Escribe los datos obtenidos en el producer
 ##
def writeDataToTopic(dataFormatted):
	#Instanciamos la conexión al servidor KAFKA
	producer = KafkaProducer(bootstrap_servers=CONF_TOPIC_KAFKA_SERVER)
	#
	#Preparamos el envío de los datos al tópico
	producer.send(CONF_TOPIC_KAFKA, dataFormatted.encode())
	#
	#Envíamos los datos al tópico
	producer.flush()

###
 # Ejecución del programa
 ##

#Obtenemos los datos de la fuente
dataFromSource = readDataFromSource()

print(dataFromSource)
type(dataFromSource)  # list

print(dataFromSource[0])
type(dataFromSource[0])

print(dataFromSource[0]['MONTO'])
type(dataFromSource[0]['MONTO'])

#Convertimos los datos a un formato compatible con KAFKA (string)
dataFormatted = convertDataToFormatProducer(dataFromSource)
print(dataFormatted)
type(dataFormatted)

#Escribimos los datos en el producer
writeDataToTopic(dataFormatted)