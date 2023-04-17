###
 # @section Librerías
 ##

#pip install kafka-python
#python

import json
import random
import time
from kafka import KafkaProducer
import requests

###
 # @section Configuración
 ##

#Ruta donde se localiza el archivo con los datos
CONF_FILE_PATH = "/dataset/transacciones.json"

#Servidor KAFKA donde se encuentra el producer
CONF_TOPIC_KAFKA_SERVER = "10.0.0.4:9092"

#Tópico KAFKA
CONF_TOPIC_KAFKA_NAME = "topic_transaccion"

#Cuántos registros son extraídos
CONF_SIZE_READ = 10

#Cada cuántos segundos son extraídos los registros  <--------------  Agregamos este parámetro
CONF_SLEEP_SECONDS = 0.1

#Cuántas iteraciones se realizarán                  <--------------  Agregamos este parámetro
CONF_NUMBER_ITERATIONS = 1000

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
	producer.send(CONF_TOPIC_KAFKA_NAME, dataFormatted.encode())
	#
	#Envíamos los datos al tópico
	producer.flush()

###
 # Detiene el proceso algunos segundos
 ##
def sleepProcess():
	time.sleep(CONF_SLEEP_SECONDS)


###
 # Programa principal
 ##
def main(args=None):
	#Realizaremos múltiples escrituras
	for i in range(0, CONF_NUMBER_ITERATIONS):
		#Obtenemos los datos de la fuente
		dataFromSource = readDataFromSource()
		#
		#Convertimos los datos a un formato compatible con KAFKA (string)
		dataFormatted = convertDataToFormatProducer(dataFromSource)
		#
		#Escribimos los datos en el producer
		print('Iteracion [{i}]: Escribiendo datos en el topico "{topic}"'.\
		replace("{i}", str(i+1)).\
		replace("{topic}", str(CONF_TOPIC_KAFKA_NAME))
		)
		writeDataToTopic(dataFormatted)
		#
		#Dormimos el proceso antes de comenzar con la siguiente iteración
		sleepProcess()

###
 # Ejecución del programa
 ##

main()