###
 # @section Librerías
 ##

#pip install kafka-python
#pip install BeautifulSoup
#pip install beautifulsoup4
#python

import json
import random
import time
from kafka import KafkaProducer
import requests
from BeautifulSoup import BeautifulSoup

###
 # @section Configuración
 ##

#Servidor KAFKA donde se encuentra el producer
CONF_TOPIC_KAFKA_SERVER = "10.0.0.4:9092"

#Tópico KAFKA
CONF_TOPIC_KAFKA_NAME = "topic_transaccion"

#Cada cuántos segundos son extraídos los registros
CONF_SLEEP_SECONDS = 1

###
 # @section Funciones
 ##

def readDataFromSource(i):
	#import requests
	response = requests.get("https://www.forosperu.net/secciones/foro-libre.149/pagina-"+str(i))
	htmlData = response.text
	#print(htmlData)
	#type(htmlData)
	#
	return htmlData

###
 # Convierte los datos obtenidos de la fuente en un formato escribible en el producer (string)
 ##
def convertDataToFormatProducer(dataFromSource):
	#Convertimos los datos a string
	dataFormatted = dataFromSource
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
	producer.send(CONF_TOPIC_KAFKA_NAME, dataFormatted.encode("utf-8"))
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
		dataFromSource = readDataFromSource(i+1)
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