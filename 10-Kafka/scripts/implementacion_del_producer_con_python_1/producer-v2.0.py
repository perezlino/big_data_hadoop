###
 # @section Librerías
 ##

#pip install kafka-python

import json
import random
from kafka import KafkaProducer

###
 # @section Programa
 ##

#Si te das cuenta podríamos ordenar estos códigos y crear una función utilitaria que envíe los datos, vamos a hacer 
#eso. En python voy a crear una función llamada “writeDataToTopic()” la cual va a recibir los datos que queremos 
#enviar. Y, ¿qué es lo que va a hacer? número uno, va a instanciar un PRODUCER, número 2 va a enviar con ese PRODUCER 
#al tópico “topic_transaccion” los datos que le pasemos como parámetro a nuestra función y tercer paso ya sabemos que 
#hay que forzar el “flush” por si acaso porque a veces el “send” se puede quedar pensando un par de segundos. Creamos 
#entonces esos 3 pasos dentro de una función. 

def writeDataToTopic(data):
	#Instanciamos la conexión al servidor KAFKA
	producer = KafkaProducer(bootstrap_servers="10.0.0.4:9092")
	#
	#Preparamos el envío de los datos al tópico
	producer.send("topic_transaccion", data.encode())
	#
	#Envíamos los datos al tópico
	producer.flush()

writeDataToTopic("Hola mundo")
writeDataToTopic("Esta es una prueba")
writeDataToTopic("De escritura en un topico kafka")
writeDataToTopic('[{"ID_PERSONA": 89, "ID_EMPRESA": 4, "MONTO": 780}, {"ID_PERSONA": 66, "ID_EMPRESA": 5, "MONTO": 1447}, {"ID_PERSONA": 50, "ID_EMPRESA": 3, "MONTO": 736}]')