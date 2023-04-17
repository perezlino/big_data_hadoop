# ¿Qué es esto del "zookeeper" ver que se está repitiendo en los comandos "kafka-topics" y "kafka-configs"? como 
# ahora estamos frente a 2 infraestructuras diferentes, ahora estamos frente a una infraestructura de servidores 
# para Kafka y una infraestructura de servidores en donde va, por ejemplo, a vivir el componente de software 
# CONSUMER, que va a extraer los datos del tópico que hemos creado y probablemente en otra infraestructura de 
# microservicios, vamos a hablar de esto en la siguiente clase, va estar escrito el PRODUCER. Tenemos 3 
# infraestructuras diferentes. Es muy importante que las configuraciones y los relojes de los servidores de estas 
# 3 infraestructuras diferentes estén sincronizados entre sí, a nivel de configuración y a nivel de reloj. Esto 
# tiene ya una explicación más que todo a nivel académico, que es por la forma en cómo trabajan las tecnologías por 
# detrás, pero para que la data fluya de manera correcta, tiene que estar todo sincronizado  

                                              16:07                      18:07  
                     __________          _________________         _________________ 
                    |          |        |  _____________  |       |                 |    
                    | Producer | ---->  | |_|_|_|_|_|_|_| | ----> | Consumer        |
                    |__________|        |_________________|       |_________________|  
                                          Infraestructura           Infraestructura
                                               Kafka                de procesamiento

# Un ejemplo muy simple, supongamos que el reloj aquí dice que son las 6:07 de la tarde y el reloj en la 
# infraestructura Kafka dice que son las 4:07 de la tarde. Hay 2 horas de diferencia. En la infraestructura Kafka 
# tenemos una configuración de que la data vive en 1 hora, entonces el CONSUMER va a querer extraer y va a haber 
# 2 horas de diferencia y simplemente va a decir: "…oye no puedo exceder la data porque están desincronizados los  
# relojes…". 

# De hecho, hay muchas otras cosas, las configuraciones también tienen que estar sincronizadas, bastante detalle 
# sincronizado. En Big data hay una herramienta llamada "ZOOKEEPER", la cual te asegura que tanto las configuraciones 
# de diferentes infraestructuras que no necesariamente están gobernadas por un mismo gestor de recursos van a estar 
# configuradas. Cuando ya empiezas a tener diferentes infraestructuras y quieres asegurarte de que todos tengan la misma 
# configuración, el estándar dice que deben haber 3 servidores en nuestro Clúster, pero como es académico solo tenemos 
# uno, tiene que haber 3 servidores ZOOKEEPER que garantice que todo esté bien sincronizado, y esta es otra infraestructura 
# aparte. 

                                         _________________ 
                                        |                 |  
                                        |    Zookeeper    |  
                                        |_________________|

                     __________          _________________         _________________ 
                    |          |        |  _____________  |       |                 |    
                    | Producer | ---->  | |_|_|_|_|_|_|_| | ----> | Consumer        |
                    |__________|        |_________________|       |_________________|  
                                          Infraestructura           Infraestructura
                                               Kafka                de procesamiento

                                              16:07                      18:07  


# Y justamente aquí estoy indicando cuál es el servidor cuál es la IP en donde está instalado el ZOOKEEPER, dos puntos y 
# por qué puerto está escuchando el ZOOKEEPER, en este ejemplo se encuentro en el puerto 2181. Esta información te lo 
# tendría que dar el encargado de la infraestructura, dónde está el ZOOKEEPER para que puedas usarlo aquí. En nuestro 
# Clúster solamente tenemos un servidor que tiene instalado esa tecnología, en estándares que tengan 3, así que si hay 
# otros 2 servidores más sería:

kafka-topics --create \
--zookeeper 10.0.0.4:2181, 10.0.0.5:2181, 10.0.0.6:2181 \   <---------------
--replication-factor 1 \
--partitions 1 \
--config max.message.bytes=100000 \
--config retention.ms=604800000 \
--topic topic_transaccion

# Si tuvieses más puedes seguir agregándolos con la coma. Sólo tenemos uno en nuestro ejemplo, así que un ZOOKEEPER. Misma 
# historia en "kafka-configs". 

# En el caso del PRODUCER y el CONSUMER de prueba, les estoy indicando con los parámetros "--broker-list 10.0.0.4:9092" y 
# "--bootstrap-server 10.0.0.4:9092" los servidores esclavos que forman parte de nuestro Clúster Kafka. En nuestro Clúster 
# solamente tenemos un servidor de Kafka, que también está en esta IP y está escuchando con este puerto. Sí nuestro Clúster 
# Kafka tuviese más servidores misma historia: 

kafka-console-producer \
--broker-list 10.0.0.4:9092, 10.0.0.5:9092 \   <---------------
--topic topic_transaccion

# El puerto estándar de Kafka es 9092. La IP y el puerto también nos las deben indicar. 

# ------------------------------------------------------------------------------------------------------------------------

#Listar los tópicos existentes
kafka-topics --list \
--zookeeper 10.0.0.4:2181

#Crear el tópico
kafka-topics --create \
--zookeeper 10.0.0.4:2181 \  
--replication-factor 1 \
--partitions 1 \
--config max.message.bytes=100000 \
--config retention.ms=604800000 \
--topic topic_transaccion

#Listar los tópicos existentes
kafka-topics --list \
--zookeeper 10.0.0.4:2181

#Modificar la configuracion
kafka-configs --alter \
--zookeeper 10.0.0.4:2181 \ 
--entity-type topics \
--entity-name topic_transaccion \
--add-config \
max.message.bytes=128000

#Crear un producer
kafka-console-producer \
--broker-list 10.0.0.4:9092 \
--topic topic_transaccion

#Crear un consumer
kafka-console-consumer \
--bootstrap-server 10.0.0.4:9092 \
--topic topic_transaccion

#Eliminar un tópico (No lo elimina inmediatamente, puede que se demore)
kafka-topics --delete \
--zookeeper 10.0.0.4:2181 \
--topic topic_transaccion

#Listar los tópicos existentes
kafka-topics --list \
--zookeeper 10.0.0.4:2181

#Volvemos a crear el tópico
kafka-topics --create --zookeeper 10.0.0.4:2181 --replication-factor 1 --partitions 1 --topic topic_transaccion

#Encendemos un consumer
kafka-console-consumer --bootstrap-server 10.0.0.4:9092 --topic topic_transaccion

#Salir del Producer y del Consumer
CTRL + C