# En Kafka solamente nos va a importar una cosa, así como en HBase sólo nos importaba crear una tabla de 
# manera correcta, por que las inserciones las vamos a hacer en tiempo real, en Kafka también nos va a 
# importar solamente crear un tópico de manera correcta, porque ya desde el lenguaje de programación vamos 
# a enviar toda la tormenta de datos a ese tópico Kafka. Kafka tiene 3 consolas, vamos a utilizar “Kafka 
# topics” que es la primera de ellas, para crear los tópicos voy a listar los tópicos existente en este 
# momento 

#Listar los tópicos existentes
kafka-topics --list \
--zookeeper 10.0.0.4:2181


# Para el parámetro “--replication-factor 1” el factor de replicación según el estándar acá siempre debe ir un 
# número 2, dos réplicas de todo lo que existe. Luego el número de partición “--partitions 1” para que el tópico 
# este distribuido en 3 servidores diferentes y no esté centralizado en uno, el estándar también nos dice que 
# aquí va un número 3. En el caso de nuestra infraestructura solamente tenemos un servidor para Kafka, por eso 
# le estoy poniendo un 1, pero en la vida real deberías tener al menos 3 servidores para poder tener 3 particiones 
# y 2 réplicas de cada uno, eso lo configuras con estos parámetros. Luego qué podría pasar, podría ser que en 
# algún momento alguien envíe un mensaje demasiado largo a Kafka, se supone que por ejemplo en nuestros tópicos 
# vamos a soportar la tormenta de datos y en promedio cada mensaje, no se pues, va a pesar 1 kb, pero ¿qué podría 
# pasar? que alguien envíe una trama de datos muy grande, se equivoca y envía 10 GB, ¿qué va a pasar? que va a 
# tumbar el tópico obviamente, el tópico está orientado a tamaños pequeños. Así que vamos a prevenir este tipo de 
# accidentes que suceden mucho en la vida real, ¿cómo? colocando un tamaño máximo de los mensajes que vamos a poder 
# admitir en bytes. En el parámetro “--config max.message.bytes=100000” le estoy diciendo a lo más el tópico va a 
# admitir mensajes de 100000 bytes, que es más o menos 1 MB. Más que eso el tópico no permite, porque implica que 
# por ahí ha pasado algo raro, se está enviando una trama que no debe enviarse. Este parámetro es muy importante 
# para controlar los tamaños máximo de lo que Kafka va a ir almacenando. Luego, tenemos que configurar el tiempo 
# de vida de los registros, se acuerdan que se dijo que por ejemplo podemos configurar para que todo lo que aterriza 
# en el tópico vive 1 hora, para eso tenemos este parámetro “--config retention.ms=604800000” y está indicado en 
# milisegundos. Acá por ejemplo se colocó el equivalente a 1 hora en milisegundos, por qué es parte del estándar 
# que es 1 hora. Y por supuesto tú puedes colocar la cantidad de milisegundos que tú quieras, pero el estándar es 
# el que colocamos. 

# Estos son los comandos que vas a necesitar el “list” y el “créate”, nada más, ya luego toda la integración la 
# tenemos que hacer en un lenguaje de programación. 

# Y finalmente con el parámetro “--topic topic_transaccion” indicamos cual es el nombre que quieres que tenga tu tópico. 

#Crear el tópico
kafka-topics --create \
--zookeeper 10.0.0.4:2181 \  <------------ Omitamoslo por un momento
--replication-factor 1 \
--partitions 1 \
--config max.message.bytes=100000 \
--config retention.ms=604800000 \
--topic topic_transaccion

#Listar los tópicos existentes
kafka-topics --list \
--zookeeper 10.0.0.4:2181


# En la vida real ahí terminó nuestro trabajo en Kafka, ya no tenemos que hacer nada más, ahora desde un lenguaje de 
# programación tendríamos que implementar un PRODUCER para enviarle la tormenta de datos y un CONSUMER para extraer 
# los datos y luego en PySpark para hacer algo con esos datos. ¿Pero qué otras cosas podrían pasar al momento de crear? 
# supongamos que nos hemos equivocado en alguna configuración, no se, o simplemente queremos actualizar algo, digamos 
# que ya no queremos que el tamaño máximo soportable sea de 1 MB ahora queremos que sea de 2 MB, ¿cómo podemos hacer un 
# alter de la configuración? para eso tenemos este otro comando, el comando “alter”. ¿Qué le vamos a indicar? Le voy a 
# decir: “…quiero modificar un tópico, para ello se indica el parámetro “--entity-type topics”, cuyo nombre se indica 
# en el parámetro “--entity-name topic_transaccion” y quiero modificar la siguiente configuración “max.message.bytes=128000” 
# esta configuración que controla los tamaños máximos de mensajes, ya no quiero que sea 1 MB, ahora quiero que sea 2 MB…” 
# con este parámetro puedes indicar modificaciones a tu configuración, esa es otra cosa que también podría llegar a pasar, 
# modificar una configuración de un tópico que hayas creado. Pero les adelanto que es muy raro, es muy raro, realmente con 
# lo que les he mostrado es suficiente, no hay más que hacer. 

#Modificar la configuracion
kafka-configs --alter \
--zookeeper 10.0.0.4:2181 \  <------------ Omitamoslo por un momento
--entity-type topics \
--entity-name topic_transaccion \
--add-config \
max.message.bytes=128000


# ¿Cómo sabemos que nuestro tópico se ha creado de manera correcta? qué es lo que pasa en la vida real, se creó el tópico y 
# le decimos a los desarrolladores:”… ya está listo el tópico, ya ponte a programar los Producers y los Consumers en el 
# lenguaje de programación para implementar el flujo real time. Se implementan y no funciona, no fluye la información de la 
# manera correcta, ¿qué es lo que podría pasar? o bien el tópico está mal o bien el código del PRODUCER o el CONSUMER, está 
# mal. Así que vamos a descartar el primer punto de falla, el tópico, que es lo más fácil de descartar, vamos a ver si el 
# tópico está bien, para eso Kafka te dice mira: “…tanto como el PRODUCER como el CONSUMER, tú lo puedes implementar en 
# Python, en Java o en algún lenguaje de programación, pero yo te voy a dar un PRODUCER en consola y un CONSUMER también 
# desde la consola para que pruebes y envíes algunos datos para ver si este flujo está funcionando, si ese flujo funciona 
# significa que en la codificación está el error, no en el tópico, si ese flujo no funciona, obviamente el error estaría en 
# el tópico, quizá hayan caído los servidores en el Clúster, que se yo, pero ahí ya podemos descartar errores…” 

# Para la gestión de creación de tópicos, la consola se llama “kafka-topics”, es la que te permite crear tópicos. Si queremos 
# alterar alguna configuración de un tópico previamente creado, la consola se llama “kafka-configs” y ya saben que aquí 
# indicamos cuál es la configuración a modificar. Así como tenemos una consola para crear tópicos y cambiar configuraciones, 
# vamos a tener una consola para crear Producers de prueba que se llama “kafka-console-producer”. Queremos crear un PRODUCER 
# que envíe algunos datos de prueba a este tópico “--topic topic_transaccion”, al que yo he creado.

#Crear un producer
kafka-console-producer \
--broker-list 10.0.0.4:9092 \  <------------ Omitamoslo por un momento
--topic topic_transaccion


# Vamos a abrir otra sesión y trabajaremos con el super usuario de Hadoop para evitar problemas. 

# sudo su
# su hdfs

# En la primera sesión tenemos un PRODUCER con el cual vamos a enviar datos desde consola para probar si el tópico está bien, 
# si le envío <Hola> y le doy Enter, eso va a aterrizar en el tópico. Ahora vamos a ver si desde un CONSUMER podemos extraer 
# ese <Hola>. En la segunda sesión (donde trabajaremos con el super usuario) vamos a utilizar otra consola, así como tenemos 
# una consola de prueba PRODUCER, tenemos una consola de prueba CONSUMER que apunta también al tópico.

# Vamos a ver si el tópico está funcionando de manera correcta. En la consola del PRODUCER (en la primera sesión) escribimos 
# <Hola> y presionamos Enter y se supone que en la sesión del CONSUMER debió haber llegado el <Hola> y efectivamente llegó 
# <Hola>. Este PRODUCER y este CONSUMER que nos da Kafka, solamente es para probar, para ver que realmente el tópico está 
# escuchando los datos, porque esto lo vamos a tener que implementar en algún lenguaje de programación o que, no se pues, que 
# se conecte a Facebook, que extraiga la data que la escriba en el tópico y luego otro CONSUMER, que todo lo que aterriza en 
# 1 segundo lo ponga en un archivo y la haga algo. Eso se hace con un lenguaje de programación, pero una vez que tú has creado 
# tu tópico tienes que hacer esto, un PRODUCER de prueba y un CONSUMER de prueba, para ver que está fluyendo la data. 

# De esta manera como hemos visto le hemos enviado algunos mensajes de prueba, ya tenemos garantía de que el flujo está correcto. 
# Entonces, y cuando alguien ya se ponga a programar el PRODUCER y el CONSUMER verdaderos que van a enviar cientos y cientos de 
# datos, te puede decir “…oye no está llegando la data al CONSUMER…” ah mira el problema no es el tópico, el Clúster está bien, 
# el problema es lo que tú has programado, por ahí te has equivocado en algo. Para eso se utilizan estas 2 consolas, la consola 
# PRODUCER de prueba y la consola CONSUMER de prueba para ver que la información está fluyendo. 

#Crear un consumer
kafka-console-consumer \
--bootstrap-server 10.0.0.4:9092 \
--topic topic_transaccion

#Eliminar un tópico
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
