###
 # @section Lectura de datos
 ##

#Lectura desde archivo de texto plano en local
dfDataLocal = spark.read.format("csv").\
option("header", "true").\
option("delimiter", "|").\
load("file:///dataset/persona.data")

#Mostramos la data
dfDataLocal.show()

#Mostramos el esquema de la data
dfDataLocal.printSchema()

#Lectura desde archivo de texto plano en HDFS
dfDataHdfs = spark.read.format("csv").\
option("header", "true").\
option("delimiter", "|").\
load("hdfs:///dataset/persona.data")

#Mostramos la data
dfDataHdfs.show()

#Lectura desde archivo JSON en local
dfJsonLocal=spark.read.format("json").\
option("multiLine", True).\
load("file:///dataset/persona.json")

#Mostramos la data
dfJsonLocal.show()
dfJsonLocal.printSchema()

#Lectura desde archivo JSON en HDFS
dfJsonHdfs=spark.read.format("json").\
option("multiLine", True).\
load("hdfs:///dataset/persona.json")

#Mostramos la data
dfJsonLocal.show()

#Lectura desde tabla HIVE
dfHive=spark.sql("select * from main_universal.transaccion_enriquecida")
dfHive.show()

#Mostramos el esquema de la data
dfHive.printSchema()