###
 # @section Definiendo los tipos de datos
 ##

#Importamos el "StructType" y el "StructField"
from pyspark.sql.types import *

#Leemos el archivo indicando el esquema
dfData = spark.read.format("csv").\
option("header", "true").\
option("delimiter", "|").\
schema(StructType([\
StructField("ID", StringType(), True),\
StructField("NOMBRE", StringType(), True),\
StructField("TELEFONO", StringType(), True),\
StructField("CORREO", StringType(), True),\
StructField("FECHA_INGRESO", StringType(), True),\
StructField("EDAD", IntegerType(), True),\
StructField("SALARIO", DoubleType(), True),\
StructField("ID_EMPRESA", StringType(), True)\
])).\
load("hdfs:///dataset/persona.data")

#Mostramos la data
dfData.show()

#Mostramos el esquema de la data
dfData.printSchema()