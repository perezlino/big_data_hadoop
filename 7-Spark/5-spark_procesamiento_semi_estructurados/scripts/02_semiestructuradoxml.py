###
 # @section Lectura de datos semi-estructurados
 ##

#Ejecutar SPARK cargando una librería
#pyspark2 --master local[1] --jars /binarios/spark-xml_2.11-0.4.1.jar

#Lectura desde archivo JSON en local
df = spark.read\
.format("com.databricks.spark.xml")\
.option("rootTag", "root")\
.option("rowTag", "element")\
.load("file:///dataset/transacciones_complejas.xml")

#Mostramos la data
df.show()

#Pintamos el esquema
df.printSchema()
