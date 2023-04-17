###
 # @section Convirtiendo dataframes en "TempView"
 ##

#Un "TempView" es un dataframe que puede ser manipulado con sentencias SQL
#Por ejemplo, leamos el archivo "persona.data" en un dataframe

dfPersona = spark.read.format("csv").\
option("header", "true").\
option("delimiter", "|").\
load("hdfs:///dataset/persona.data") # <--- Estamos leyendo el archivo desde el Sistem de archivos distribuidos HDFS
# load("file:///dataset/empresa.data") <--- O podriamos leer el archivo desde el Sistema de archivos local Getway


dfPersona.show()

#Creemos un TempView del resultado para poder manipularlo con SQL
#Por lo general se le pone el mismo nombre
dfPersona.createOrReplaceTempView("dfPersona")

#Manipulemoslo desde SQL
dfResultado = spark.sql("SELECT * FROM dfPersona WHERE EDAD > 40")
dfResultado.show()