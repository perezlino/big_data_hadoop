###
 # @section Lectura de datos semi-estructurados
 ##

#Lectura desde archivo JSON en local
dfJson=spark.read.format("json").\
option("multiLine", True).\
load("hdfs:///dataset/transacciones_complejas.json")

#Mostramos la data
dfJson.show()
dfJson.show(5, False)

#Pintamos el esquema
dfJson.printSchema()

###
 # @section Seleccionando campos
 ##

#Seleccionar algunos campos
df1 = dfJson.select("EMPRESA", "PERSONA")
df1.show(5, False)

#Seleccionar los subcampos
df2 = dfJson.select("EMPRESA.ID_EMPRESA", "EMPRESA.NOMBRE", "PERSONA.ID_PERSONA", "PERSONA.NOMBRE")
df2.show(5, False)

df3 = dfJson.select("PERSONA.ID_PERSONA", "PERSONA.NOMBRE", "PERSONA.CONTACTO")
df3.show(5, False)

df4 = dfJson.select("PERSONA.CONTACTO")
df4.show(5, False)

df5 = dfJson.select(dfJson["PERSONA.CONTACTO"])
df5.show(5, False)

df6 = dfJson.select(dfJson["PERSONA.CONTACTO"].getItem(0))
df6.show(5, False)

df7 = dfJson.select(dfJson["PERSONA.CONTACTO"].getItem(0).alias('CONTACTO_1'))
df7.show(5, False)

df8 = dfJson.select(
dfJson["PERSONA.NOMBRE"],
dfJson["PERSONA.CONTACTO"].getItem(0)['TIPO'], 
dfJson["PERSONA.CONTACTO"].getItem(0)['VALOR']
)
df8.show(5, False)

df9 = dfJson.select(
dfJson["PERSONA.NOMBRE"].alias('NOMBRE_PERSONA'),
dfJson["PERSONA.CONTACTO"].getItem(0)['TIPO'].alias('TIPO_1'), 
dfJson["PERSONA.CONTACTO"].getItem(0)['VALOR'].alias('VALOR_1')
)
df9.show(5, False)

df10 = dfJson.select(
dfJson["PERSONA.NOMBRE"].alias('NOMBRE_PERSONA'),
dfJson["PERSONA.CONTACTO"].getItem(0)['TIPO'].alias('TIPO_1'), 
dfJson["PERSONA.CONTACTO"].getItem(0)['VALOR'].alias('VALOR_1'),
dfJson["PERSONA.CONTACTO"].getItem(1)['TIPO'].alias('TIPO_2'), 
dfJson["PERSONA.CONTACTO"].getItem(1)['VALOR'].alias('VALOR_2'),
dfJson["PERSONA.CONTACTO"].getItem(2)['TIPO'].alias('TIPO_3'), 
dfJson["PERSONA.CONTACTO"].getItem(2)['VALOR'].alias('VALOR_3'),
dfJson["PERSONA.CONTACTO"].getItem(3)['TIPO'].alias('TIPO_4'), 
dfJson["PERSONA.CONTACTO"].getItem(3)['VALOR'].alias('VALOR_4')
)
df10.show(5, False)

###
 # @section Aplanando campos
 ##

from pyspark.sql.functions import explode

df11= dfJson.select(
	dfJson["PERSONA.NOMBRE"].alias('NOMBRE'), 
	explode("PERSONA.CONTACTO").alias('DATOS_CONTACTO')
)

df11.show(5, False)
df11.printSchema()

df12 = df11.filter(df11["DATOS_CONTACTO"]['TIPO'] == 'CORREO')
df12.show(5, False)
