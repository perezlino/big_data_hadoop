#Importamos la librería de funciones
import pyspark.sql.functions as f

#Leemos los datos con SPARK SQL
dfData = spark.sql("SELECT * FROM main_UNIVERSAL.PERSONA")

#Seleccionamos algunas columnas
#SELECT ID, NOMBRE, EDAD FROM dfData
df1 = dfData.select(dfData["ID"], dfData["NOMBRE"], dfData["EDAD"])
df1.show()

#Hacemos un filtro
#SELECT * FROM dfData WHERE EDAD > 60
df2 = dfData.filter(dfData["EDAD"] > 60)
df2.show()

#Hacemos un filtro con un "and"
#SELECT * FROM dfData WHERE EDAD > 60 AND SALARIO > 20000
df3 = dfData.filter((dfData["EDAD"] > 60) & (dfData["SALARIO"] > 20000))
df3.show()

#Hacemos un filtro con un "or"
#SELECT * FROM dfData WHERE EDAD > 60 OR SALARIO < 20
df4 = dfData.filter((dfData["EDAD"] > 60) | (dfData["EDAD"] < 20))
df4.show()

#Hacemos un GROUP BY
#SELECT 
#	EDAD
#	COUNT(EDAD)
#	MIN(FECHA_INGRESO)
#	SUM(SALARIO)
#	MAX(SALARIO)
#FROM
#	dfData
#GROUP BY
#	EDAD
df5 = dfData.groupBy("EDAD").agg(f.count("EDAD"), f.min("FECHA_INGRESO"), f.sum("SALARIO"), f.max("SALARIO"))
df5.show()

#Revisemos el esquema, notamos que las columnas reciben nombre "extraños"
df5.printSchema()

#Colocando alias
df6 = dfData.groupBy("EDAD").agg(\
f.count("EDAD").alias("CANTIDAD"), \
f.min("FECHA_INGRESO").alias("FECHA_CONTRATO_MAS_RECIENTE"), \
f.sum("SALARIO").alias("SUMA_SALARIOS"), \
f.max("SALARIO").alias("SALARIO_MAYOR")\
)

#Revisamos
df6.show()
df6.printSchema()

#Ordenar ascendentemente por un campo
df7 = dfData.sort(dfData["EDAD"].asc())
df7.show()

#Ordenar ascendentemente y descendentemente por más de un campo
df8 = dfData.sort(dfData["EDAD"].asc(), dfData["SALARIO"].desc())
df8.show()

#Joins

#Lectura de PERSONA
dfPersona = spark.sql("select * from main_universal.persona")
dfPersona.show()

#Lectura de TRANSACCIONES
dfTransaccion = spark.sql("select * from main_universal.transaccion")
dfTransaccion.show()

#Ejecución del JOIN
dfJoin = dfTransaccion.alias("T").join(
dfPersona.alias("P"), 
f.col("T.ID_PERSONA") == f.col("P.ID")
).select("P.NOMBRE", "P.EDAD", "P.SALARIO", "T.MONTO", "T.FECHA")

#Resultado
dfJoin.show()