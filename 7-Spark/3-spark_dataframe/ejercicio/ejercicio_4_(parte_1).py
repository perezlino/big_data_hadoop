#Importamos la librería de funciones
import pyspark.sql.functions as f

###
 # @section Parámetros
 ##

username = "MIUSUARIO"

###
 # @section Lectura de datos
 ##

dfPersona = spark.sql("""
SELECT 
* 
FROM 
{var:username}_UNIVERSAL.PERSONA
""".\
replace("{var:username}", username)
)

dfPersona.show()

dfEmpresa = spark.sql("""
SELECT 
* 
FROM 
{var:username}_UNIVERSAL.EMPRESA
""".\
replace("{var:username}", username)
)

dfEmpresa.show()

dfTransaccion = spark.sql("""
SELECT 
* 
FROM 
{var:username}_UNIVERSAL.TRANSACCION
""".\
replace("{var:username}", username)
)

dfTransaccion.show()

###
 # @section Procesamiento
 ##

#Obtenemos el nombre de la empresa donde trabaja la persona
dfPersonaEnriquecida = dfPersona.alias("P").join(
	dfEmpresa.alias("E"), 
	f.col("P.ID_EMPRESA") == f.col("E.ID")
).select(
	f.col("P.ID").alias("ID_PERSONA"),
	f.col("P.NOMBRE").alias("NOMBRE_PERSONA"),
	f.col("P.EDAD").alias("EDAD_PERSONA"),
	f.col("P.SALARIO").alias("SALARIO_PERSONA"),
	f.col("E.NOMBRE").alias("TRABAJO_PERSONA")
)

dfPersonaEnriquecida.show()

#Enriquecemos la transacción con los datos de la persona
dfPersonaEnriquecidaTransaccion = dfPersonaEnriquecida.alias("P").join(
	dfTransaccion.alias("T"), 
	f.col("P.ID_PERSONA") == f.col("T.ID_PERSONA")
).select(
	f.col("P.ID_PERSONA").alias("ID_PERSONA"),
	f.col("P.NOMBRE_PERSONA").alias("NOMBRE_PERSONA"),
	f.col("P.EDAD_PERSONA").alias("EDAD_PERSONA"),
	f.col("P.SALARIO_PERSONA").alias("SALARIO_PERSONA"),
	f.col("P.TRABAJO_PERSONA").alias("TRABAJO_PERSONA"),
	f.col("T.ID_EMPRESA").alias("ID_EMPRESA_TRANSACCION"),
	f.col("T.MONTO").alias("MONTO_TRANSACCION"),
	f.col("T.FECHA").alias("FECHA_TRANSACCION")
)

dfPersonaEnriquecidaTransaccion.show()

#Enriquecemos la transacción colocando el nombre de la empresa donde se realizó la transacción
dfReporte1 = dfPersonaEnriquecidaTransaccion.alias("P").join(
	dfEmpresa.alias("E"), 
	f.col("P.ID_EMPRESA_TRANSACCION") == f.col("E.ID")
).select(
	f.col("P.ID_PERSONA").alias("ID_PERSONA"),
	f.col("P.NOMBRE_PERSONA").alias("NOMBRE_PERSONA"),
	f.col("P.EDAD_PERSONA").alias("EDAD_PERSONA"),
	f.col("P.SALARIO_PERSONA").alias("SALARIO_PERSONA"),
	f.col("P.TRABAJO_PERSONA").alias("TRABAJO_PERSONA"),
	f.col("P.MONTO_TRANSACCION").alias("MONTO_TRANSACCION"),
	f.col("P.FECHA_TRANSACCION").alias("FECHA_TRANSACCION"),
	f.col("E.NOMBRE").alias("EMPRESA_TRANSACCION")
)

dfReporte1.show()

#Guardamos el dataframe de reporte en la tabla Hive
dfReporte1.createOrReplaceTempView("dfReporte1")

#Activamos el particionamiento dinamico
spark.sql("SET hive.exec.dynamic.partition=true")
spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

#Almacenamos el resultado en Hive
spark.sql("""
INSERT INTO {var:username}_UNIVERSAL.TRANSACCION_ENRIQUECIDA
SELECT
*
FROM 
dfReporte1 T
""".\
replace("{var:username}", username)
)

#Verificamos que el reporte se haya insertado correctamente
spark.sql("""
SELECT 
* 
FROM 
{var:username}_UNIVERSAL.TRANSACCION_ENRIQUECIDA 
LIMIT 10
""".\
replace("{var:username}", username)
).show()