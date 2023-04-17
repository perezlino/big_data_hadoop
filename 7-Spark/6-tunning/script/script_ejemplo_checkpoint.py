# -*- coding: utf-8 -*-    <----- Es propio de Python, nos permite usar tildes y caracteres especiales

###
 # @section Librerías
 ##

import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import udf, struct
from pyspark.sql import SparkSession
import sys    # <---------- La libreria 'sys' de Python nos permite recibir parametros desde Consola

###
 # @section Parámetros
 ##

PRM_USERNAME = sys.argv[1]

###
 # @section Tuning
 ##

spark = SparkSession.builder.\
appName("Nombre de mi aplicacion").\
config("spark.master", "local[1]").\
config("spark.dynamicAllocation.maxExecutors", "10").\
config("spark.executor.cores", "2").\
config("spark.executor.memory", "500m").\
config("spark.executor.memoryOverhead", "500m").\
config("spark.driver.memory", "500m").\
config("spark.driver.memoryOverhead", "500m").\
enableHiveSupport().\
getOrCreate()

###
 # @section Funciones
 ##

###
 # Calcula el salario mensual de una persona
 #
 # @param salarioMensual {double} Salario mensual de la persona
 # @return {double}
 ##
def salarioAnual(salarioMensual):
	#Calculo del salario anual
	salarioAnual = salarioMensual*12
	#
	#Devolvemos el valor
	return salarioAnual

###
 # Calcula el salario multiplicado por la edad
 #
 # @param salario {double} Salario mensual de la persona
 # @param edad {double} Edad de la persona
 # @return {double}
 ##
def salarioPorEdad(salario, edad):
	#Calculo del salario anual
	respuesta = salario*edad
	#
	#Devolvemos el valor
	return respuesta

###
 # @section UDF
 ##

udfSalarioAnual = udf(
	salarioAnual, 
	DoubleType()
)

udfSalarioPorEdad = udf(
	(lambda parametros : salarioPorEdad(parametros[0], parametros[1])),
	DoubleType()
)

###
 # @section Programa
 ##

def main():
	###
	 # @section Lectura de datos
	 ##
	#
	dfPersona = spark.sql("""
		SELECT 
			* 
		FROM 
			{var:PRM_USERNAME}_UNIVERSAL.PERSONA
	""".\
	replace("{var:PRM_USERNAME}", PRM_USERNAME)
	)
	#
	dfPersona.show()
	#
	dfEmpresa = spark.sql("""
		SELECT 
			* 
		FROM 
			{var:PRM_USERNAME}_UNIVERSAL.EMPRESA
	""".\
	replace("{var:PRM_USERNAME}", PRM_USERNAME)
	)
	#
	dfEmpresa.show()
	#
	dfTransaccion = spark.sql("""
		SELECT 
			* 
		FROM 
			{var:PRM_USERNAME}_UNIVERSAL.TRANSACCION
	""".\
	replace("{var:PRM_USERNAME}", PRM_USERNAME)
	)
	#
	dfTransaccion.show()
	#
	###
	 # @section Procesamiento
	 ##
	#
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
	#
	dfPersonaEnriquecida.show()
	#
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
	#
	dfPersonaEnriquecidaTransaccion.show()
	
	# ------------------ Ejemplo Checkpoint ------------------	
	'''
	Luego de que tengas una serie de pasos acumulados en un Dataframe, para evitar el colapso de memoria RAM, 
	lo que tenemos que hacer es, el Dataframe intermedio, que va a ser el input para el siguiente Dataframe 
	intermedio para seguir procesando, digamos que luego de 10 pasos, lo bajamos a disco duro. Cuando tú haces 
	una bajada al disco duro de un Dataframe hay garantía de que la RAM se va a liberar del clúster para todos 
	los Dataframes intermedios que se usaron para crear este Dataframe. 
	'''

	dfPersonaEnriquecidaTransaccion.write.\
	mode('overwrite').\
	format('parquet').\
	option('compression','snappy').\
	save('/user/main/db/TABLA_JOIN/TABLA_JOIN')

	'''
	Lo guardaremos en alguna ruta HDFS, inmediatamente después de eso, si quieres en la misma variable vuelves a 
	cargarlo desde HDFS, esto puede parecer un paso algo trivial, pero no lo es, por que lo que hacemos es una bajada 
	a disco duro y por lo tanto liberando memoria RAM de todo lo que hayamos procesando anteriormente. Y luego seguir 
	procesando normalmente. 
	'''
	dfPersonaEnriquecidaTransaccion = spark.read.\
	format('parquet').\
	load('/user/main/tmp/dfJoin')

	'''
	Aquí ya no hay una recomendación general, ¿cada cuánto se tiene que poner este patrón, cada 10 pasos, cada 100 pasos? 
	no se puede saber, porque va depender de qué tan complejos sean los pasos anteriores y qué tanta data manipulan cada 
	uno estos pasos intermedios, así que no hay una forma de cómo utilizar un CHECKPOINT dentro de un Arquetipo, solo saber 
	que cada cierta cantidad de pasos podemos hacer una bajada a Disco duro. Lo que si podemos hacer es que al momento de 
	ir probando nuestro código, si tienes garantía, en Jupyter por ejemplo, de que hasta el paso 10 ha estado ejecutándose 
	bien, ejecutas, creas un nuevo paso 11 y ese ya no se ejecuta tan bien, en vivo ahí puedes ir averiguando dónde puede 
	ir un CHECKPOINT. Ahí por ejemplo si de pronto ya se empieza a poner lento el código, ahí hay que colocar un CHECKPOINT. 
	Y luego sigues avanzando y vas programando y de pronto se empieza a poner lento nuevamente tu programa, ahí ese es otro 
	CHECKPOINT. 
	'''

	# --------------------------------------------------------

	#
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
	#
	dfReporte1.show()
	#
	#Guardamos el dataframe de reporte en la tabla Hive
	dfReporte1.createOrReplaceTempView("dfReporte1")
	#
	#Activamos el particionamiento dinamico
	spark.sql("SET hive.exec.dynamic.partition=true")
	spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
	#
	#
	#Almacenamos el resultado en Hive
	spark.sql("""
		INSERT INTO {var:PRM_USERNAME}_UNIVERSAL.TRANSACCION_ENRIQUECIDA
			SELECT
				*
			FROM 
				dfReporte1 T
	""".\
	replace("{var:PRM_USERNAME}", PRM_USERNAME)
	)
	#
	#Verificamos que el reporte se haya insertado correctamente
	spark.sql("""
		SELECT 
			* 
		FROM 
			{var:PRM_USERNAME}_UNIVERSAL.TRANSACCION_ENRIQUECIDA 
		LIMIT 10
	""".\
	replace("{var:PRM_USERNAME}", PRM_USERNAME)
	).show()

main()