from pyspark.sql.types import *
from pyspark.sql.functions import udf, struct

username = "miusuario"

#Funcion para multiplicar el salario por la edad
def salarioPorEdad(parametros):
	#Capturamos los parámetros
	salario = parametros[0]
	edad = parametros[1]
	#
	#Calculo del salario anual
	respuesta = salario*edad
	#
	#Devolvemos el valor
	return respuesta

#Creamos la función personalizada
#Primer parámetro la función
#Segundo parámetro el tipo de dato que devuelve la función
udfsalarioPorEdad = udf(salarioPorEdad, DoubleType())

#Leemos los datos
dfPersona = spark.sql("""
SELECT 
* 
FROM 
{var:username}_UNIVERSAL.PERSONA
""".\
replace("{var:username}", username)
)

dfPersona.show()

#Aplicamos la función
df2 = dfPersona.select(
	dfPersona["NOMBRE"].alias("NOMBRE"),
	udfsalarioPorEdad(
		struct(
			dfPersona["SALARIO"], 
			dfPersona["EDAD"]
		)
	).alias("SALARIO_POR_EDAD")
)

df2.show()

#También lo podemos escribir in-line
df3 = dfPersona.select(
dfPersona["NOMBRE"].alias("NOMBRE"),
udfsalarioPorEdad(struct(dfPersona["SALARIO"], dfPersona["EDAD"])).alias("SALARIO_POR_EDAD")
)

df3.show()