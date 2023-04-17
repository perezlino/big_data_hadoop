from pyspark.sql.types import *
from pyspark.sql.functions import udf
import pyspark.sql.functions as f

username = "main"

#Funcion para calcular el salario anual
def salarioAnual(salarioMensual):
	#Calculo del salario anual
	salarioAnual = salarioMensual*12
	#
	#Devolvemos el valor
	return salarioAnual

#Creamos la función personalizada
#Primer parámetro la función
#Segundo parámetro el tipo de dato que devuelve la función
udfSalarioAnual = udf(salarioAnual, DoubleType())

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
#Forma recomendada
#El estándar recomienda utilizar "select" porque asi tenemos controlado que columnas son las 
#que queremos ir llevando en nuestro procesamiento
df2 = dfPersona.select(
dfPersona["NOMBRE"].alias("NOMBRE"),
dfPersona["SALARIO"].alias("SALARIO_MENSUAL"),
udfSalarioAnual(dfPersona["SALARIO"]).alias("SALARIO_ANUAL")
)

df2.show()

#Otra forma de aplicar la funcion
#No se recomienda esta forma porque hace un uso excesivo de memoria RAM
#¿Cuando utilizar "withColumn"? Solamente si en la cadena de procesamientos vas a necesitar
#todas las columnas del Dataframe al cual le aplicamos el 'withColumn'
df3 = dfPersona.withColumn("SALARIO_ANUAL", udfSalarioAnual(dfPersona["SALARIO"]))
df3.show()

df4 = df3.select(
df3["NOMBRE"],
df3["SALARIO"].alias("SALARIO_MENSUAL"),
df3["SALARIO_ANUAL"]
)
df4.show()

#¿Y si necesitamos aplicar la función a una constante?
df3 = dfPersona.select(
dfPersona["NOMBRE"].alias("NOMBRE"),
dfPersona["SALARIO"].alias("SALARIO").alias("SALARIO_MENSUAL"),
udfSalarioAnual(f.lit(1000.0)).alias("SALARIO_ANUAL")
)

df3.show()