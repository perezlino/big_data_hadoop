#Importamos la librería de funciones
import pyspark.sql.functions as f

###
 # @section Parámetros
 ##

username = "MIUSUARIO"
edad = 50
inicial = "A"
porcentaje = 0.25

###
 # @section Lectura de datos
 ##

dfTransaccionEnriquecida = spark.sql("""
SELECT 
*
FROM 
{var:username}_UNIVERSAL.TRANSACCION_ENRIQUECIDA
""".\
replace("{var:username}", username)
)

dfTransaccionEnriquecida.show()

###
 # @section Procesamiento
 ##

#Reporte 1
dfReporte1 = dfTransaccionEnriquecida.filter(dfTransaccionEnriquecida["EDAD_PERSONA"] > edad)
dfReporte1.show()

#Reporte 2
dfReporte2 = dfTransaccionEnriquecida.filter(f.upper(f.substring(dfTransaccionEnriquecida["NOMBRE_PERSONA"], 0, 1)) == f.upper(f.lit(inicial)))
dfReporte2.show()

#Reporte 3
dfReporte3 = dfTransaccionEnriquecida.filter(dfTransaccionEnriquecida["MONTO_TRANSACCION"] >= dfTransaccionEnriquecida["SALARIO_PERSONA"]*porcentaje)
dfReporte3.show()

#Reporte 4
dfReporte4 = dfTransaccionEnriquecida.filter(dfTransaccionEnriquecida["TRABAJO_PERSONA"] == dfTransaccionEnriquecida["EMPRESA_TRANSACCION"])
dfReporte4.show()