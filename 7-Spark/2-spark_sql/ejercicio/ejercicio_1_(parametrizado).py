###
 # @section Parametrizando
 ##

#Valor del parámetro
username = "MIUSUARIO"
#XXX =
#YYY = 

#Limpiamos la tabla
spark.sql("""
TRUNCATE TABLE {var:username}_UNIVERSAL.TRANSACCION_ENRIQUECIDA
""".\
replace("{var:username}", username)
)

#Construimos el reporte
df1 = spark.sql("""
SELECT
T.ID_PERSONA AS ID_PERSONA,
P.NOMBRE AS NOMBRE_PERSONA,
P.EDAD AS EDAD_PERSONA,
P.SALARIO AS SALARIO_PERSONA,
P.ID_EMPRESA AS ID_EMPRESA_PERSONA,
T.MONTO AS MONTO_TRANSACCION,
T.FECHA AS FECHA_TRANSACCION,
T.ID_EMPRESA AS ID_EMPRESA_TRANSACCION
FROM
{var:username}_UNIVERSAL.TRANSACCION T
JOIN {var:username}_UNIVERSAL.PERSONA P
ON T.ID_PERSONA = P.ID
""".\
replace("{var:username}", username) # .\
#replace("{var:XXX}, XXX") .\   <------- De esta forma agregamos más parámetros
#replace("{var:YYY}, YYY")      <------- De esta forma agregamos más parámetros
)   

df1.show()
df1.createOrReplaceTempView("df1")

df2 = spark.sql("""
SELECT
T.ID_PERSONA AS ID_PERSONA,
T.NOMBRE_PERSONA AS NOMBRE_PERSONA,
T.EDAD_PERSONA AS EDAD_PERSONA,
T.SALARIO_PERSONA AS SALARIO_PERSONA,
E.NOMBRE AS TRABAJO_PERSONA,
T.MONTO_TRANSACCION AS MONTO_TRANSACCION,
T.FECHA_TRANSACCION AS FECHA_TRANSACCION,
T.ID_EMPRESA_TRANSACCION AS ID_EMPRESA_TRANSACCION
FROM
df1 T
JOIN {var:username}_UNIVERSAL.EMPRESA E
ON T.ID_EMPRESA_PERSONA = E.ID
""".\
replace("{var:username}", username)
)

df2.show()
df2.createOrReplaceTempView("df2")

df3 = spark.sql("""
SELECT
T.ID_PERSONA AS ID_PERSONA,
T.NOMBRE_PERSONA AS NOMBRE_PERSONA,
T.EDAD_PERSONA AS EDAD_PERSONA,
T.SALARIO_PERSONA AS SALARIO_PERSONA,
T.TRABAJO_PERSONA AS TRABAJO_PERSONA,
T.MONTO_TRANSACCION AS MONTO_TRANSACCION,
T.FECHA_TRANSACCION AS FECHA_TRANSACCION,
E.NOMBRE AS EMPRESA_TRANSACCION
FROM
df2 T
JOIN {var:username}_UNIVERSAL.EMPRESA E
ON T.ID_EMPRESA_TRANSACCION = E.ID
""".\
replace("{var:username}", username)
)

df3.show()
df3.createOrReplaceTempView("df3")

# Bajada de la resultante "df3" a la tabla "MIUSUARIO_UNIVERSAL.TRANSACCION_ENRIQUECIDA"

spark.sql("SET hive.exec.dynamic.partition=true")            # Activamos el particionamiento dinámico
spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")  # Activamos el particionamiento dinámico

spark.sql("""
INSERT OVERWRITE TABLE {var:username}_UNIVERSAL.TRANSACCION_ENRIQUECIDA
PARTITION (FECHA_TRANSACCION)
SELECT
ID_PERSONA,
NOMBRE_PERSONA,
EDAD_PERSONA,
SALARIO_PERSONA,
TRABAJO_PERSONA,
MONTO_TRANSACCION,
EMPRESA_TRANSACCION,
FECHA_TRANSACCION
FROM
df3
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