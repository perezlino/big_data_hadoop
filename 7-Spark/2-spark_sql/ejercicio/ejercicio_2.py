#Valor del parámetro
username = "MIUSUARIO"

#Reporte 1

edad = 50

dfReporte1 = spark.sql("""
SELECT 
T.* 
FROM 
{var:username}_UNIVERSAL.TRANSACCION_ENRIQUECIDA T
WHERE 
T.EDAD_PERSONA > {var:edad}
""".\
replace("{var:edad}", str(edad)).\
replace("{var:username}", username)
)

dfReporte1.show()

# -------------------------------------------------------------------------------------------

#Reporte 2

inicial = "A"
edad = 25

dfReporte2 = spark.sql("""
SELECT 
T.* 
FROM 
{var:username}_UNIVERSAL.TRANSACCION_ENRIQUECIDA T
WHERE
T.EDAD_PERSONA > {var:edad} AND
UPPER(SUBSTRING(T.NOMBRE_PERSONA, 0, 1)) = UPPER('{var:inicial}')
""".\
# "UPPER('{var:inicial}')" El UPPER lo utilizamos para asegurarnos de que sea Mayuscula
# dado que el parametro quizas alguien lo coloque como inicial = "a".
replace("{var:inicial}", str(inicial)).\
replace("{var:username}", username)
)

dfReporte2.show()

# -------------------------------------------------------------------------------------------

#Reporte 3

porcentaje = 0.25

dfReporte3 = spark.sql("""
SELECT 
T.* 
FROM 
{var:username}_UNIVERSAL.TRANSACCION_ENRIQUECIDA T
WHERE 
T.MONTO_TRANSACCION >= T.SALARIO_PERSONA*{var:porcentaje}
""".\
replace("{var:porcentaje}", str(porcentaje)).\
replace("{var:username}", username)
)

dfReporte3.show()

# -------------------------------------------------------------------------------------------

#Reporte 4

dfReporte4 = spark.sql("""
SELECT 
T.* 
FROM 
{var:username}_UNIVERSAL.TRANSACCION_ENRIQUECIDA T
WHERE 
T.TRABAJO_PERSONA = T.EMPRESA_TRANSACCION
""".\
replace("{var:username}", username)
)

dfReporte4.show()

# -------------------------------------------------------------------------------------------