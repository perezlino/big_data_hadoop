GENERACION DE REPORTERIA
========================

Esto corresponde a la capa "SMART".


TRANSACCION                                 TRANSACCION_ENRIQUECIDA                                                            TRANSACCION_POR_EDAD
 ________                                              ________    ID_PERSONA                                 ___________            ________     EDAD_PERSONA     
|__|__|__| ID_PERSONA                                 |__|__|__|   NOMBRE_PERSONA                            |           |          |__|__|__|    CANTIDAD_TRANSACCIONES
|__|__|__| ID_EMPRESA ----.                .--------> |__|__|__|   EDAD_PERSONA                    .-------> | PROCESO 2 | -------> |__|__|__|    SUMA_MONTO_TRANSACCIONES
|__|__|__| MONTO          |                |          |__|__|__|   SALARIO_PERSONA                 |         |___________|          |__|__|__|
           FECHA          |                |                       TRABAJO_PERSONA  ---------------|
PERSONA                   |                |                       MONTO_TRANSACCION               | 
 ________  ID             |           _____|_____                  FECHA_TRANSACCION               |                           TRANSACCION_POR_TRABAJO
|__|__|__| NOMBRE         |          |           |                 EMPRESA_TRANSACCION             |          ___________            ________     TRABAJO_PERSONA
|__|__|__| TELEFONO ------|--------> | PROCESO 1 |                                                 |         |           |          |__|__|__|    CANTIDAD_TRANSACCIONES
|__|__|__| CORREO         |          |___________|                                                 |-------> | PROCESO 3 | -------> |__|__|__|    SUMA_MONTO_TRANSACCIONES
           FECHA_INGRESO  |                                                                        |         |___________|          |__|__|__|
           EDAD           |                                                                        | 
           SALARIO        |                                                                        | 
           ID_EMPRESA     |                                                                        |                                                                           
EMPRESA                   |                                                                        |                           TRANSACCION_POR_EMPRESA      
 ________                 |                                                                        |          ___________            ________    EMPRESA_TRANSACCION
|__|__|__| ID             |                                                                        |         |           |          |__|__|__|   CANTIDAD_TRANSACCIONES
|__|__|__| NOMBRE --------'                                                                        '-------> | PROCESO 4 | -------> |__|__|__|   SUMA_MONTO_TRANSACCIONES
|__|__|__|                                                                                                   |___________|          |__|__|__|


----------------------------------------------------------------------------------------------------------------

DESAROLLO
=========

----------------
Crear las tablas
----------------

DROP TABLE IF EXISTS MIUSUARIO_SMART.TRANSACCION_POR_EDAD;
CREATE TABLE MIUSUARIO_SMART.TRANSACCION_POR_EDAD(
EDAD_PERSONA INT,
CANTIDAD_TRANSACCIONES INT,
SUMA_MONTO_TRANSACCIONES DOUBLE
)
STORED AS PARQUET
LOCATION '/user/miusuario/ejercicio2/database/miusuario_smart/transaccion_por_edad'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

DROP TABLE IF EXISTS MIUSUARIO_SMART.TRANSACCION_POR_TRABAJO;
CREATE TABLE MIUSUARIO_SMART.TRANSACCION_POR_TRABAJO(
TRABAJO_PERSONA STRING,
CANTIDAD_TRANSACCIONES INT,
SUMA_MONTO_TRANSACCIONES DOUBLE
)
STORED AS PARQUET
LOCATION '/user/miusuario/ejercicio2/database/miusuario_smart/transaccion_por_trabajo'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

DROP TABLE IF EXISTS MIUSUARIO_SMART.TRANSACCION_POR_EMPRESA;
CREATE TABLE MIUSUARIO_SMART.TRANSACCION_POR_EMPRESA(
EMPRESA_TRANSACCION STRING,
CANTIDAD_TRANSACCIONES INT,
SUMA_MONTO_TRANSACCIONES DOUBLE
)
STORED AS PARQUET
LOCATION '/user/miusuario/ejercicio2/database/miusuario_smart/transaccion_por_empresa'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

--------------------------------------------
EJECUCION DE SCRIPTS DE SOLUCION POR CONSOLA
--------------------------------------------

PROCESO 2:
         ________________________________________________________________________________________________
        |                                                                                                |         
        |   beeline -u jdbc:hive2:// -f /home/main/proceso_2.sql --hiveconf "PARAM_USERNAME=MIUSUARIO"   |
        |________________________________________________________________________________________________|

PROCESO 3:
         ________________________________________________________________________________________________
        |                                                                                                |         
        |   beeline -u jdbc:hive2:// -f /home/main/proceso_3.sql --hiveconf "PARAM_USERNAME=MIUSUARIO"   |
        |________________________________________________________________________________________________|    

PROCESO 4:
         ________________________________________________________________________________________________
        |                                                                                                |         
        |   beeline -u jdbc:hive2:// -f /home/main/proceso_4.sql --hiveconf "PARAM_USERNAME=MIUSUARIO"   |
        |________________________________________________________________________________________________|    