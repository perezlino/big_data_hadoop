IMPLEMENTACION DE UN DATALAKE
=============================
                               ___________________________________________________________________________________________________________________________________________________________
                              |                             |                             |                                                                   |                           |  
                              |    miusuario_landing_tmp    |      miusuario_landing      |                      miusuario_universal                          |      miusuario_smart      |   
                              |_____________________________|_____________________________|___________________________________________________________________|___________________________|  
 _____________________________|_____________________________|_____________________________|______________________________                                     |                           |  
|                             |          TEXTFILE           |             AVRO            |           PARQUET            |                                    |                           | 
|     _____                   |                ________     |                ________     |                ________      |                                    |                           |    
|    |-----|                  |       'put'   |__|__|__|    |               |__|__|__|    |               |__|__|__|     |                                    |                           |   
|    |-----|--------------->  |    ○ -------> |__|__|__| ------> ○ -------> |__|__|__| ------> ○ -------> |__|__|__|--.  |                                    |                           |   
|    |_____|                  |   HDFS        |__|__|__|    |   HIVE        |__|__|__|    |   HIVE        |__|__|__|  |  |            IMPLEMENTAR             |                           | 
|  persona.data               |  Getway          HIVE       | MapReduce        HIVE       | MapReduce        HIVE     |  |                                    |                           |  
|                             |                persona      |                persona      |                persona    |  |                                    |                           |      
|_____________________________|_____________________________|_____________________________|___________________________|__|                                    |                           | 
                              |                             |                             |                           |                                       |                           |                                          
 _____________________________|_____________________________|_____________________________|___________________________|__                                     |                           | 
|     _____                   |                ________     |                ________     |                ________   |  |                   ________         |                           |   
|    |-----|                  |       'put'   |__|__|__|    |               |__|__|__|    |               |__|__|__|  |  |                  |__|__|__|        |                           |   
|    |-----|--------------->  |    ○ -------> |__|__|__| ------> ○ -------> |__|__|__| ------> ○ -------> |__|__|__|---------->  ○ -------> |__|__|__|        |                           |            
|    |_____|                  |   HDFS        |__|__|__|    |   HIVE        |__|__|__|    |   HIVE        |__|__|__|  |  |      HIVE        |__|__|__|        |                           |   
|  empresa.data               |  Getway          HIVE       | MapReduce        HIVE       | MapReduce        HIVE     |  |      SPARK          HIVE           |                           |   
|                             |                empresa      |                empresa      |                empresa    |  |            transaccion_enriquecida |                           | 
|                             |                             |                             |                           |  |                                    |                           |   
|  _____  _____  _____        |                ________     |                ________     |                ________   |  |                                    |                           |    
| |-----||-----||-----|       |       'put'   |__|__|__|    |               |__|__|__|    |               |__|__|__|  |  |                                    |                           |   
| |-----||-----||-----| --->  |    ○ -------> |__|__|__| ------> ○ -------> |__|__|__| ------> ○ -------> |__|__|__|--'  |                                    |                           |         
| |_____||_____||_____|       |   HDFS        |__|__|__|    |   HIVE        |__|__|__|    |   HIVE        |__|__|__|     |                                    |                           |  
|                             |  Getway          HIVE       | MapReduce        HIVE       | MapReduce        HIVE        |                                    |                           |  
| transacciones-2018-01-21    |               transaccion   |                transaccion  |                transaccion   |                                    |                           | 
| transacciones-2018-01-22    |                             |                             |                              |                                    |                           |              
| transacciones-2018-01-23    |                             |                             |                              |                                    |                           |    
|_____________________________|_____________________________|_____________________________|______________________________|                                    |                           |
                              |                             |                             |                                                                   |                           |        
                              |_____________________________|_____________________________|___________________________________________________________________|___________________________|


----------------------------------------------------------------------------------------------------------------

DESAROLLO
=========

----------------------------------------------
1.- Crear la estructura de carpetas sobre HDFS
----------------------------------------------

#Crear la estructura de carpetas
hdfs dfs -mkdir /user/miusuario/ejercicio2
hdfs dfs -mkdir /user/miusuario/ejercicio2/database

hdfs dfs -mkdir /user/miusuario/ejercicio2/database/miusuario_landing_tmp
hdfs dfs -mkdir /user/miusuario/ejercicio2/database/miusuario_landing_tmp/persona

hdfs dfs -mkdir /user/miusuario/ejercicio2/database/miusuario_landing
hdfs dfs -mkdir /user/miusuario/ejercicio2/database/miusuario_landing/persona

hdfs dfs -mkdir /user/miusuario/ejercicio2/database/miusuario_universal
hdfs dfs -mkdir /user/miusuario/ejercicio2/database/miusuario_universal/persona

hdfs dfs -mkdir /user/miusuario/ejercicio2/database/miusuario_smart

hdfs dfs -mkdir /user/miusuario/ejercicio2/schema
hdfs dfs -mkdir /user/miusuario/ejercicio2/schema/database

hdfs dfs -mkdir /user/miusuario/ejercicio2/schema/database/miusuario_landing

---------------------------------------------------
Tambien se podria optimizar la creacion de carpetas ===> Desarrollo Clase
---------------------------------------------------

hdfs dfs -mkdir -p \
/user/miusuario/ejercicio2/database \
/user/miusuario/ejercicio2/database/miusuario_landing_tmp/persona \
/user/miusuario/ejercicio2/database/miusuario_landing_tmp/empresa \
/user/miusuario/ejercicio2/database/miusuario_landing_tmp/transaccion 

hdfs dfs -mkdir -p \
/user/miusuario/ejercicio2/database \
/user/miusuario/ejercicio2/database/miusuario_landing/persona \
/user/miusuario/ejercicio2/database/miusuario_landing/empresa \
/user/miusuario/ejercicio2/database/miusuario_landing/transaccion

hdfs dfs -mkdir -p \
/user/miusuario/ejercicio2/database \
/user/miusuario/ejercicio2/database/miusuario_universal/persona \
/user/miusuario/ejercicio2/database/miusuario_universal/empresa \
/user/miusuario/ejercicio2/database/miusuario_universal/transaccion \
/user/miusuario/ejercicio2/database/miusuario_universal/transaccion_enriquecida

hdfs dfs -mkdir -p \
/user/miusuario/ejercicio2/database \
/user/miusuario/ejercicio2/database/miusuario_smart/transaccion_por_edad \
/user/miusuario/ejercicio2/database/miusuario_smart/transaccion_por_trabajo \
/user/miusuario/ejercicio2/database/miusuario_smart/transaccion_por_empresa 

hdfs dfs -mkdir /user/miusuario/ejercicio2/schema/miusuario_landing

-----------------------------------------------------------------------------------
Realizar la carga a HDFS (Los archivos ya estan cargados previamente en el Gateway) ==> Desarrollo Clase
-----------------------------------------------------------------------------------

hdfs dfs -put
/home/main/persona.avsc \
/home/main/empresa.avsc \
/home/main/transaccion.avsc \
/user/miusuario/ejercicio2/schema/miusuario_landing

----------------------------------------------------------------------------------------------------------------
----------------------------
2.- Crear las bases de datos
----------------------------

-- En la consola de HIVE, adaptar y ejecutar:
CREATE DATABASE IF NOT EXISTS MIUSUARIO_LANDING_TMP LOCATION '/user/miusuario/ejercicio2/database/miusuario_landing_tmp';
CREATE DATABASE IF NOT EXISTS MIUSUARIO_LANDING LOCATION '/user/miusuario/ejercicio2/database/miusuario_landing';
CREATE DATABASE IF NOT EXISTS MIUSUARIO_UNIVERSAL LOCATION '/user/miusuario/ejercicio2/database/miusuario_universal';
CREATE DATABASE IF NOT EXISTS MIUSUARIO_SMART LOCATION '/user/miusuario/ejercicio2/database/miusuario_smart';

----------------------------------------------------------------------------------------------------------------

-----------------------------
3.- Crear la capa LANDING_TMP ==> Desarrollo Clase
-----------------------------

                                              ------------
                                                 PERSONA    
                                              ------------

-- En la consola de HIVE, adaptar y ejecutar
CREATE TABLE MIUSUARIO_LANDING_TMP.PERSONA(
ID STRING,
NOMBRE STRING,
TELEFONO STRING,
CORREO STRING,
FECHA_INGRESO STRING,
EDAD STRING,
SALARIO STRING,
ID_EMPRESA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/miusuario/ejercicio2/database/miusuario_landing_tmp/persona';

-- Subida de datos
-- En la consola HADOOP, adaptar y ejecutar
hdfs dfs -put /dataset/persona.data /user/miusuario/database/miusuario_landing_tmp/persona

-- O desde HIVE utilizando (es lo mismo)
LOAD DATA LOCAL INPATH '/dataset/persona.data'
INTO TABLE MIUSUARIO_LANDING_TMP.PERSONA;

-- En la consola HADOOP o en HIVE, verificar que existan los datos en la tabla
SELECT * FROM MIUSUARIO_LANDING_TMP.PERSONA LIMIT 10;

-- (Se cargara el "ENCABEZADO" como un registro más, eso no es correcto, sin embargo, este tipo de  errores se CORRIGEN EN LA ETAPA "UNIVERSAL)

-- Repetir los pasos anteriores para las tablas "EMPRESA" y "TRANSACCION"


                                              ------------
                                                 EMPRESA    
                                              ------------

-- En la consola de HIVE, adaptar y ejecutar
CREATE TABLE MIUSUARIO_LANDING_TMP.EMPRESA(
ID STRING,
NOMBRE STRING,
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/miusuario/ejercicio2/database/miusuario_landing_tmp/empresa';

-- Subida de datos
-- En la consola HADOOP, adaptar y ejecutar
hdfs dfs -put /dataset/empresa.data /user/miusuario/database/miusuario_landing_tmp/empresa

-- O desde HIVE utilizando (es lo mismo)
LOAD DATA LOCAL INPATH '/dataset/empresa.data'
INTO TABLE MIUSUARIO_LANDING_TMP.EMPRESA;


                                            ----------------
                                               TRANSACCION    
                                            ----------------

-- En la consola de HIVE, adaptar y ejecutar
CREATE TABLE MIUSUARIO_LANDING_TMP.TRANSACCION(
ID_PERSONA STRING,
ID_EMPRESA STRING,
MONTO STRING,
FECHA STRING, <------------------------------------- Agrego este campo, siendo que no existe en el archivo
)                                                    de definicion de esquema ???
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/miusuario/ejercicio2/database/miusuario_landing_tmp/transaccion';

-- Subida de datos
-- En la consola HADOOP, adaptar y ejecutar
hdfs dfs -put /dataset/transacciones.data /user/miusuario/database/miusuario_landing_tmp/transaccion

-- O desde HIVE utilizando (es lo mismo)
LOAD DATA LOCAL INPATH '/dataset/transacciones.data'
INTO TABLE MIUSUARIO_LANDING_TMP.TRANSACCION;

----------------------------------------------------------------------------------------------------------------

-------------------------
4.- Crear la capa LANDING 
-------------------------
                                              ------------
                                                 PERSONA    
                                              ------------

------------------------------------------------
CREAR Y CARGAR ARCHIVOS DE DEFINICION DE ESQUEMA
------------------------------------------------

#En tu laptop, crear un archivo llamado "persona.txt" <----------------- Estos archivos de definicion de esquema
                                                                         ya los subimos en la primera etapa !!!!
#Al archivo "persona.txt" colocarle el siguiente contenido:              Sin embargo, tambien podrian seguirse en  
{                                                                        esta etapa. 
  "name": "PERSONA",
  "type": "record",
  "fields": [
    {"name": "ID", "type": ["string", "null"]},
    {"name": "NOMBRE", "type": ["string", "null"]},
    {"name": "TELEFONO", "type": ["string", "null"]},
    {"name": "CORREO", "type": ["string", "null"]},
    {"name": "FECHA_INGRESO", "type": ["string", "null"]},
    {"name": "EDAD", "type": ["string", "null"]},
    {"name": "SALARIO", "type": ["string", "null"]},
    {"name": "ID_EMPRESA", "type": ["string", "null"]}
  ]
}

#Desde MobaXterm, subir el archivo "persona.txt" a tu ruta home de LINUX (/home/miusuario)

#Desde la consola HDFS, subir el archivo "persona.txt"
hdfs dfs -put /home/main/persona.txt /user/miusuario/ejercicio2/schema/database/miusuario_landing


------------
CREAR TABLAS
------------

-- Desde HIVE, creamos la tabla PERSONA 
CREATE TABLE MIUSUARIO_LANDING.PERSONA
STORED AS AVRO
LOCATION '/user/miusuario/ejercicio2/database/miusuario_landing/persona'
TBLPROPERTIES (
'avro.schema.url'='hdfs:///user/miusuario/ejercicio2/schema/database/miusuario_landing/persona.avsc',
'avro.output.codec'='snappy'
);

------------------
ACTIVAR COMPRESION
------------------

-- Desde HIVE, activamos la compresión y el formato SNAPPY
SET hive.exec.compress.output=true;
SET avro.output.codec=snappy;

------------------------------
INSERCION DE DATOS EN LA TABLA
------------------------------

-- Desde HIVE, ejecutamos la inserción de datos desde la tabla "LANDING_TMP" a la tabla "LANDING"
INSERT OVERWRITE TABLE MIUSUARIO_LANDING.PERSONA
SELECT * FROM  MIUSUARIO_LANDING_TMP.PERSONA;

-- Desde HIVE, verificamos que se hayan insertado los datos en la tabla "LANDING"
SELECT * FROM MIUSUARIO_LANDING_TMP.PERSONA LIMIT 10;


                                              ------------
                                                 EMPRESA    
                                              ------------




                                            ----------------
                                               TRANSACCION    
                                            ----------------

------------
CREAR TABLAS
------------

Queremos crear una tabla particionada. Las particiones NO SON FLEXIBLES aunque esten en un AVRO, son
ESTATICAS. Porque a nivel de HDFS, una particion es un Subdirectorio de HDFS, asi que es algo estatico.

-- Desde HIVE, creamos la tabla TRANSACCION 
CREATE TABLE MIUSUARIO_LANDING.TRANSACCION
PARTITIONED BY (FECHA STRING)
STORED AS AVRO
LOCATION '/user/miusuario/ejercicio2/database/miusuario_landing/transaccion'
TBLPROPERTIES (
'avro.schema.url'='hdfs:///user/miusuario/ejercicio2/schema/database/miusuario_landing/transaccion.avsc',
'avro.output.codec'='snappy'
);

------------------
ACTIVAR COMPRESION
------------------

-- Desde HIVE, activamos la compresión y el formato SNAPPY
SET hive.exec.compress.output=true;
SET avro.output.codec=snappy;

---------------------------------
ACTIVAR PARTICIONAMIENTO DINAMICO
---------------------------------

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

------------------------------
INSERCION DE DATOS EN LA TABLA
------------------------------

-- Desde HIVE, ejecutamos la inserción de datos desde la tabla "LANDING_TMP" a la tabla "LANDING"
INSERT OVERWRITE TABLE MIUSUARIO_LANDING.TRANSACCION
PARTITION(FECHA)
SELECT * FROM  MIUSUARIO_LANDING_TMP.TRANSACCION;

-- Desde HIVE, verificamos que se hayan insertado los datos en la tabla "LANDING"
SELECT * FROM MIUSUARIO_LANDING_TMP.PERSONA LIMIT 10;

-- Verificamos
SHOW PARTITIONS MIUSUARIO_LANDING.TRANSACCION;
--   ________________
--  |   partition    |
--  |----------------|
--  |fecha=2018-01-21|
--  |fecha=2018-01-22|
--  |fecha=2018-01-23|
--  |fecha=FECHA     | <---------------- Debemos borrar esta particion erronea, eso lo
--  |________________|                   haremos en la capa UNIVERSAL.

----------------------------------------------------------------------------------------------------------------

---------------------------
5.- Crear la capa UNIVERSAL 
---------------------------

                                              ------------
                                                 PERSONA    
                                              ------------

-- Desde HIVE, creamos la tabla PERSONA (Ahora utilizamos los tipos de datos correctos)
CREATE TABLE MIUSUARIO_UNIVERSAL.PERSONA(
ID STRING,
NOMBRE STRING,
TELEFONO STRING,
CORREO STRING,
FECHA_INGRESO STRING,
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
STORED AS PARQUET
LOCATION '/user/miusuario/ejercicio2/database/miusuario_universal/persona'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

------------------
ACTIVAR COMPRESION
------------------

-- Desde HIVE, activamos la compresión y el formato SNAPPY
SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;

------------------------------
INSERCION DE DATOS EN LA TABLA
------------------------------

-- Desde HIVE, ejecutamos la inserción de datos desde la tabla "LANDING" a la tabla "UNIVERSAL"
INSERT OVERWRITE TABLE MIUSUARIO_UNIVERSAL.PERSONA
SELECT
  cast(ID AS STRING),
  cast(NOMBRE AS STRING),
  cast(TELEFONO AS STRING),
  cast(CORREO AS STRING),
  cast(FECHA_INGRESO AS STRING),
  cast(EDAD AS INT),
  cast(SALARIO AS DOUBLE),
  cast(ID_EMPRESA AS STRING)
FROM 
  MIUSUARIO_LANDING.PERSONA
WHERE 
  ID != 'ID';    <-------------------------- Con esta linea filtramos ese registro que nos mostraba
                                             los encabezados como un registro más.
 EDAD > 0        <-------------------------- Es aqui donde podemos comenzar a restringir los datos
 SALARIO > 1000                              (Estas restricciones son solo para ejemplo, no tomar en cuenta)

-- Desde HIVE, verificamos que se hayan insertado los datos en la tabla "LANDING"
SELECT * FROM MIUSUARIO_UNIVERSAL.PERSONA LIMIT 10;


                                              ------------
                                                 EMPRESA    
                                              ------------

-- Desde HIVE, creamos la tabla EMPRESA (Ahora utilizamos los tipos de datos correctos)
CREATE TABLE MIUSUARIO_UNIVERSAL.EMPRESA(
ID STRING,
NOMBRE STRING,
)
STORED AS PARQUET
LOCATION '/user/miusuario/ejercicio2/database/miusuario_universal/empresa'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

------------------
ACTIVAR COMPRESION
------------------

-- Desde HIVE, activamos la compresión y el formato SNAPPY
SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;

------------------------------
INSERCION DE DATOS EN LA TABLA
------------------------------

-- Desde HIVE, ejecutamos la inserción de datos desde la tabla "LANDING" a la tabla "UNIVERSAL"
INSERT OVERWRITE TABLE MIUSUARIO_UNIVERSAL.EMPRESA
SELECT
  cast(ID AS STRING),
  cast(NOMBRE AS STRING),
FROM 
  MIUSUARIO_LANDING.EMPRESA
WHERE 
  ID != 'ID';    <-------------------------- Con esta linea filtramos ese registro que nos mostraba
                                             los encabezados como un registro más.

-- Desde HIVE, verificamos que se hayan insertado los datos en la tabla "LANDING"
SELECT * FROM MIUSUARIO_UNIVERSAL.EMPRESA LIMIT 10;


                                            ----------------
                                               TRANSACCION    
                                            ----------------

-- Desde HIVE, creamos la tabla TRANSACCION (Ahora utilizamos los tipos de datos correctos)
CREATE TABLE MIUSUARIO_UNIVERSAL.EMPRESA(
ID_PERSONA STRING,
ID_EMPRESA STRING,
MONTO DOUBLE
)
PARTITIONED BY (FECHA STRING)
STORED AS PARQUET
LOCATION '/user/miusuario/ejercicio2/database/miusuario_universal/transaccion'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

------------------
ACTIVAR COMPRESION <---------------------------- Esto no lo hizo en la Clase
------------------

-- Desde HIVE, activamos la compresión y el formato SNAPPY
SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;

---------------------------------
ACTIVAR PARTICIONAMIENTO DINAMICO
---------------------------------

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

------------------------------
INSERCION DE DATOS EN LA TABLA
------------------------------

-- Desde HIVE, ejecutamos la inserción de datos desde la tabla "LANDING" a la tabla "UNIVERSAL"
INSERT OVERWRITE TABLE MIUSUARIO_UNIVERSAL.TRANSACCION
PARTITION(FECHA)
SELECT
  cast(ID_PERSONA AS STRING),
  cast(ID_EMPRESA AS STRING),
  cast(MONTO AS DOUBLE),
  cast(FECHA AS STRING)
FROM 
  MIUSUARIO_LANDING.TRANSACCION
WHERE 
  ID_PERSONA != 'ID_PERSONA';    <-------------------------- Con esta linea filtramos ese registro que nos mostraba
                                             los encabezados como un registro más.

-- Desde HIVE, verificamos que se hayan insertado los datos en la tabla "LANDING"
SELECT * FROM MIUSUARIO_UNIVERSAL.TRANSACCION LIMIT 10;

-- Verificamos
SHOW PARTITIONS MIUSUARIO_UNIVERSAL.TRANSACCION;
--   ________________
--  |   partition    |
--  |----------------|
--  |fecha=2018-01-21|
--  |fecha=2018-01-22|
--  |fecha=2018-01-23|
--  |________________|

                                              ---------------------------
                                                TRANSACCION_ENRIQUECIDA
                                              ---------------------------

-- Desde HIVE, creamos la tabla TRANSACCION_ENRIQUECIDA
CREATE TABLE MIUSUARIO_UNIVERSAL.TRANSACCION_ENRIQUECIDA(
ID_PERSONA INT,
NOMBRE_PERSONA STRING,
EDAD_PERSONA INT,
SALARIO_PERSONA DOUBLE,
TRABAJO_PERSONA STRING,
MONTO_TRANSACCION DOUBLE,
EMPRESA_TRANSACCION STRING
)
PARTITIONED BY (FECHA_TRANSACCION STRING)
STORED AS PARQUET
LOCATION '/user/miusuario/ejercicio2/database/miusuario_universal/transaccion_enriquecida'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

----------------------------------------------------------------------------------------------------------------

----------------------------------------------------------
6.- Crear una tabla desnormalizada sobre la capa UNIVERSAL
----------------------------------------------------------

En la capa "universal" crear la tabla "transaccion_enriquecida" la cual debe ser construida en función de las 
tablas "persona", "empresa" y "transaccion". Los campos de la tabla son los siguientes:

               _____________________________________________________________________________________
              |        CAMPO        |  TIPO  |                    DESCRIPCION                       |
              |---------------------|--------|------------------------------------------------------|
              | ID_PERSONA          | INT    | ID de la persona que realizo la transaccion          |
              |---------------------|--------|------------------------------------------------------|
              | NOMBRE_PERSONA      | STRING | Nombre de la persona que realizó la transaccion      |
              |---------------------|--------|------------------------------------------------------|
              | EDAD_PERSONA        | INT    | Edad de la persona que realizó la transaccion        |
              |---------------------|--------|------------------------------------------------------|
              | SALARIO_PERSONA     | DOUBLE | Salario de la persona que realizó la transaccion     |
              |---------------------|--------|------------------------------------------------------|
              | TRABAJO_PERSONA     | STRING | Nombre de la empresa donde trabaja la persona        |
              |---------------------|--------|------------------------------------------------------|
              | MONTO_TRANSACCION   | DOUBLE | Monto de la transaccion realizada                    |
              |---------------------|--------|------------------------------------------------------|
              | FECHA_TRANSACCION   | STRING | Fecha de la transaccion realizada                    |
              |---------------------|--------|------------------------------------------------------|
              | EMPRESA_TRANSACCION | STRING | Nombre de la empresa donde se realizó la transaccion |
              |_____________________|________|______________________________________________________|


----------------------------------------------
DIAGRAMA INPUT/INTERMEDIATE/OUTPUT [PROCESO 1]
----------------------------------------------

TRANSACCION_PERSONA_ENRIQUECIDA_1         -----.
TRANSACCION_PERSONA_ENRIQUECIDA_2              |------> TABLAS TEMPORALES 
TRANSACCION_PERSONA_EMPRESA_ENRIQUECIDA   -----'    Seran creadas como tablas temporales
                                                    (al cerrar sesion se borran automaticamente)
                                                    porque solo nos sirven para obtener la     
                                                    tabla output "TRANSACCION_ENRIQUECIDA"

                    UNIVERSAL                                                    


TRANSACCION                                 TRANSACCION_PERSONA_ENRIQUECIDA_1
 ________                                              ________    ID_PERSONA
|__|__|__|                                            |__|__|__|   NOMBRE_PERSONA
|__|__|__|--------.                        .--------> |__|__|__|   EDAD_PERSONA
|__|__|__|        |                        |          |__|__|__|   SALARIO_PERSONA
                  |         ______         |               |       ID_EMPRESA_PERSONA ----> Hace referencia al ID de la Empresa donde la Persona trabaja 
PERSONA           |--------| JOIN |--------'               |       MONTO_TRANSACCION
 ________         |        |______|                        |       FECHA_TRANSACCION 
|__|__|__|        |   TRANSACCION.ID_PERSONA               |       ID_EMPRESA_TRANSACCION ----> Hace referencia al ID de la Empresa donde se realizo la transaccion
|__|__|__|--------'             =                          | 
|__|__|__|                  PERSONA.ID                     | 
                                                           |                                                                                                                                                TRANSACCION_ENRIQUECIDA                                                  
EMPRESA                                                    |                         TRANSACCION_PERSONA_ENRIQUECIDA_2                                                                                             ________ 
 ________                                                __˅___                                  ________  ID_PERSONA                                                                       ________              |__|__|__|  
|__|__|__|                                              | JOIN |                                |__|__|__| NOMBRE_PERSONA                                    .---------------------------> | INSERT |-----------> |__|__|__|
|__|__|__|--------------------------------------------->|______|------------------------------> |__|__|__| EDAD_PERSONA                                      |                             |________|             |__|__|__|
|__|__|__|                             TRANSACCION_PERSONA_ENRIQUECIDA_1.ID_EMPRESA             |__|__|__| SALARIO_PERSONA                                   | 
    |                                                       =                                       |      TRABAJO_PERSONA                                   |   
    |                                                   EMPRESA.ID                                  |      MONTO_TRANSACCION                                 | 
    |                                                                                               |      FECHA_TRANSACCION                                 |
    |                                                                                               |      ID_EMPRESA_TRANSACCION       TRANSACCION_PERSONA_EMPRESA_ENRIQUECIDA
    |                                                                                               |                                                    ____|___  ID_PERSONA
    |                                                                                            ___˅__                                                 |__|__|__| NOMBRE_PERSONA
--  '-----------------------------------------------------------------------------------------> | JOIN |----------------------------------------------> |__|__|__| EDAD_PERSONA
                                                                                                |______|                                                |__|__|__| SALARIO_PERSONA
                                                                      TRANSACCION_PERSONA_ENRIQUECIDA_2.ID_EMPRESA_TRANSACCION                                     TRABAJO_PERSONA 
                                                                                                   =                                                               MONTO_TRANSACCION 
                                                                                               EMPRESA.ID                                                          FECHA_TRANSACCION 
                                                                                                                                                                   EMPRESA_TRANSACCION 

----------------------------------------
TRUNCAR LA TABLA TRANSACCION_ENRIQUECIDA
----------------------------------------

TRUNCATE TABLE main_UNIVERSAL.TRANSACCION_ENRIQUECIDA; <----------- Truncamos la tabla para asegurarnos de que
                                                                    este vacia.

--------------------------------------------------------------------------------------
CREACION Y INSERCION DE DATOS PARA LA TABLA TEMPORAL TRANSACCION_PERSONA_ENRIQUECIDA_1
--------------------------------------------------------------------------------------

DROP TABLE IF EXISTS MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_ENRIQUECIDA_1;
CREATE TEMPORARY TABLE MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_ENRIQUECIDA_1(
ID_PERSONA STRING,
NOMBRE_PERSONA STRING,
EDAD_PERSONA INT,                                -- No es necesario COMPRIMIR una Tabla temporal
SALARIO_PERSONA DOUBLE,                          -- Como tampoco indicar el LOCATION
ID_EMPRESA_PERSONA STRING,
MONTO_TRANSACCION DOUBLE,
FECHA_TRANSACCION STRING,
ID_EMPRESA_TRANSACCION STRING
)
STORED AS PARQUET;

INSERT INTO MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_ENRIQUECIDA_1
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
  MIUSUARIO_UNIVERSAL.TRANSACCION T
JOIN 
  MIUSUARIO_UNIVERSAL.PERSONA P
ON 
  T.ID_PERSONA = P.ID;

SELECT * FROM MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_ENRIQUECIDA_1 LIMIT 10;

--------------------------------------------------------------------------------------
CREACION Y INSERCION DE DATOS PARA LA TABLA TEMPORAL TRANSACCION_PERSONA_ENRIQUECIDA_2
--------------------------------------------------------------------------------------

DROP TABLE IF EXISTS MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_ENRIQUECIDA_2;
CREATE TEMPORARY TABLE MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_ENRIQUECIDA_2(
ID_PERSONA STRING,
NOMBRE_PERSONA STRING,
EDAD_PERSONA INT,
SALARIO_PERSONA DOUBLE,
TRABAJO_PERSONA STRING,
MONTO_TRANSACCION DOUBLE,
FECHA_TRANSACCION STRING,
ID_EMPRESA_TRANSACCION STRING
)
STORED AS PARQUET;

INSERT INTO MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_ENRIQUECIDA_2
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
  MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_ENRIQUECIDA_1 T
JOIN 
  MIUSUARIO_UNIVERSAL.EMPRESA E
ON 
  T.ID_EMPRESA_PERSONA = E.ID;

SELECT * FROM MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_ENRIQUECIDA_2 LIMIT 10;

--------------------------------------------------------------------------------------------
CREACION Y INSERCION DE DATOS PARA LA TABLA TEMPORAL TRANSACCION_PERSONA_EMPRESA_ENRIQUECIDA
--------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_EMPRESA_ENRIQUECIDA;
CREATE TEMPORARY TABLE MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_EMPRESA_ENRIQUECIDA(
ID_PERSONA STRING,
NOMBRE_PERSONA STRING,
EDAD_PERSONA INT,
SALARIO_PERSONA DOUBLE,
TRABAJO_PERSONA STRING,
MONTO_TRANSACCION DOUBLE,
FECHA_TRANSACCION STRING,
EMPRESA_TRANSACCION STRING
)
STORED AS PARQUET;

INSERT INTO MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_EMPRESA_ENRIQUECIDA
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
  MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_ENRIQUECIDA_2 T
JOIN 
  MIUSUARIO_UNIVERSAL.EMPRESA E
ON 
  T.ID_EMPRESA_TRANSACCION = E.ID;

SELECT * FROM MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_EMPRESA_ENRIQUECIDA LIMIT 10;

-----------------------------------------------------------
INSERCION FINAL DE DATOS A LA TABLA TRANSACCION_ENRIQUECIDA
-----------------------------------------------------------

ACTIVAR PARTICIONAMIENTO DINAMICO
---------------------------------
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CONFIGURAR NUMERO DE PARTICIONES =======================> Por defecto se generan 100 particiones. Pero en el caso de que
--------------------------------                          estemos cargando data historica y se generen mas particiones
SET hive.exec.max.dynamic.partitions=9999;                devolvera un error. Es por eso, que se escribe este comando
SET hive.exec.max.dynamic.partitions.pernode=9999;        dandole un numero xxx de particiones.

INSERT OVERWRITE TABLE MIUSUARIO_UNIVERSAL.TRANSACCION_ENRIQUECIDA
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
  MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_EMPRESA_ENRIQUECIDA;

SELECT * FROM MIUSUARIO_UNIVERSAL.TRANSACCION_ENRIQUECIDA LIMIT 10;

--------------------------------
ELIMINACION DE TABLAS TEMPORALES
--------------------------------

DROP TABLE IF EXISTS MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_ENRIQUECIDA_1;
DROP TABLE IF EXISTS MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_ENRIQUECIDA_2;
DROP TABLE IF EXISTS MIUSUARIO_UNIVERSAL.TRANSACCION_PERSONA_EMPRESA_ENRIQUECIDA;