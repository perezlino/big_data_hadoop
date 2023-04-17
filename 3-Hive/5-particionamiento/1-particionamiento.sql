PARTICIONAMIENTO
================

-- PARTICIONAMIENTO ESTATICO : Dentro del directorio en HDFS vamos a crear Subdirectorios en funcion
--                             de algun campo, como puede ser el campo 'Fecha'.

---------------------------------------------------------------------------------------------------------------- 

Tablas particionadas sobre HIVE ==> Particionamiento estático
-------------------------------

-- Es común tener procesos que cada cierto día dejen un delta de información
-- Por ejemplo, podemos tener un proceso que cada día escriba las transacciones realizadas por una persona en cierta empresa
-- Cuando tenemos datos que vengan por lotes con una frecuencia predecible (por hora, diario, mensual), entonces estos datos podemos almacenarlos en una tabla particionada
-- Una tabla particionada se crea de la siguiente manera:
CREATE TABLE main_TEST.TRANSACCION(
ID_PERSONA STRING,
ID_EMPRESA STRING,
MONTO DOUBLE
)
PARTITIONED BY (FECHA STRING) <----------- FECHA es el nombre que tendra el Subdirectorio
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/main/bd/main_test/transaccion';

    -- PARTITIONED BY (FECHA STRING) -----> Tambien pudimos haber utilizado (FECHA DATE), pero en HIVE no se recomienda utilizar tipos de datos DATE
    --                                      Hay un formato llamado 'parquet' que para algunas versiones de parquet los tipos de datos DATE no son compatibles.
    --                                      Por otro lado, hay algunas herramientas como IMPALA que trabajan con algunas tablas de HIVE que no soportan campos
    --                                      particionados que tengan un tipo DATE, tambien para ciertas versiones.    

-- Por medio de "PARTITIONED BY" indicamos el campo por el cual se particiona
-- En el ejemplo es por medio de la "FECHA" de la transacción

-- Crearemos una partición y le agregaremos un archivo de datos
LOAD DATA LOCAL INPATH '/dataset/transacciones-2018-01-21.data' 
OVERWRITE INTO TABLE main_TEST.TRANSACCION 
PARTITION (FECHA='2018-01-21');  <------------------- Se creara el Subdirectorio "fecha=2018-01-21" y dentro de el estará nuestro archivo 'transacciones-2018-01-21.data'. 

-- Para indicar en que partición se colocarán los datos usamos "PARTITION"
-- Entre paréntesis, colocamos el nombre del campo particionado, seguido por el valor de la partición

-- Verificamos que haya datos
SELECT * FROM main_TEST.TRANSACCION LIMIT 10;

-- Notemos que a pesar que nuestro archivo tiene 3 campos, en la tabla se refleja como 4 campos, ya que toma el valor de la partición como un campo más

-- Mostramos la particiones existentes
SHOW PARTITIONS main_TEST.TRANSACCION;

-- ¿Cómo se guarda una partición sobre HDFS?
-- Listemos el contenido de la carpeta HDFS de nuestra tabla
hdfs dfs -ls /user/main/bd/main_test/transaccion

-- Notamos que se ha creado una carpeta llamada "fecha=2018-01-21"
-- Listemos el contenido de esta carpeta
hdfs dfs -ls /user/main/bd/main_test/transaccion/fecha=2018-01-21

-- Dentro encontramos el archivo de datos que subimos
-- Cada partición dentro de una tabla es reflejada como una subcarpeta dentro de HDFS
-- Al subir el siguiente archivo de datos deberá crearse otra subcarpeta (fecha=2018-01-22)
LOAD DATA LOCAL INPATH '/dataset/transacciones-2018-01-22.data' 
OVERWRITE INTO TABLE main_TEST.TRANSACCION 
PARTITION (FECHA='2018-01-22');

-- Mostramos la particiones existentes
SHOW PARTITIONS main_TEST.TRANSACCION;

-- Listamos el contenido de la carpeta HDFS de la tabla
hdfs dfs -ls /user/main/bd/main_test/transaccion

-- También podemos agregar manualmente los datos a nuestra tabla desde HDFS
-- En la ruta HDFS de nuestra tabla, creamos la siguiente carpeta:
hdfs dfs -mkdir -p /user/main/bd/main_test/transaccion/fecha=2018-01-23

-- Subimos el archivo de transacciones de ese día
hdfs dfs -put /dataset/transacciones-2018-01-23.data /user/main/bd/main_test/transaccion/fecha=2018-01-23

-- Verificamos si existe la partición
SHOW PARTITIONS main_TEST.TRANSACCION;

-- Notamos que la subcarpeta creada aún no ha sido tomada como partición
-- Debemos indicarle a HIVE que "repare" la tabla para que agregue la partición
MSCK REPAIR TABLE main_TEST.TRANSACCION;

-- Verificamos si existe la partición
SHOW PARTITIONS main_TEST.TRANSACCION;

-- Verificamos que hayan aumentado los datos de la tabla
SELECT COUNT(*) FROM main_TEST.TRANSACCION;

---------------------------------------------------------------------------------------------------------------- 

Tablas particionadas sobre HIVE ==> Particionamiento dinámico
-------------------------------

/*  Proceso al cual yo le indico el campo por el cual quiero particionar, por ejemplo, se tiene un UNICO 
    ARCHIVO el cual posee TODA LA DATA HISTORICA de transacciones de 10 años, es decir, tiene 3650 fechas
    diferentes, y queremos particionar por el campo FECHA. HIVE se dara cuenta de eso y creara 3650 archivos 
    diferentes, creara 3650 subdirectorios dentro del directorio de la tabla y hara el 'put' de manera
    automatica en cada uno de estos Subdirectorios. En otras palabras, el "Particionamiento dinamico" me
    automatiza una CARGA HISTORICA.     */  

-- Imaginemos que tenemos un único gran archivo histórico de transacciones que contiene todas las transacciones realizadas para todas las fechas
-- Ejemplos de registros de este archivo:
--
-- ID_PERSONA|ID_EMPRESA|MONTO|FECHA
-- 18|3|1383|2018-01-21
-- 30|6|2331|2018-01-21
-- 12|4|467|2018-01-23
-- 28|1|730|2018-01-21
-- 24|6|4475|2018-01-22
-- 67|9|561|2018-01-22
-- 9|4|3765|2018-01-22
-- 36|2|2659|2018-01-23
-- 91|5|3497|2018-01-22
--
-- Queremos subir este archivo a una tabla particionada, en donde la partición es la FECHA
-- Sabemos que una tabla particionada tiene subcarpetas por cada partición, dentro de las subcarpetas, van los archivos
-- ¿Cómo cortamos el archivo para que aterrice en forma de partición en cada subcarpeta?

-- Existen dos maneras de solucionar esto: 

-- 1.- Para ambas es necesario primero crear una tabla sin particionar que contenga todo el archivo
--     Creamos la tabla, notemos que el campo FECHA es incluido dentro de la sentencia de creación
CREATE TABLE main_TEST.TRANSACCION_SIN_PARTICIONAR(
ID_PERSONA STRING,
ID_EMPRESA STRING,
MONTO DOUBLE,
FECHA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/main/bd/main_test/transaccion_sin_particionar';

-- Colocamos el archivo de datos
LOAD DATA LOCAL INPATH '/dataset/transacciones.data' 
OVERWRITE INTO TABLE main_TEST.TRANSACCION_SIN_PARTICIONAR;

-- Verificamos
SELECT * FROM main_TEST.TRANSACCION_SIN_PARTICIONAR LIMIT 10;

-- 2.- También creamos la tabla en donde aterrizarán los datos particionados
CREATE TABLE main_TEST.TRANSACCION_PARTICIONADA(
ID_PERSONA STRING,
ID_EMPRESA STRING,
MONTO DOUBLE
)
PARTITIONED BY (FECHA STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/main/bd/main_test/transaccion_particionada';

-- Revisemos las dos soluciones

-- SOLUCIÓN 1: AGREGAR MANUALMENTE LAS PARTICIONES
-- Por medio de un query, agregamos partición por partición

-- Insertamos la partición de la fecha "2018-01-21"
INSERT OVERWRITE TABLE main_TEST.TRANSACCION_PARTICIONADA 
PARTITION (FECHA='2018-01-21')
SELECT T_S_P.ID_PERSONA, T_S_P.ID_EMPRESA, T_S_P.MONTO 
FROM main_TEST.TRANSACCION_SIN_PARTICIONAR T_S_P
WHERE T_S_P.FECHA = '2018-01-21';

-- Verificamos si existe la partición y tiene datos
SHOW PARTITIONS main_TEST.TRANSACCION_PARTICIONADA;
SELECT * FROM main_TEST.TRANSACCION_SIN_PARTICIONAR WHERE FECHA = '2018-01-21' LIMIT 10;

-- Insertamos la partición de la fecha "2018-01-22"
INSERT OVERWRITE TABLE main_TEST.TRANSACCION_PARTICIONADA 
PARTITION (FECHA='2018-01-22')
SELECT T_S_P.ID_PERSONA, T_S_P.ID_EMPRESA, T_S_P.MONTO 
FROM main_TEST.TRANSACCION_SIN_PARTICIONAR T_S_P
WHERE T_S_P.FECHA = '2018-01-22';

-- Verificamos si existe la partición y tiene datos
SHOW PARTITIONS main_TEST.TRANSACCION_PARTICIONADA;
SELECT * FROM main_TEST.TRANSACCION_SIN_PARTICIONAR WHERE FECHA = '2018-01-22' LIMIT 10;

-- Insertamos la partición de la fecha "2018-01-23"
INSERT OVERWRITE TABLE main_TEST.TRANSACCION_PARTICIONADA 
PARTITION (FECHA='2018-01-23')
SELECT T_S_P.ID_PERSONA, T_S_P.ID_EMPRESA, T_S_P.MONTO 
FROM main_TEST.TRANSACCION_SIN_PARTICIONAR T_S_P
WHERE T_S_P.FECHA = '2018-01-23';

-- Verificamos si existe la partición y tiene datos
SHOW PARTITIONS main_TEST.TRANSACCION_PARTICIONADA;
SELECT * FROM main_TEST.TRANSACCION_SIN_PARTICIONAR WHERE FECHA = '2018-01-23' LIMIT 10;

-- Esta solución es buena para un par de particiones
-- ¿Que pasaría si tuvieramos 1000 particiones?, esta solución sería impráctica
-- En general se recomienda usar la solución 2

-- SOLUCIÓN 2: AGREGAR DINÁMICAMENTE LAS PARTICIONES
-- Con esta solución, HIVE automáticamente crea todas las particiones necesarias

-- Limpiaremos la tabla del ejemplo para volver a cargar las particiones
-- Borramos todas las particiones existentes
ALTER TABLE main_TEST.TRANSACCION_PARTICIONADA 
DROP IF EXISTS PARTITION (FECHA='2018-01-21');

ALTER TABLE main_TEST.TRANSACCION_PARTICIONADA 
DROP IF EXISTS PARTITION (FECHA='2018-01-22');

ALTER TABLE main_TEST.TRANSACCION_PARTICIONADA 
DROP IF EXISTS PARTITION (FECHA='2018-01-23');

-- Verificamos
SHOW PARTITIONS main_TEST.TRANSACCION_PARTICIONADA;
SELECT * FROM main_TEST.TRANSACCION_PARTICIONADA LIMIT 10;

-- Primero debemos activar el particionamiento dinámico con estos tres parámetros de HIVE
SET hive.exec.dynamic.partition=true;           <------------ Activamos el Particionamiento Dinamico
SET hive.exec.dynamic.partition.mode=nonstrict; <------------ Le estamos diciendo que el Particionamiento Dinamico es NO ESTRICTO
                                                        --    Por tanto, particionara todo, incluso con errores.

-- Ahora ejecutamos la carga de particiones dinámicas
-- Notemos que ahora debemos poner todos los campos en la sentencia "SELECT", incluyendo el campo de la partición
INSERT OVERWRITE TABLE main_TEST.TRANSACCION_PARTICIONADA 
PARTITION (FECHA) 
SELECT * 
FROM main_TEST.TRANSACCION_SIN_PARTICIONAR T_S_P;

-- Verificamos
SHOW PARTITIONS main_TEST.TRANSACCION_PARTICIONADA;
--   ________________
--  |   partition    |
--  |----------------|
--  |fecha=2018-01-21|
--  |fecha=2018-01-22|
--  |fecha=2018-01-23|
--  |fecha=FECHA     | <---------------- Debemos borrar esta particion erronea
--  |________________|

-- Eliminamos la particion errónea "fecha=FECHA"
ALTER TABLE main_TEST.TRANSACCION_PARTICIONADA 
DROP PARTITION (FECHA = 'FECHA')

-- Verificamos
SHOW PARTITIONS main_TEST.TRANSACCION_PARTICIONADA;
--   ________________
--  |   partition    |
--  |----------------|
--  |fecha=2018-01-21|
--  |fecha=2018-01-22|
--  |fecha=2018-01-23|
--  |________________|

SELECT * FROM main_TEST.TRANSACCION_PARTICIONADA LIMIT 10;

-- Si revisamos las rutas HDFS de las particiones podemos encontrar las subcarpetas generadas por cada partición
hdfs dfs -ls /user/main/bd/main_test/transaccion_particionada

-- Si en HDFS listamos alguna partición, podemos ver el archivo de datos generado
hdfs dfs -ls /user/main/bd/main_test/transaccion_particionada/fecha=2018-01-21

-- Si abrimos el archivo, podemos ver su contenido
hdfs dfs -tail /user/main/bd/main_test/transaccion_particionada/fecha=2018-01-21/000000_0

-- ¿Por qué el archivo se llama 000000_0?
-- En las herramientas basadas en el ecosistema de HADOOP mientras más grandes los archivos, más "fragmentos" se generan
-- Por ejemplo, si estuvieramos escribiendo a disco el resultado de un procesamiento de un archivo que en local pesa 10TB, es muy probable que encontremos estos archivos:
-- 000000_0, 000001_0, 000002_0, 000003_0, ..., 099243_0, ...