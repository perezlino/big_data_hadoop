UTILIZAR HIVE POR MEDIO DE CONSOLA
==================================

-- Ingresar a consola Hive con el cliente HIVE (deprecated --> Obsoleto)
hive

-- Ingresar a consola Hive con el cliente BEELINE (Esta se recomienda para trabajar)
beeline -u jdbc:hive2://

-- Para salir de la consola
CTRL + C (probar con 'quit;')

---------------------------------------------------------------------------------------------------------------- 

CREACION DE BASES DE DATOS Y TABLAS POR MEDIO DE CONSOLA Y/O HUE
================================================================


-- En HIVE, listamos las bases de datos existentes
SHOW DATABASES;

-- En HIVE, creamos una base de datos
CREATE DATABASE main_TEST;

-- En HIVE, verificamos
SHOW DATABASES;

-- En HIVE, también es posible crear bases de datos de la siguiente manera:
-- CREATE SCHEMA main_TEST;

-- En HIVE, creamos la tabla <-------------- Vamos a cargar en esta tabla el archivo "persona.data"
CREATE TABLE main_TEST.PERSONA(
ID STRING,
NOMBRE STRING,
TELEFONO STRING,
CORREO STRING,
FECHA_INGRESO STRING,
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'    <------------------- Separador de CAMPOS
LINES TERMINATED BY '\n'    <------------------- Separador de lineas, en este caso es el 'SALTO DE LINEA'
STORED AS TEXTFILE;         <------------------- Nos indica que el archivo que vamos a mapear es de TEXTO PLANO

-- En HIVE, verificamos
SHOW TABLES IN main_TEST;

-- ¿Dónde se crea la base de datos y la tabla? ¿Cual es el directorio que se ha asociado a nuestra tabla?
-- Se crean sobre HDFS, en la ruta "/user/hive/warehouse"

-- En HDFS, listar el contenido de la carpeta "/user/hive/warehouse"
hdfs dfs -ls /user/hive/warehouse

-- Notamos que se ha creado la carpeta "/user/hive/warehouse/main_test.db"
-- También notamos que no importa que hemos escrito en HIVE en mayúsculas el nombre de la base de 
-- datos (main_TEST), sobre HDFS la creará en minúsculas y le agregará el .db (main_test.db)
-- En HDFS, listar el contenido de la carpeta
hdfs dfs -ls /user/hive/warehouse/main_test.db

-- Dentro podemos ver la carpeta "/user/hive/warehouse/main_test.db/persona"
-- Este es el directorio que se asocia a la tabla que hemos creado
-- En HDFS, listar el contenido de la carpeta
hdfs dfs -ls /user/hive/warehouse/main_test.db/persona

-- En HDFS, subir el archivo LINUX "/dataset/persona.data" a "/user/hive/warehouse/main_test.db/persona"
hdfs dfs -put /dataset/persona.data /user/hive/warehouse/main_test.db/persona

-- En HIVE, mostramos algunos registros de la tabla
SELECT * FROM main_TEST.PERSONA LIMIT 10;

-- Si en determinado caso, algun registro para un determinado campo rompe el esquema definido (es decir,
   si se asigno un campo como INT y el dato del archivo es STRING, por ejemplo, el valor devuelto sera NULL.

-- En la sentencia de creación de la tabla HIVE, notamos que:
-- - Con "ROW FORMAT DELIMITED", indicamos que nuestra tabla tendra un archivo cuyo contenido está delimitado
-- - Con "FIELDS TERMINATED BY '|'", indicamos que nuestra tabla separa sus campos con el caracter "|"
-- - Con "LINES TERMINATED BY '\n'", indicamos que cada registro está separado por "\n" (salto de línea, es decir ENTER)
-- - Con "STORED AS TEXTFILE", indicamos que el FORMATO del archivo es del tipo TEXTFILE (texto plano, es decir si hacemos un "cat" sobre el archivo en HDFS encontraremos texto legible por un humano)
-- - Dependiendo de la estructura de nuestro archivo, deberemos cambiar el separador de campos y registros, por ejemplo, en un archivo CSV el separador será ','

-- Podemos ver una descripción de las columnas y sus tipos de datos
DESC main_TEST.PERSONA;

-- También podemos ver una descripción más detallada de la tabla
DESC FORMATTED main_TEST.PERSONA; 

     -- Veremos un dato llamado "Location" el caul nos indica el directorio asociado a la tabla

-- Como hemos visto, por defecto HIVE crea sus bases de datos y sus tablas en la ruta HDFS "/user/hive/warehouse"
-- Es posible que nosotros le indiquemos la ruta HDFS donde queremos que las creen

-- Crearemos una base de datos llamada "main_TEST2" sobre la ruta HDFS "/user/main/bd"
-- Creamos la ruta HDFS de la base de datos
hdfs dfs -mkdir -p /user/main/bd/main_test2

-- Creamos la base de datos, apuntando a la ruta HDFS
CREATE DATABASE main_TEST2 LOCATION "/user/main/bd/main_test2";

-- Crear la ruta HDFS de la tabla
hdfs dfs -mkdir -p /user/main/bd/main_test2/persona

-- Crear la tabla sobre la ruta HDFS
CREATE TABLE main_TEST2.PERSONA(
ID STRING,
NOMBRE STRING,
TELEFONO STRING,
CORREO STRING,
FECHA_INGRESO STRING,
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/main/bd/main_test2/persona'; <----- -- Le agregamos este nuevo parametro para asociarla a un directorio 
                                                    -- que nosotros indiquemos y no sea por defecto

-- Consultar la tabla
SELECT * FROM main_TEST2.PERSONA LIMIT 10;

-- Colocar el archivo de datos
-- En HDFS, subir el archivo LINUX "/dataset/persona.data" a "/user/main/bd/main_test2/persona"
hdfs dfs -put /dataset/persona.data /user/main/bd/main_test2/persona

-- Podemos hacerlo con un "put" de LINUX a HDFS, pero tambien existe la opcion de hacerlo con un comando propio de HIVE (en HUE o por Consola)
LOAD DATA LOCAL INPATH '/dataset/persona.data' INTO TABLE main_TEST2.PERSONA;

-- Consultar la tabla
SELECT * FROM main_TEST2.PERSONA LIMIT 10;

-- Eliminar la tabla
DROP TABLE main_TEST2.PERSONA;

-- ¿Qué pasa con la ruta y el archivo HDFS cuando eliminamos una tabla?
-- Revisemos la ruta donde estába la tabla
hdfs dfs -ls /user/main/bd/main_test2

-- Vemos que se ha borrado la carpeta y el archivo dentro de ella
-- Cuando nosotros creamos una tabla, por defecto esta se crea como "INTERNAL"
-- Una tabla "INTERNAL" es una tabla gestionada por HIVE, esto significa que si eliminamos la tabla, también su contenido sobre HDFS
-- El otro tipo de tabla es la "EXTERNAL", en este caso si eliminamos la tabla el contenido sobre HDFS se mantendrá
-- Volveremos a crear la tabla, pero esta vez como "EXTERNAL"
CREATE EXTERNAL TABLE main_TEST2.PERSONA(
ID STRING,
NOMBRE STRING,
TELEFONO STRING,
CORREO STRING,
FECHA_INGRESO STRING,
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/main/bd/main_test2/persona';

-- Cargamos el archivo
LOAD DATA LOCAL INPATH '/dataset/persona.data' INTO TABLE main_TEST2.PERSONA;

-- Verificamos
SELECT * FROM main_TEST2.PERSONA LIMIT 10;

-- Eliminamos la tabla
DROP TABLE main_TEST2.PERSONA;

-- Verificamos
SHOW TABLES IN main_TEST2;

-- Verificamos en la ruta HDFS de la base de datos
hdfs dfs -ls /user/main/bd/main_test2

-- Verificamos en la ruta HDFS de la tabla
hdfs dfs -ls /user/main/bd/main_test2/persona

/**
 * @section Consideraciones adicionales
 */

-- El comando para eliminar todo el contenido de una tabla es:
-- TRUNCATE TABLE MI_BASE_DE_DATOS.MI_TABLA;
-- Sin embargo, este comando no funciona sobre tablas "EXTERNAL", ya que en esos casos HIVE no puede eliminar los archivos HDFS
-- Para hacer un "TRUNCATE" sobre una tabla "EXTERNAL" deberá ejecutar el comando "hdfs dfs -rm -r -f /ruta/a/mi/tabla/*"

-- El comando para eliminar una base de datos es el siguiente:
-- DROP DATABASE MI_BASE_DE_DATOS;
-- Sin embargo, sólo funcionará si es que la base de datos no tiene tablas
-- Si queremos borrar una base de datos con tablas deberemos ejecutar:
-- DROP DATABASE MI_BASE_DE_DATOS CASCADE;

-- El comando para cargar datos: 
-- LOAD DATA LOCAL INPATH '/data/en/linux/archivo.data' INTO TABLE MI_BASE_DE_DATOS.MI_TABLA;
-- Agrega datos sobre la carpeta HDFS
-- Si queremos eliminar toda la data de la carpeta HDFS para luego colocar el nuevo archivo de datos, debemos ejecutar un "OVERWRITE":
-- LOAD DATA LOCAL INPATH '/data/en/linux/archivo.data' 
-- OVERWRITE INTO TABLE MI_BASE_DE_DATOS.MI_TABLA;

-- En algunas sentencias podemos agregar validadores "IF EXISTS" o "IF NOT EXISTS" para verificar algo antes de ejecutar la sentencia
-- Por ejemplo:
--
-- Crear una base de datos si no existe, si existe, no lanzará un mensaje de error
CREATE DATABASE IF NOT EXISTS MI_BASE_DE_DATOS;

-- Crear una tabla si no existe, si existe, no lanzará un mensaje de error
CREATE TABLE IF NOT EXISTS MI_BASE_DE_DATOS.MI_TABLA(CAMPO1 STRING);

-- Eliminar una tabla si existe, si no existe, no lanzará un mensaje de error
DROP TABLE IF EXISTS MI_BASE_DE_DATOS.MI_TABLA;

-- Eliminar una base de datos si existe, si no existe, no lanzará un mensaje de error
DROP DATABASE IF EXISTS MI_BASE_DE_DATOS;

---------------------------------------------------------------------------------------------------------------- 