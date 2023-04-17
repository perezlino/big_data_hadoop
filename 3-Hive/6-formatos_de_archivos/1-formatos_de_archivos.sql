FORMATOS DE ARCHIVOS
====================

¿Que tipo de formato BINARIO escojo?

En HIVE existen dos tipos de formatos validos:

- FORMATOS ESTRUCTURADOS      : Aquellos archivos en los cuales existe una variabilidad baja en el numero de sus 
                                campos.

- FORMATOS SEMI-ESTRUCTURADOS : Aquellos archivos en los cuales existe mucha variabilidad en el numero de sus campos.
                                Por ejemplo, periodicamente podriamos tener la llegada de archivos, para los cuales 
                                varia el numero de sus campos, es decir, el dia 1 nos llega el archivo con los campos
                                'nombre','apellido','edad','ciudad', sin embargo, para el dia 2 nos llega el archivo
                                solo con los campos 'nombre' y 'edad', y asi sucesivamente, donde para los archivos 
                                su numero de campos VARIA DE FORMA RECURRENTE.

==> Al trabajar con archivos SEMI-ESTRUCTURADOS se recomienda utilizar el formato binario AVRO. Si la variabilidad de
    campos es ALTA debemos formatear nuestros archivos al formato AVRO.                                

==> Al trabajar con archivos ESTRUCTURADOS se recomienda utilizar los formatos binarios PARQUET y ORC.
    Sin embargo PARQUET no es compatible con el tipo de dato DATE y el formato ORC si lo es. Por otro
    lado, PARQUET puede llegar a ser 10 veces mas rapido que ORC.

---------------------------------------------------------------------------------------------------------------- 

-- Hasta ahora hemos trabajado con un formato de archivos de texto plano
-- Este formato formalmente es llamado TEXTFILE
-- El formato TEXTFILE es el peor formato que existe para procesamiento, es muy lento
/* Generalmente el formato TEXTFILE se utiliza SOLO PARA REALIZAR LA INGESTA DE DATOS, 
   pero esta PROHIBIDO lazanr cualquier proceso sobre estas tablas. Tan simple como un COUNT 
   o tan complejo como una Red Neuronal.  */

-- Existen 4 formatos de archivo:
--
-- - TEXTFILE: Formato de texto plano, bueno porque nos permite colocar con un "put" la data sobre HDFS, malo porque es muy lento al procesar (por ejemplo, para operaciones JOIN y GROUP BY)
-- - PARQUET: Formato binario, bueno porque es el formato más rápido para procesamiento, malo porque en algunas versiones no se soporta el tipo "DATE".
-- - ORC: Formato binario, bueno porque es el segundo formato más rápido para procesamiento y soportar tipos de datos DATE, malo porque no es compatible con otras herramientas como IMPALA.
-- - AVRO: Formato binario, bueno porque permite tener un esquema dinámico que varia en el tiempo, malo porque tiene un procesamiento lento.

-- ¿Qué formatos se utilizan en la vida real?
-- Explicar el patrón de ingesta de datos

PROCESO DE FORMATEO

1.- INGESTA DEL ARCHIVO TEXTO PLANO
2.- FORMATEO DEL ARCHIVO

-- Se va a tomar el archivo de formato TEXTFILE (que ya se encuentra previamente cargado en HDFS) y vamos a realizar un
-- proceso de conversión. Se toma de entrada este archivo de texto plano, y en una tabla que tiene su propio directorio
-- HDFS, va escribir ese archivo pero en formato BINARIO.

FORMATEO A FORMATO PARQUET

-- Creación de la base de datos
CREATE DATABASE main_TEST3 LOCATION '/user/main/bd/main_test3';    

-- Creación de una tabla en TEXTFILE
CREATE TABLE main_TEST3.PERSONA_TEXTFILE(
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
LOCATION '/user/main/bd/main_test3/persona_textfile';

-- Le cargamos datos
LOAD DATA LOCAL INPATH '/dataset/persona.data' INTO TABLE main_TEST3.PERSONA_TEXTFILE;

-- Verificamos
SELECT * FROM main_TEST3.PERSONA_TEXTFILE LIMIT 10;
hdfs dfs -tail /user/main/bd/main_test3/persona_textfile/persona.data

-- Creación de una tabla en PARQUET
CREATE TABLE main_TEST3.PERSONA_PARQUET(
ID STRING,
NOMBRE STRING,
TELEFONO STRING,
CORREO STRING,
FECHA_INGRESO STRING, <-------------- El tipo de dato de la FECHA debemos colocarla como STRING, no soporta el tipo DATE
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
STORED AS PARQUET <------------------------------------------ Le indicamos el tipo de formato
LOCATION '/user/main/bd/main_test3/persona_parquet';

-- Le cargamos datos
INSERT OVERWRITE TABLE main_TEST3.PERSONA_PARQUET
SELECT * FROM  main_TEST3.PERSONA_TEXTFILE
WHERE ID != 'ID'; <------------------------------------ Esta linea no la hizo en la Clase

-- Verificamos
SELECT * FROM main_TEST3.PERSONA_PARQUET LIMIT 10;

-- Verificamos que se haya creado el archivo de datos
hdfs dfs -ls /user/main/bd/main_test3/persona_parquet/

-- El archivo de datos tiene un formato PARQUET, si lo abrimos, veremos código binario
hdfs dfs -tail /user/main/bd/main_test3/persona_parquet/000000_0

---------------------------------------------------------------------------------------------------------------- 

FORMATEO A FORMATO ORC

-- Creación de una tabla en ORC
CREATE TABLE main_TEST3.PERSONA_ORC(
ID STRING,
NOMBRE STRING,
TELEFONO STRING,
CORREO STRING,
FECHA_INGRESO DATE, <-------------- El tipo de dato de la FECHA aca podemos utilizar DATE
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
STORED AS ORC <------------------------------------------ Le indicamos el tipo de formato
LOCATION '/user/main/bd/main_test3/persona_orc';

-- Le cargamos datos
INSERT OVERWRITE TABLE main_TEST3.PERSONA_ORC
SELECT * FROM  main_TEST3.PERSONA_TEXTFILE
WHERE ID != 'ID'; <------------------------------------ Esta linea no la hizo en la Clase

-- Verificamos
SELECT * FROM main_TEST3.PERSONA_ORC LIMIT 10;

-- Verificamos que se haya creado el archivo de datos
hdfs dfs -ls /user/main/bd/main_test3/persona_orc/

-- El archivo de datos tiene un formato ORC, si lo abrimos, veremos código binario
hdfs dfs -tail /user/main/bd/main_test3/persona_orc/000000_0

---------------------------------------------------------------------------------------------------------------- 

FORMATEO A FORMATO AVRO

-- Avro necesita de un archivo de esquema
-- En el archivo de esquema se coloca la estructura de la tabla
-- Debemos subirlo a HDFS
-- Creamos la carpeta en donde estará el archivo
hdfs dfs -mkdir -p /user/main/schema/main_test3

-- Subimos el archivo de definicion de esquema AVRO
hdfs dfs -put /dataset/persona_avro.avsc /user/main/schema/main_test3

-- En el archivo AVRO .avsc se especifica lo siguiente:

  "namespace": "MIUSUARIO_TEST3",  <---------- Nombre de la BBDD
  "name": "PERSONA_AVRO",          <---------- Nombre de la tabla asociada
  "type": "record",                <---------- Mos indica que son registros
  "fields": [                      <---------- Mos indica los campos
    {"name": "ID", "type": ["int", "null"]},
    {"name": "NOMBRE", "type": ["string", "null"]},          -- Los archivos AVRO tiene flexibilidad en el
    {"name": "TELEFONO", "type": ["string", "null"]},        -- tipo de dato. Por ejemplo, el campo ID indica
    {"name": "CORREO", "type": ["string", "null"]},          -- que puede ser del tipo ENTERO como del tipo NULL.
    {"name": "FECHA_INGRESO", "type": ["string", "null"]},   -- Y podriamos agregarle mas tipos de datos.
    {"name": "EDAD", "type": ["int", "null"]},
    {"name": "SALARIO", "type": ["double", "null"]},
    {"name": "ID_EMPRESA", "type": ["string", "null"]}
  ]

-- Creación de una tabla en AVRO
CREATE TABLE main_TEST3.PERSONA_AVRO
STORED AS AVRO
LOCATION '/user/main/bd/main_test3/persona_avro'
TBLPROPERTIES (
'avro.schema.url'='hdfs:///user/main/schema/main_test3/persona_avro.avsc'
);

-- Le cargamos datos
INSERT OVERWRITE TABLE main_TEST3.PERSONA_AVRO
SELECT * FROM  main_TEST3.PERSONA_TEXTFILE
WHERE ID != 'ID'; <------------------------------------ Esta linea no la hizo en la Clase

-- Verificamos
SELECT * FROM main_TEST3.PERSONA_AVRO LIMIT 10;

-- Verificamos que se haya creado el archivo de datos
hdfs dfs -ls /user/main/bd/main_test3/persona_avro/

-- El archivo de datos tiene un formato AVRO, si lo abrimos, veremos código binario
-- Esta vez abriremos con un "cat" para ver la cabecera del archivo
hdfs dfs -cat /user/main/bd/main_test3/persona_avro/000000_0

-- Si vemos las primeras líneas del archivo, encontraremos la estructura del archivo avro


PONGAMONOS EN EL CASO DE QUE EL ARCHIVO "persona.data" NOS LLEGO EL DIA 1, PERO PARA EL DIA 2
NOS LLEGA UN ARCHIVO CON MAS CAMPOS, ¿QUE HACEMOS?

1.- DROPEAR la tabla main_TEST3.PERSONA_TEXTFILE y volver a crearla agregando los campos nuevos
    que aparecen en el archivo del DIA 2

2.- Modificar nuestro ARCHIVO DE DEFINICION DE ESQUEMA AVRO   

3.- Conectarnos desde HDFS y borrar el archivo de definicion de esquema que habiamos subido y 
    volverlo a subir con los cambios que hemos efectuado

4.- Luego ejecutar las inserciones