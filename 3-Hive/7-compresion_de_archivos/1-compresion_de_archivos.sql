COMPRESION DE ARCHIVOS
======================

-- Independientemente del formato de archivos, existe la compresión de datos
-- La compresión de datos permite ahorrar espacio en disco duro, aunque perdemos algo de velocidad en el procesamiento
-- Existen varios formatos de compresión como 'GZIP' y 'SNAPPY', pero el que actualmente es estándar es 'SNAPPY'
-- SNAPPY permite tener una reducción significativa en el tamaño del archivo (entre el 20% al 80%), y reduce mínimamente la velocidad de procesamiento (entre el 5% al 10%)
-- (En la Clase) Se comenta que la compresion SNAPPY conlleva una penalizacion de un 20% en la velocidad de procesamiento. Esto significa que si el proceso demora 100 minutos
-- ahora demorara 120 minutos en completarse.
-- Una penalizacion para un procesamiento "REAL TIME" si podria llegar a ser considerable, pero para un entorno "BATCH" no lo es tanto. Por lo tanto, si vamos a trabajar
-- en un entorno BATCH, el estandar es que hay que guardar la data con compresion.

-- Dependiendo del tipo de formato, creamos una tabla comprimida de la siguiente manera

-- Una cosa es que la tabla soporte compresion y otra cosa es que la tabla tenga archivos comprimidos.

COMPRESION DE UN FORMATO PARQUET
--------------------------------

-- Creación de tabla PARQUET con compresión SNAPPY (una tabla que soporte compresion)
CREATE TABLE main_TEST3.PERSONA_PARQUET_SNAPPY(
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
LOCATION '/user/main/bd/main_test3/persona_parquet_snappy'
TBLPROPERTIES ("parquet.compression"="SNAPPY"); <-------------- Tabla soporta compresion SNAPPY

-- Para llenar los datos, ejecutamos las siguiente líneas
-- Primero indicamos con estas dos líneas que el resultado del procesamiento será un comprimido
-- Y que el tipo de compresión es SNAPPY
SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;

-- Ahora ejecutamos la sentencia de carga de datos
INSERT OVERWRITE TABLE main_TEST3.PERSONA_PARQUET_SNAPPY
SELECT * FROM  main_TEST3.PERSONA_TEXTFILE
WHERE ID != 'ID'; <------------------------------------ Esta linea no la hizo en la Clase

-- Verificamos
SELECT * FROM main_TEST3.PERSONA_PARQUET_SNAPPY LIMIT 10;

-- Si comparamos los pesos del archivo sin comprimir y el comprimido, notaremos la reducción de tamaño
hdfs dfs -ls /user/main/bd/main_test3/persona_parquet/
hdfs dfs -ls /user/main/bd/main_test3/persona_parquet_snappy/

---------------------------------------------------------------------------------------------------------------- 

COMPRESION DE UN FORMATO ORC
----------------------------

-- Creación de tabla ORC con compresión SNAPPY
CREATE TABLE main_TEST3.PERSONA_ORC_SNAPPY(
ID STRING,
NOMBRE STRING,
TELEFONO STRING,
CORREO STRING,
FECHA_INGRESO DATE,
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
STORED AS ORC
LOCATION '/user/main/bd/main_test3/persona_orc_snappy'
TBLPROPERTIES ("orc.compression"="SNAPPY");

-- Para llenar los datos, ejecutamos las siguiente líneas
-- Primero indicamos con estas dos líneas que el resultado del procesamiento será un comprimido
-- Y que el tipo de compresión es SNAPPY
SET hive.exec.compress.output=true;
SET orc.compression=SNAPPY;

-- Ahora ejecutamos la sentencia de carga de datos
INSERT OVERWRITE TABLE main_TEST3.PERSONA_ORC_SNAPPY
SELECT * FROM  main_TEST3.PERSONA_TEXTFILE
WHERE ID != 'ID';

-- Verificamos
SELECT * FROM main_TEST3.PERSONA_ORC_SNAPPY LIMIT 10;

-- Si comparamos los pesos del archivo sin comprimir y el comprimido, notaremos la reducción de tamaño
hdfs dfs -ls /user/main/bd/main_test3/persona_orc/
hdfs dfs -ls /user/main/bd/main_test3/persona_orc_snappy/

---------------------------------------------------------------------------------------------------------------- 

COMPRESION DE UN FORMATO AVRO
-----------------------------

-- Creación de una tabla en AVRO con compresión SNAPPY

-- Creamos la carpeta en donde estará el archivo
hdfs dfs -mkdir -p /user/main/schema/main_test3

-- Subimos el archivo de esquema
hdfs dfs -put /dataset/persona_avro_snappy.avsc /user/main/schema/main_test3

-- Creamos la tabla
CREATE TABLE main_TEST3.PERSONA_AVRO_SNAPPY
STORED AS AVRO
LOCATION '/user/main/bd/main_test3/persona_avro_snappy'
TBLPROPERTIES (
'avro.schema.url'='hdfs:///user/main/schema/main_test3/persona_avro_snappy.avsc',
'avro.output.codec'='snappy'
);

-- Para llenar los datos, ejecutamos las siguiente líneas
-- Primero indicamos con estas dos líneas que el resultado del procesamiento será un comprimido
-- Y que el tipo de compresión es SNAPPY
SET hive.exec.compress.output=true;
SET avro.output.codec=snappy;

-- Ahora ejecutamos la sentencia de carga de datos
INSERT OVERWRITE TABLE main_TEST3.PERSONA_AVRO_SNAPPY
SELECT * FROM  main_TEST3.PERSONA_TEXTFILE
WHERE ID != 'ID';

-- Verificamos
SELECT * FROM main_TEST3.PERSONA_AVRO_SNAPPY LIMIT 10;

-- Si comparamos los pesos del archivo sin comprimir y el comprimido, notaremos la reducción de tamaño
hdfs dfs -ls /user/main/bd/main_test3/persona_avro/
hdfs dfs -ls /user/main/bd/main_test3/persona_avro_snappy/