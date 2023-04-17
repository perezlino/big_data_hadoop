-- 
-- @author Alonso Melgarejo
-- @email alonsoraulmgs@gmail.com
-- @copyright Big Data Academy
--
-- Despliegue del esquema "LANDING"
-- 

-- 
-- @section Tuning
-- 

SET hive.execution.engine=mr;
SET mapreduce.job.maps=8;
SET mapreduce.input.fileinputformat.split.maxsize=128000000;
SET mapreduce.input.fileinputformat.split.minsize=128000000;
SET mapreduce.map.cpu.vcores=2;
SET mapreduce.map.memory.mb=128;
SET mapreduce.job.reduces=8;
SET mapreduce.reduce.cpu.vcores=2;
SET mapreduce.reduce.memory.mb=128;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=9999;
SET hive.exec.max.dynamic.partitions.pernode=9999;
SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;
SET orc.compression=SNAPPY;
SET avro.output.codec=SNAPPY;
SET mapred.job.queue.name=q_user_main;
SET spark.job.queue.name=q_user_main;
SET tez.job.queue.name=q_user_main;

--
-- @section Programa
--

-- Borrado de la base de datos
DROP DATABASE IF EXISTS ${hiveconf:PARAM_USERNAME}_LANDING_TMP CASCADE;

-- Creación de la base de datos
CREATE DATABASE ${hiveconf:PARAM_USERNAME}_LANDING_TMP;

--
-- Tabla Persona
--

-- Creación de tabla
CREATE TABLE ${hiveconf:PARAM_USERNAME}_LANDING_TMP.PERSONA(
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
LOCATION '/user/${hiveconf:PARAM_USERNAME}/ejercicio2/database/${hiveconf:PARAM_USERNAME}_LANDING_TMP/persona';

-- Subida de datos
LOAD DATA LOCAL INPATH '/dataset/persona.data'
INTO TABLE ${hiveconf:PARAM_USERNAME}_LANDING_TMP.PERSONA;

-- Impresión de datos
SELECT * FROM ${hiveconf:PARAM_USERNAME}_LANDING_TMP.PERSONA LIMIT 10;

--
-- Tabla Empresa
--

-- Creación de tabla
CREATE TABLE ${hiveconf:PARAM_USERNAME}_LANDING_TMP.EMPRESA(
ID STRING,
NOMBRE STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/${hiveconf:PARAM_USERNAME}/ejercicio2/database/${hiveconf:PARAM_USERNAME}_LANDING_TMP/empresa';

-- Subida de datos
LOAD DATA LOCAL INPATH '/dataset/empresa.data'
INTO TABLE ${hiveconf:PARAM_USERNAME}_LANDING_TMP.EMPRESA;

-- Impresión de datos
SELECT * FROM ${hiveconf:PARAM_USERNAME}_LANDING_TMP.EMPRESA LIMIT 10;

--
-- Tabla Transaccion
--

-- Creación de tabla
CREATE TABLE ${hiveconf:PARAM_USERNAME}_LANDING_TMP.TRANSACCION(
ID_PERSONA STRING,
ID_EMPRESA STRING,
MONTO STRING,
FECHA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/${hiveconf:PARAM_USERNAME}/ejercicio2/database/${hiveconf:PARAM_USERNAME}_LANDING_TMP/transaccion';

-- Subida de datos
LOAD DATA LOCAL INPATH '/dataset/transacciones.data'
INTO TABLE ${hiveconf:PARAM_USERNAME}_LANDING_TMP.TRANSACCION;

-- Impresión de datos
SELECT * FROM ${hiveconf:PARAM_USERNAME}_LANDING_TMP.TRANSACCION LIMIT 10;