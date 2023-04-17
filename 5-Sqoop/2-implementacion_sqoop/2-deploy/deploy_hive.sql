--
-- @author ALONSO RAÚL MELGAREJO GALVÁN
-- 
-- @email alonsoraulmgs@gmail.com
-- @linkedin https://www.linkedin.com/in/alonsoraulmg
-- @facebook https://www.facebook.com/alonsoraulmg
-- @copyright Big Data Academy
-- 
-- Tablas para el esquema "LANDING".
--

-- Crear base de datos
CREATE DATABASE IF NOT EXISTS MIUSUARIO_LANDING_TMP;

-- Creación de tabla
DROP TABLE IF EXISTS MIUSUARIO_LANDING_TMP.TARJETA_CLIENTE;
CREATE TABLE MIUSUARIO_LANDING_TMP.TARJETA_CLIENTE(
ID_PERSONA STRING,
ID_TIPO_TARJETA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/main/bd/miusuario_landing_tmp/tarjeta_cliente';

-- Creación de tabla
DROP TABLE IF EXISTS MIUSUARIO_LANDING_TMP.TARJETA_TIPO;
CREATE TABLE MIUSUARIO_LANDING_TMP.TARJETA_TIPO(
ID STRING,
DESCRIPCION STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/main/bd/miusuario_landing_tmp/tarjeta_tipo';