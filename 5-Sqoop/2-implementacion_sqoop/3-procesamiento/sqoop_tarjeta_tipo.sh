#!/bin/sh

###
 # @author ALONSO RAÚL MELGAREJO GALVÁN
 # 
 # @email alonsoraulmgs@gmail.com
 # @linkedin https://www.linkedin.com/in/alonsoraulmg
 # @facebook https://www.facebook.com/alonsoraulmg
 # @copyright Big Data Academy
 # 
 # Sube los datos al esquema LANDING_TMP
 ##

#Verificamos en HIVE
#SELECT * FROM MIUSUARIO_LANDING_TMP.TARJETA_TIPO LIMIT 10;

#Agregamos el JDBC de MySQL al PATH
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:"/binarios/mysql-connector-java-8.0.12.jar"

#Ejecución de sqoop
sqoop import \
--connect "jdbc:mysql://localhost:3306/main?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC" \
--username root \
--password bigdataacademy \
--split-by ID \
--columns ID,DESCRIPCION \
--table TARJETA_TIPO \
--fields-terminated-by "|" \
--hive-import \
--hive-table MIUSUARIO_LANDING_TMP.TARJETA_TIPO \
--hive-overwrite \
--delete-target-dir

#Verificamos en HIVE
#SELECT * FROM MIUSUARIO_LANDING_TMP.TARJETA_TIPO LIMIT 10;