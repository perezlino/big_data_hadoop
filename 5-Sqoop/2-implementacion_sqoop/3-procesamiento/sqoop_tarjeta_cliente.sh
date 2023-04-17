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
#SELECT * FROM MIUSUARIO_LANDING_TMP.TARJETA_CLIENTE LIMIT 10;

#Agregamos el JDBC de MySQL al PATH
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:"/binarios/mysql-connector-java-8.0.12.jar"

#Ejecución de sqoop
sqoop import \
--connect "jdbc:mysql://localhost:3306/main?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC" \
--username root \
--password bigdataacademy \
--split-by ID_PERSONA \   <---------------- Nivel de paralelizacion es controlado por el campo 'split-by'
--columns ID_PERSONA,ID_TIPO_TARJETA \
--table TARJETA_CLIENTE \
--fields-terminated-by "|" \     <------------------------- Los campos estaran separados por un |. La data se esta pasando a un TEXTFILE.
--hive-import \
--hive-table MIUSUARIO_LANDING_TMP.TARJETA_CLIENTE \
--hive-overwrite \     <-------------------------------------------------- Si se buscar sobreescribir el contenido 
--delete-target-dir  <---------  Indicamos que todos los archivos          de la tabla "MIUSUARIO_LANDING_TMP.TARJETA_CLIENTE"                     
                                 temporales que se generen para            utilizamos 'hive-overwrite'.                  
                                 ingestar los datos, al finalizar
                                 se eliminen.


#Verificamos en HIVE  
#SELECT * FROM MIUSUARIO_LANDING_TMP.TARJETA_CLIENTE LIMIT 10;