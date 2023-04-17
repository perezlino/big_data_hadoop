#!/bin/bash

#Eliminamos la carpeta si existe
echo "Eliminando la carpeta"
hdfs dfs -rm -r -f /user/main/ejercicio2

#Estructura de carpetas para "landing_tmp"
echo "Creando la estructura de carpetas para landing_tmp..."
hdfs dfs -mkdir -p \
/user/main/ejercicio2/database/landing_tmp \
/user/main/ejercicio2/database/landing_tmp/persona \
/user/main/ejercicio2/database/landing_tmp/empresa \
/user/main/ejercicio2/database/landing_tmp/transaccion

#Estructura de carpetas para "landing"
echo "Creando la estructura de carpetas para landing..."
hdfs dfs -mkdir -p \
/user/main/ejercicio2/database/landing \
/user/main/ejercicio2/database/landing/persona \
/user/main/ejercicio2/database/landing/empresa \
/user/main/ejercicio2/database/landing/transaccion \
/user/main/ejercicio2/schema/landing

#Estructura de carpetas para "universal"
echo "Creando la estructura de carpetas para universal..."
hdfs dfs -mkdir -p \
/user/main/ejercicio2/database/universal \
/user/main/ejercicio2/database/universal/persona \
/user/main/ejercicio2/database/universal/empresa \
/user/main/ejercicio2/database/universal/transaccion \
/user/main/ejercicio2/database/universal/transaccion_enriquecida

#Estructura de carpetas para "smart"
echo "Creando la estructura de carpetas para smart..."
hdfs dfs -mkdir -p \
/user/main/ejercicio2/database/smart

#Subida de archivos de "schema"
echo "Subiendo archivos de schema..."
hdfs dfs -put \
/home/main/persona.avsc \
/home/main/empresa.avsc \
/home/main/transaccion.avsc \
/user/main/ejercicio2/schema/landing
