#!/bin/sh

#hbase shell

#Creación del esquema
create_namespace 'MAIN_SMART_PROYECTO_4'

#Creación de tabla
create 'MAIN_SMART_PROYECTO_4:PERSONA', 'DATOS'

#Creación de tabla
create 'MAIN_SMART_PROYECTO_4:EMPRESA', 'DATOS'

#Creación de tabla
create 'MAIN_SMART_PROYECTO_4:TRANSACCION_ENRIQUECIDA', 'PERSONA', 'EMPRESA', 'TRANSACCION'

#Verificamos
list
