#!/bin/sh

#hbase shell

#Consultamos los datos
scan 'MAIN_SMART_PROYECTO_4:PERSONA', {'LIMIT' => 10}

#Consultamos los datos
scan 'MAIN_SMART_PROYECTO_4:EMPRESA', {'LIMIT' => 10}

#Consultamos los datos
scan 'MAIN_SMART_PROYECTO_4:TRANSACCION_ENRIQUECIDA', {'LIMIT' => 10}