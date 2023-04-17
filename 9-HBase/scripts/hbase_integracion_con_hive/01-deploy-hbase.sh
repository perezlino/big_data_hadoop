#Crearemos una tabla en HBase para integrarla con Hive
create_namespace 'MAIN_SMART_PROYECTO_3'
create 'MAIN_SMART_PROYECTO_3:PERSONA', 'datos_personales', 'datos_contacto', 'datos_empresariales'