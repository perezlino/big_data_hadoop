#Desde HBASE, insertamos datos
put 'MAIN_SMART_PROYECTO_3:PERSONA', '57445143', 'datos_personales:nombre', 'Mauricio'

#Desde HIVE, verificamos
SELECT * FROM MAIN_SMART_PROYECTO_3.PERSONA;

#Desde HBASE, insertamos más campos al mismo registro
# (Si hacemos la inserción desde HBase va a ser en tiempo real, pero si hacemos la inserción
# desde la interfaz de Hive, esto no va a ser en tiempo real)
put 'MAIN_SMART_PROYECTO_3:PERSONA', '57445143', 'datos_contacto:telefono', '111222333'
put 'MAIN_SMART_PROYECTO_3:PERSONA', '57445143', 'datos_contacto:correo', 'abc@zzz.com'
put 'MAIN_SMART_PROYECTO_3:PERSONA', '57445143', 'datos_empresariales:fecha_ingreso', '2019-01-12'
put 'MAIN_SMART_PROYECTO_3:PERSONA', '57445143', 'datos_personales:edad', '33'
put 'MAIN_SMART_PROYECTO_3:PERSONA', '57445143', 'datos_empresariales:salario', '1500'
put 'MAIN_SMART_PROYECTO_3:PERSONA', '57445143', 'datos_empresariales:id_empresa', '2'

#Desde HIVE, verificamos
SELECT * FROM MAIN_SMART_PROYECTO_3.PERSONA;

#Desde HIVE, insertamos datos
INSERT INTO MAIN_SMART_PROYECTO_3.PERSONA VALUES ('22385897', 'Martin', '999888777', 'mmm@xyz.com', NULL, '27', '3000', '1');

#Desde HBASE, verificamos
get 'MAIN_SMART_PROYECTO_3:PERSONA', '22385897'

#En HBASE, actualizamos un valor
put 'MAIN_SMART_PROYECTO_3:PERSONA', '57445143', 'datos_contacto:correo', 'xxx@yyy.com'

#En HBASE, Verificamos
get 'MAIN_SMART_PROYECTO_3:PERSONA', '57445143'

#En HIVE, verificamos
SELECT * FROM MAIN_SMART_PROYECTO_3.PERSONA;

#En HIVE, eliminamos la tabla
# (Solo se elimina en Hive)
DROP TABLE IF EXISTS MAIN_SMART_PROYECTO_3.PERSONA;

#En HBASE, verificamos que aún exista la tabla y sus datos
scan 'MAIN_SMART_PROYECTO_3:PERSONA'