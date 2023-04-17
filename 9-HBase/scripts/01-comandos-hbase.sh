# En la vida real con HBase solamente vamos a hacer 2 cosas: crear tablas y eliminar tablas, porque el llenado 
# de los datos se tiene que hacer desde la fuente de datos en tiempo real, así que en consola o en interfaz 
# gráfica quieres usar, no vamos a utilizar otros comandos. De todas maneras voy a enseñarles todos los comandos 
# disponibles, porque vamos a tener que antes de hacer el arquetipo, primero vamos a tener que emular la ingesta 
# en tiempo real y una vez que tengamos dominada todos los componentes de software por separado, ya vamos a 
# ponerlo en un arquetipo que va a correr de manera automática.

#Acceder a la consola
hbase shell

#Listando los namespaces
list_namespace

#Creando un namespace
create_namespace 'MIUSUARIO_SMART_PROYECTO_3'

#Verificamos
list_namespace

#Creando una tabla (En la vida real, hasta aca se utiliza HBase, luego se va insertando los datos con Spark Streaming)
create 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', 'datos_personales', 'datos_contacto', 'familiares'

#Verificamos
list

#Insertamos datos (Para efectos del ejemplo vamos a insertar datos)
#Tomaremos como key el DNI de la persona

#Registro 1
                                          # ROW_KEY    # Familia de columnas
put 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '57445143', 'datos_personales:nombres', 'Juan Luis'
put 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '57445143', 'datos_personales:apellidos', 'Perez Gomez'
put 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '57445143', 'datos_contacto:telefono', '981 212 218'
put 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '57445143', 'datos_contacto:correo', 'juanluis@gmail.com'
put 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '57445143', 'familiares:padre', 'Luis Perez'
put 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '57445143', 'familiares:madre', 'Ana Gomez'

#Verificamos
scan 'MIUSUARIO_SMART_PROYECTO_3:PERSONA'

#Registro 2
put 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '22385897', 'datos_personales:nombres', 'Alberto'
put 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '22385897', 'datos_personales:apellidos', 'Ruiz'
put 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '22385897', 'datos_contacto:telefono1', '928 172 032'
put 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '22385897', 'datos_contacto:telefono2', '01 389 2212'
put 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '22385897', 'datos_contacto:correo', 'juanluis@gmail.com'
put 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '22385897', 'familiares:hermano1', 'Julisa Ruiz'
put 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '22385897', 'familiares:hermano2', 'Martin Ruiz'
put 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '22385897', 'familiares:hermano3', 'Jose Ruiz'

#Verificamos
scan 'MIUSUARIO_SMART_PROYECTO_3:PERSONA'

#En HBASE podemos actualizar los datos
put 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '57445143', 'datos_personales:nombres', 'Marcos Luis'

#Verificamos
scan 'MIUSUARIO_SMART_PROYECTO_3:PERSONA'

#También podemos consultar solo un registro
get 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '57445143'

#Podemos mostrar sólo una familia de columnas
get 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '57445143', {COLUMN => 'datos_personales'}

#Podemos mostrar sólo una familia de columnas
get 'MIUSUARIO_SMART_PROYECTO_3:PERSONA', '57445143', {COLUMN => 'datos_personales:nombres'}

# Para eliminar una tabla debemos primero deshabilitarla
# Lo que hace el comando "disable" es atender las últimas peticiones que hayan llovido en la tabla,
# termina de atenderlas y cierra la tabla y ya no permite más incersiones. Pra volver a activarla
# utilizamos el comando "enable", para seguir soportando la tormenta de datos.
disable 'MIUSUARIO_SMART_PROYECTO_3:PERSONA'

#Luego eliminarla
drop 'MIUSUARIO_SMART_PROYECTO_3:PERSONA'

#Verificamos
list

#También, cuando creamos una tabla podemos indicarle como repartir la tabla en regiones
#En este ejemplo, estamos creando 4 regiones
#Region 1: las keys que se encuentren en el rango lexicografico por debajo de 1
#Region 2: las keys que se encuentren en el rango lexicografico igual o por encima de 1, y por debajo de 2
#Region 3: las keys que se encuentren en el rango lexicografico igual o por encima de 2, y por debajo de 3
#Region 4: las keys que se encuentren en el rango lexicografico igual o por encima de 3
#Esto nos ayuda a balancear mejor la distribución de datos
create 'MIUSUARIO_SMART_PROYECTO_3:empresa', 'datos_juridicos', 'datos_contacto', SPLITS=> ['1', '2', '3'] # quede con dudas

#Si tenemos muchas tablas sobre un namespace
create 'MIUSUARIO_SMART_PROYECTO_3:t1', 'f1', 'f2', 'f3'
create 'MIUSUARIO_SMART_PROYECTO_3:t2', 'f1', 'f2', 'f3'
create 'MIUSUARIO_SMART_PROYECTO_3:t3', 'f1', 'f2', 'f3'

#Verificamos
list

#Podemos eliminar todas las tablas de la siguiente manera
disable_all 'MIUSUARIO_SMART_PROYECTO_3:.*'
drop_all 'MIUSUARIO_SMART_PROYECTO_3:.*'

#Verificamos
list

#También podemos eliminar el namespace, siempre y cuando este no contenga tablas
drop_namespace 'MIUSUARIO_SMART_PROYECTO_3'

#Verificamos
list_namespace

#Para salir de la HBase shell
CTRL + C