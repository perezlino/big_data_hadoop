-- Ingresamos a la consola HIVE
-- beeline -u jdbc:hive2://

-- Creación de base de datos
CREATE DATABASE MAIN_SMART_PROYECTO_3;

-- Creación de tabla
CREATE EXTERNAL TABLE MAIN_SMART_PROYECTO_3.PERSONA(
key STRING,
nombre STRING,
telefono STRING,
correo STRING,
fecha_ingreso STRING,
edad STRING,
salario STRING,
id_empresa  STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping'='
datos_personales:nombre,
datos_contacto:telefono,
datos_contacto:correo,
datos_empresariales:fecha_ingreso,
datos_personales:edad,
datos_empresariales:salario,
datos_empresariales:id_empresa
')
TBLPROPERTIES('hbase.table.name'='MAIN_SMART_PROYECTO_3:PERSONA');


-- Se habia comentado que HBase no procesa datos. Supongamos que queremos hacer un SELECT * COUNT para saber
-- cantidad de registros en la tabla, HBase no puede hacer esto. Para eso podemos utilizar HIVE:

                     ___________________________
                    |            Hive           |    Creamos por encima de la tabla HBase, una tabla 
                    |___________________________|    Hive que mapee algunas columnas de algunas familias
                    |                           |    de columnas de la tabla HBase. La columna "nombre"
                    |        ___________        |    en la tabla de HIVE va a estar interconectada a la
                    |       |___________|       |    familia de columna "datos_personales" y columna
                    |       |           |       |    "nombre". Habran registros que en esa familia de 
                    |       |   HBase   |       |    tenga esta columna y los podremos ver en Hive.  
                    |       |___________|       |    Habran registros que en esa familia de columna no   
                    |                           |    tenga esa columna y Hive para ello nos mostrará
                    |___________________________|    "null". Y asi, los otros campos de la tabla Hive

como "telefono", "correo", "fecha_ingreso", "edad", "salario" y "id_empresa" también se interconectara
con sus campos similares en la tabla HBase. Ahora, la columna que va a estar mapeada con el ROW KEY de
HBase en Hive siempre se va a llamar "key". Por estándar, se tiende a colocar los MISMOS NOMBRES de 
columnas en AMBAS TABLAS, pero podrian tener distintos nombres. Del mismo modo, por estándar se tiende
a que ambas tablas tengan el mismo nombre.                    

¿Que significa que las tablas estén interconectadas?
----------------------------------------------------

En realidad, todo está a nivel HBase, Hive no está almacenando nada. Eso significa que si yo inserto algo,
un nuevo registro en la tabla HBase desde Hive lo voy a poder ver. Y si desde Hive yo inserto algo, todo
eso se va a ir almacenando en HBase, al final el nucleo es HBase, no es que los datos se esten duplicando.
