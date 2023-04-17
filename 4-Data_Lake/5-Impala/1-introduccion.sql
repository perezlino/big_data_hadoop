INTRODUCCION
============

HIVE es una muy buena herramienta para hacer procesamientos del tipo "ETL", pero es muy poco eficiente
para hacer analisis exploratorio de datos. Eso es porque HIVE para que nuestro codigo funcione nos va
a pedir que le hagamos 'tunning'. Ahora que tenemos el OUTPUT final, el usuario que empiece a jugar con
los datos y necesita que sea lo mas rapido posible, en ese caso caso vamos a necesitar una herramienta
que se'autotunee' y esa herramienta es IMPALA. IMPALA nos permite ejecutar el codigo tambien con SQL, 
pero la diferencia es que IMPALA tiene una cola de recursos reservada y va a utilizar la mayor cantidad
de recursos disponibles para poder resolver la sentencia que se lance lo mas rapido posible. ¿En que se
traduce esto? A veces nuestro SQL en IMPALA puede que demore 10 segundos, esperamos un par de segundos y
volvemos a ejecutar la misma sentencia y quizas ahora demore 10 minutos. IMPALA es muy inestable trata de
hacerlo lo mas rapido posible. HIVE no, como nosotros vamos a tunear nuestro codigo en HIVE, ya le estamos
poniendo cuanta RAM y CPU queremos, y esto nos da garantia que SIEMPRE SE VA A DEMORAR LO MISMO.

Lo unico que tenemos que saber de IMPALA es que debemos SINCRONIZARLO CON HIVE. Cuando iniciemos sesion en
IMPALA debemos invalidar la METADATA de IMPALA para que se actualice.

-- Actualizamos la metadata de impala
INVALIDATE METADATA;

-- Ejecutamos alguna consulta
SELECT * FROM main_UNIVERSAL.TRANSACCION_ENRIQUECIDA WHERE EDAD_PERSONA < 25;