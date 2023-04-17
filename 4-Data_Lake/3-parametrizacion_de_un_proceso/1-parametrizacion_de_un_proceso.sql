PARAMETRIZACION DE UN PROCESO DE SOLUCION
=========================================

Podemos parametrizar nuestro codigo SQL utilizado en el punto (6) para la Implementacion del DataLake.

Para ello en consola utilizamos el siguiente comando: 
("proceso_1_v2.0.sql" se supone que es el archivo con el proceso con codigo SQL parametrizado)
         _____________________________________________________________________________________________________
        |                                                                                                     |         
        |   beeline -u jdbc:hive2:// -f /home/main/proceso_1_v2.0.sql --hiveconf "PARAM_USERNAME=MIUSUARIO"   |
        |_____________________________________________________________________________________________________|

Si en el caso de que existiese MÁS DE 1 PARAMETRO, se agrega a continuacion del primero:
         _____________________________________________________________________________________________________________________________
        |                                                                                                                             |         
        |   beeline -u jdbc:hive2:// -f /home/main/proceso_1_v2.0.sql --hiveconf "PARAM_USERNAME=MIUSUARIO" --hiveconf "PARAM2=XXXX"  |
        |_____________________________________________________________________________________________________________________________|

Sin embargo, el archivo "proceso_1_v3.0.sql" es un ejemplo de un 'ARQUETIPO DE CODIGO', es un Arquetipo base, 
dependiendo de la necesidad de nuestra empresa vamos a tener que hacer variaciones de este Arquetipo. En este
Script se genera el codigo que se emplea para la solucion necesitada, para nuestro ejemplo, seria la creacion
de la tabla "TRANSACCION_ENRIQUECIDA".