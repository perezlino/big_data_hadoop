##
## @author Alonso Melgarejo
## @email alonsoraulmgs@gmail.com
## @copyright Big Data Academy
##
## Asigna permisos de acceso
## 

## 
## @section Parámetros
## 

PARAM_USERNAME=$1

##
## @section Programa
##

#Asignamos permisos
echo "Asignando permisos..."
hdfs dfs -chown usuario5:grupoA /user/$PARAM_USERNAME/ejercicio1/data1/file.txt
hdfs dfs -chmod 755 /user/$PARAM_USERNAME/ejercicio1/data1/file.txt
hdfs dfs -chmod 700 /user/$PARAM_USERNAME/ejercicio1/data1/voidfile.txt
hdfs dfs -chown -R usuario2:grupoK /user/$PARAM_USERNAME/ejercicio1/data2
hdfs dfs -chmod -R 777 /user/$PARAM_USERNAME/ejercicio1/data2