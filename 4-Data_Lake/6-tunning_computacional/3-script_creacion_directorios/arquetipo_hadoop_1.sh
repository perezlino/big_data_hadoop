##
## @author Alonso Melgarejo
## @email alonsoraulmgs@gmail.com
## @copyright Big Data Academy
##
## Crea la estructura de carpetas
## 

## 
## @section Parámetros
## 

PARAM_USERNAME=$1           <------ Parametro que se va a recibir en la posicion 1 en la consola
PARAM_NOMBRE_CARPETA=$2     <------ Parametro que se va a recibir en la posicion 2 en la consola

##
## @section Programa
##

#Eliminamos la carpeta del ejercicio
echo "Eliminando carpeta raiz..."
hdfs dfs -rm -r -f /user/$PARAM_USERNAME/$PARAM_NOMBRE_CARPETA

#Creamos la estructura de carpetas
echo "Creando carpetas..."
hdfs dfs -mkdir \
/user/$PARAM_USERNAME/ejercicio1 \
/user/$PARAM_USERNAME/ejercicio1/carpeta1 \
/user/$PARAM_USERNAME/ejercicio1/carpeta2 \
/user/$PARAM_USERNAME/ejercicio1/carpeta3 \
/user/$PARAM_USERNAME/ejercicio1/data1 \
/user/$PARAM_USERNAME/ejercicio1/data2 \
/user/$PARAM_USERNAME/ejercicio1/data3 \
/user/$PARAM_USERNAME/ejercicio1/carpeta1/subcarpeta1 \
/user/$PARAM_USERNAME/ejercicio1/carpeta1/subcarpeta2 \
/user/$PARAM_USERNAME/ejercicio1/carpeta1/subcarpeta3 \
/user/$PARAM_USERNAME/ejercicio1/data2/2017-01-27 \
/user/$PARAM_USERNAME/ejercicio1/data2/2017-01-28 \
/user/$PARAM_USERNAME/ejercicio1/data2/2017-01-29

#Subimos los archivos
echo "Subiendo archivos..."
hdfs dfs -put /dataset/persona.data /user/$PARAM_USERNAME/ejercicio1/carpeta1/subcarpeta1
hdfs dfs -put /dataset/empresa.data /user/$PARAM_USERNAME/ejercicio1/carpeta2
hdfs dfs -put /home/$PARAM_USERNAME/file.txt /user/$PARAM_USERNAME/ejercicio1/data1
hdfs dfs -touchz /user/$PARAM_USERNAME/ejercicio1/data1/voidfile.txt
hdfs dfs -put /home/$PARAM_USERNAME/file.txt /user/$PARAM_USERNAME/ejercicio1/data2/2017-01-27
hdfs dfs -put /home/$PARAM_USERNAME/file.txt /user/$PARAM_USERNAME/ejercicio1/data2/2017-01-28