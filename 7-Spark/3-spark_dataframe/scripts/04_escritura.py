###
 # @section Guardando el resultado
 ##

#Guardándolo sobre HDFS (Bajando de RAM a Disco duro)
dfJoin.write.\
mode("overwrite").\
format("parquet").\
option("compression", "snappy").\
save("/user/main/tmp/dfJoin")

#Guardándolo sobre un partición de una tabla Hive
dfJoin.write.\
mode("overwrite").\
format("parquet").\
option("compression", "snappy").\
save("/user/main/db/TABLA_JOIN/TABLA_JOIN")

###
 # @section Leyendo un resultado guardado
 ##

dfJoinRead = spark.read.\
format("parquet").\
load("/user/main/tmp/dfJoin")

dfJoinRead.show()