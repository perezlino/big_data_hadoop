###
 # @section Lectura de datos
 ##

#Leer desde SPARK SQL
df1=spark.sql("SELECT * FROM main_universal.persona")

#Mostramos la data
df1.show()

#Proceso en SPARK SQL
df2=spark.sql("SELECT * FROM main_universal.persona WHERE edad > 40")

#Mostramos la data
df2.show()