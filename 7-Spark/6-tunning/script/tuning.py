###
 # @section Separación de recursos
 ##

#De manera similar a Hive, debemos tunear nuestro código SPARK indicando al menos:
#
# - spark.dynamicAllocation.maxExecutors: Máximo número de executors que nuestra aplicación puede solicitar a YARN
# - spark.executor.cores: Número de CPUs por executor, generalmente se le coloca 2.
# - spark.executor.memory: Cantidad de memoria RAM separada para cada executor, se le coloca como mínimo la suma total del peso de todos los archivos con los que se trabaja
# - spark.executor.memoryOverhead: Cantidad de memoria RAM adicional por si se llenase el "spark.executor.memory", generalmente se coloca el 10% de "spark.executor.memory" o 1gb
# - spark.driver.memory: Cantidad de memoria RAM asignada al driver, generalmente se le coloca 1gb
# - spark.driver.memoryOverhead: Cantidad de memoria RAM adicional por si se llenase el "spark.driver.memory", generalmente se coloca 1gb

#Instanciamos "spark" con el tuning
spark = SparkSession.builder.\
appName("Nombre de mi aplicacion").\
config("spark.driver.memory", "1g").\
config("spark.driver.memoryOverhead", "1g").\
config("spark.dynamicAllocation.maxExecutors", "10").\
config("spark.executor.cores", "2").\
config("spark.executor.memory", "2g").\
config("spark.executor.memoryOverhead", "1g").\
enableHiveSupport().\
getOrCreate()