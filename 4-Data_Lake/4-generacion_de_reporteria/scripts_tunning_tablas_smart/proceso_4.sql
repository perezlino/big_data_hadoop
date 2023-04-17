--
-- @author ALONSO RAÚL MELGAREJO GALVÁN
-- 
-- @email alonsoraulmgs@gmail.com
-- @linkedin https://www.linkedin.com/in/alonsoraulmg
-- @facebook https://www.facebook.com/alonsoraulmg
-- @copyright Big Data Academy
-- 
-- Proceso 4
--

-- 
-- @section Tuning
-- 

SET hive.execution.engine=mr;
SET mapreduce.job.maps=8;
SET mapreduce.input.fileinputformat.split.maxsize = 128000000;
SET mapreduce.input.fileinputformat.split.minsize = 128000000;
SET mapreduce.map.cpu.vcores=2;
SET mapreduce.map.memory.mb=128;
SET mapreduce.job.reduces=8;
SET mapreduce.reduce.cpu.vcores=2;
SET mapreduce.reduce.memory.mb=128;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=9999;
SET hive.exec.max.dynamic.partitions.pernode=9999;


--
-- @section Tablas temporales
--

-- No hay

--
-- @section Programa
--

INSERT OVERWRITE TABLE ${hiveconf:PARAM_USERNAME}_SMART.TRANSACCION_POR_EMPRESA
SELECT
    EMPRESA_TRANSACCION,
    COUNT(1),
    SUM(MONTO_TRANSACCION)
FROM 
    ${hiveconf:PARAM_USERNAME}_UNIVERSAL.TRANSACCION_ENRIQUECIDA
GROUP BY
    EMPRESA_TRANSACCION
ORDER BY
    EMPRESA_TRANSACCION;

SELECT * FROM ${hiveconf:PARAM_USERNAME}_SMART.TRANSACCION_POR_EMPRESA LIMIT 10;