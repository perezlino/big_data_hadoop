-- Activamos la compresión y el formato SNAPPY
SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;

-- Activamos el particionamiento dinámico
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Insertamos los datos
INSERT OVERWRITE TABLE main_UNIVERSAL.TRANSACCION_ENRIQUECIDA
PARTITION (FECHA_TRANSACCION)
SELECT
T.ID_PERSONA,
P.NOMBRE,
P.EDAD,
P.SALARIO,
E2.NOMBRE,
T.MONTO,
E1.NOMBRE,
T.FECHA FECHA_TRANSACCION
FROM
main_UNIVERSAL.TRANSACCION T
JOIN main_UNIVERSAL.PERSONA P ON T.ID_PERSONA = P.ID
JOIN main_UNIVERSAL.EMPRESA E1 ON T.ID_EMPRESA = E1.ID
JOIN main_UNIVERSAL.EMPRESA E2 ON P.ID_EMPRESA = E2.ID;

-- Mostramos los datos
SELECT * FROM main_UNIVERSAL.TRANSACCION LIMIT 10;