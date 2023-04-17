-- Actualizar metadata
invalidate metadata;

-- Reporte 1
-- Cantidad de transacciones totales
select 
	count(*) 
from
	MAIN_SMART_PROYECTO_4.TRANSACCION_ENRIQUECIDA;

-- Reporte 2
-- Cantidad de transacciones por empresa
select 
	nombre_empresa, 
	count(*) 
from 
	MAIN_SMART_PROYECTO_4.TRANSACCION_ENRIQUECIDA 
group by 
	nombre_empresa 
order by 
	count(*) desc;

-- Reporte 3
-- Suma de montos de transacciones por empresa
select 
	nombre_empresa, 
	sum(cast(monto as int)) 
from 
	MAIN_SMART_PROYECTO_4.TRANSACCION_ENRIQUECIDA 
group by 
	nombre_empresa 
order by 
	sum(cast(monto as int)) desc;

-- Reporte 4
-- Cantidad de transacciones por persona
-- Top 10 de personas que realizan más transacciones
select 
	nombre_persona, 
	count(*) 
from 
	MAIN_SMART_PROYECTO_4.TRANSACCION_ENRIQUECIDA 
group by 
	nombre_persona 
order by 
	count(*) desc 
limit 
	10;

-- Reporte 5
-- Suma de montos de transacciones por persona
-- Top 10 de personas que más gastan
select 
	nombre_persona, 
	sum(cast(monto as int)) 
from 
	MAIN_SMART_PROYECTO_4.TRANSACCION_ENRIQUECIDA 
group by 
	nombre_persona 
order by 
	sum(cast(monto as int)) desc 
limit 
	10;