--sql 1
set hive.cli.print.header=false ;
drop table db_reportes.otc_t_360_ubicacion_tmp;
create table db_reportes.otc_t_360_ubicacion_tmp as 
select a.id_cliente as telefono
,a.franja_horaria 
,a.id_celda
,a.lat_celda
,a.lon_celda
,a.dpa_sector
,a.cod_sector
,a.dpa_zona
,a.cod_zona
,a.dpa_parroquia
,a.cod_parroquia
,a.dpa_canton
,a.cod_canton
,a.dpa_provincia
,a.cod_provincia
,a.dpa_zipcode
,a.cod_zipcode
,a.fecha_proceso
from db_ipaccess.mksharevozdatos_90 a 
inner join (SELECT max(fecha_proceso) max_fecha FROM db_ipaccess.mksharevozdatos_90 where fecha_proceso <= '$FECHAEJE') fm on fm.max_fecha = a.fecha_proceso
where a.franja_horaria='GLOBAL';


--sql 2
set hive.cli.print.header=false ; 
ALTER TABLE db_reportes.otc_t_360_ubicacion DROP IF EXISTS PARTITION(fecha_proceso=$FECHAEJE);
;

--sql 3
set hive.cli.print.header=false ; 
insert into db_reportes.otc_t_360_ubicacion partition(fecha_proceso)
select 
telefono
,a.franja_horaria 
,a.id_celda
,a.lat_celda
,a.lon_celda
,a.dpa_sector
,a.cod_sector
,a.dpa_zona
,a.cod_zona
,a.dpa_parroquia
,a.cod_parroquia
,a.dpa_canton
,a.cod_canton
,a.dpa_provincia
,a.cod_provincia
,a.dpa_zipcode
,a.cod_zipcode
,cast('$FECHAEJE' as bigint) as fecha_proceso
from db_reportes.otc_t_360_ubicacion_tmp a;

--sql 4

set hive.cli.print.header=false ; 
			drop table db_reportes.otc_t_360_ubicacion_tmp;