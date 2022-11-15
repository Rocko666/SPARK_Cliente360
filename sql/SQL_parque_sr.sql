---sql 1

set hive.cli.print.header=false;	
	
Drop Table db_temporales.OTC_T_PARQUE_SIN_RECARGA;
create table db_temporales.OTC_T_PARQUE_SIN_RECARGA as
select 
t1.num_telefonico, 
t1.linea_negocio_homologado, 
t3.bonos_combos, 
t3.recargas
from db_reportes.otc_t_360_general t1 
inner join
(select t2.num_telefonico,
case when coalesce(sum(t2.valor_bono_combo_pdv_rec_diario),0) > 0 then 1 else 0 end as bonos_combos,--colocamos 1 a los que en los ultimos 30 dÃ­a hayan generado algun ingreso por bonos y combos 0 a los que no
case when coalesce(sum(t2.ingreso_recargas_dia),0) > 0 then 1 else 0 end as recargas--colocamos 1 a los que en los ultimos 30 dÃ­a hayan generado alguna recarga 0 a los que no
from db_reportes.otc_t_360_ingresos t2
where t2.fecha_proceso>=$fecha_inicio and t2.fecha_proceso<=$FECHAEJE --PERIODO DE LOS ULTIMOS 30 DÃAS
group by t2.num_telefonico) as t3
on t1.num_telefonico=t3.num_telefonico
where 
t1.fecha_proceso =$FECHAEJE --fecha a la cual se obtiene la informaciÃ³n
and t1.codigo_plan not in (select codigo_plan from db_reportes.OTC_PLANES_MOVISTAR_LIBRE) --BIG-99, Nathalie Herrera, 24-06-2021,Se agrega condiciÃ²n que excluye planes movistar libre que se encuentran en la tabla db_reporte.otc_t_planes_movistar_libre
and t1.sub_segmento <> 'PREPAGO BLACK' --BIG-99Nathalie Herrera, 24-06-2021,Se agrega condiciÃ²n que excluye subsegmento 'PREPAGO BLACK'
and t3.bonos_combos =0 and t3.recargas=0 --condiciones de 0 recargas bonos y combos
and t1.es_parque ='SI' --parque activo
and t1.linea_negocio_homologado ='PREPAGO' --solo prepago
--and t1.fecha_alta <= '$fini' --GARANTIZAR LA ANTIGUEDAD DE LAS LÃNEAS (LÃNEAS CON FECHAS POSTERIORES A ESTA FECHA NO TENDRÃAN ANTIGÃœEDAD DE 30 DÃAS POR LO QUE NO SE CONSIDERAN)
;