----- sql 1
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_PERIODO;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_1;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_1;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_1;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_PERIODO_1;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_FINAL;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_UNICOS;
DROP TABLE $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_MENOS30;
DROP TABLE $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_BONO_MENOS30;
DROP TABLE $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_COMBO_MENOS30;


----sql 2
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO;

create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO as
select numero_telefono
, case when operadora='MOVISTAR' or operadora is null or operadora ='' then 'TELEFONICA' else operadora end marca -- si 'MOVISTAR O NULLA O BLANCO' ENTONCES 'TELEFONICA', SINO PONER LA OPERADORA
, fecha_proceso --cada dia  del rango
, sum(valor_recarga_base)/1.12 valor_recargas --retitar el IVA
, count(1) cantidad_recargas
from db_cs_recargas.otc_t_cs_detalle_recargas a
inner join db_altamira.par_origen_recarga ori  -- usar el cat?logo de recargas v?lidas
on ori.ORIGENRECARGAID= a.origen_recarga_aa
where (fecha_proceso >= $fechaIni_menos_3meses AND fecha_proceso <= $fecha_eje2)
AND operadora in ('MOVISTAR')
AND TIPO_TRANSACCION = 'ACTIVA' --transacciones validas
AND ESTADO_RECARGA = 'RECARGA' --asegurar que son recargas
AND rec_pkt ='REC' group by numero_telefono
, case when operadora='MOVISTAR' or operadora is null or operadora ='' then 'TELEFONICA' else operadora end   -- si 'MOVISTAR O NULLA O BLANCO' ENTONCES 'TELEFONICA', SINO PONER LA OPERADORA
, fecha_proceso;

--bonos y combos
-------------------------------------------------
--(KV 303551) se modifica el rango de fecha para obtener bonos y combos de 2 meses atras
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM as
select  fecha_proceso,r.numero_telefono as num_telefono,
case when r.operadora='MOVISTAR' then 'TELEFONICA' else r.operadora end as marca
,b.tipo as combo_bono
,SUM(r.valor_recarga_base)/1.12 coste--Para quitar el valor del impuesto
,count(*) cantidad --combos o bonos seg?n el tipo de la tabla db_reportes.cat_bonos_pdv , hacer el case correspondiente
,$fecha_eje2 as fecha_proc ------- parametro del ultimo dia del rango
from db_cs_recargas.otc_T_cs_detalle_recargas r
inner join (select distinct codigo_pm, tipo from db_reportes.cat_bonos_pdv ) b --INNER join db_reportes.cat_bonos_pdv b
on (b.codigo_pm=r.codigo_paquete
and (r.codigo_paquete<>''
and r.codigo_paquete is not null))
-- solo los que se venden en PDV
where fecha_proceso>=$fechaIni_menos_2meses --
and fecha_proceso<=$fecha_eje2  --(di  a n)
and r.rec_pkt='PKT' -- solo los que se venden en PDV
and plataforma in ('PM')
AND TIPO_TRANSACCION = 'ACTIVA'
AND ESTADO_RECARGA = 'RECARGA'
AND r.operadora='MOVISTAR'group by fecha_proceso, r.numero_telefono
,case when r.operadora='MOVISTAR' then 'TELEFONICA' else r.operadora end
,b.tipo;


drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS AS
select b.numero_telefono
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO b 
union all 
select c.num_telefono
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM c;

drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_UNICOS;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_UNICOS as
select numero_telefono, 
count(1) as cant_t 
from $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS 
group by numero_telefono;

--mes 0
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_0;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_0 as
select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso>= $fecha_inico_mes_1_2 and fecha_proceso <= $fecha_eje2
group by numero_telefono;

--RECARGAS menos 30 dias
-------------------------------------------------
--(KV 303551) se obtienen las recargas de un rango de 30 dias menos a la fecha de ejecucion
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_MENOS30;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_MENOS30 as
select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso>= $fecha_menos30 and fecha_proceso < $fecha_eje2
group by numero_telefono;


--mes menos 1
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_1;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_1 as
select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso>= $fechaIni_menos_2meses and fecha_proceso < $fecha_inico_mes_1_2
group by numero_telefono;

--mes menos 2
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_2;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_2 as
select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso>= $fechaIni_menos_3meses and fecha_proceso < $fechaIni_menos_2meses
group by numero_telefono;

--mes menos 3
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_3;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_3 as
select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso>= $fechaIni_menos_4meses and fecha_proceso < $fechaIni_menos_3meses
group by numero_telefono;

--dia ejecucion
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_1;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_1 as
select numero_telefono, sum(valor_recargas) costo_recargas_dia, sum(cantidad_recargas) cant_recargas_dia
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso= $fecha_eje2
group by numero_telefono;

--BONOS ACUMULADOS DEL MES
-------------------------------------------------
--(KV 303551) se modifica el rango de fecha para obtener bonos y combos del mes
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_BONO;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_BONO as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='BONO' and fecha_proceso>= $fecha_inico_mes_1_2 and fecha_proceso <= $fecha_eje2
group by num_telefono;

--BONOS DEL DIA
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_BONO;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_BONO as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='BONO' AND fecha_proceso=$fecha_eje2
group by num_telefono;


--BONOS menos 30 dias
-------------------------------------------------
--(KV 303551) se obtienen los bonos de un rango de 30 dias menos a la fecha de ejecucion
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_BONO_MENOS30;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_BONO_MENOS30 as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='BONO' AND fecha_proceso>= $fecha_menos30 and fecha_proceso < $fecha_eje2
group by num_telefono;


--COMBOS ACUMULADOS DEL MES
-------------------------------------------------
--(KV 303551) se modifica el rango de fecha para obtener bonos y combos del mes
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_COMBO;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_COMBO as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='COMBO' and fecha_proceso>= $fecha_inico_mes_1_2 and fecha_proceso <= $fecha_eje2
group by num_telefono;

--COMBOS DEL DIA
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_COMBO;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_COMBO as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='COMBO' AND fecha_proceso=$fecha_eje2
group by num_telefono;


--COMBOS menos 30 dias
-------------------------------------------------
--(KV 303551) se obtienen los combos de un rango de 30 dias menos a la fecha de ejecucion
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_COMBO_MENOS30;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_COMBO_MENOS30 as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='COMBO' AND fecha_proceso>= $fecha_menos30 and fecha_proceso < $fecha_eje2
group by num_telefono;



--CONSOLIDACION DE TODOS LOS VALORES OBTENIDOS
-------------------------------------------------
--(KV 303551) modificacion se agregan los campos ingreso_recargas_30,cantidad_recargas_30, ingreso_bonos_30,cantidad_bonos_30,
--ingreso_combos_30,cantidad_combos_30 para obtener PARQUE_RECARGADOR_30_DIAS
-------------------------------------------------

drop table $ESQUEMA_TEMP.TMP_OTC_T_360_RECARGAS;
create table $ESQUEMA_TEMP.TMP_OTC_T_360_RECARGAS as
select a.numero_telefono
,coalesce(c.costo_recargas_dia,0) ingreso_recargas_dia
,coalesce(c.cant_recargas_dia,0) cantidad_recarga_dia
,coalesce(b.costo_recargas_acum,0) ingreso_recargas_m0
,coalesce(b.cant_recargas_acum,0) cantidad_recargas_m0
,coalesce(b1.costo_recargas_acum,0) ingreso_recargas_m1
,coalesce(b1.cant_recargas_acum,0) cantidad_recargas_m1
,coalesce(b2.costo_recargas_acum,0) ingreso_recargas_m2
,coalesce(b2.cant_recargas_acum,0) cantidad_recargas_m2
,coalesce(b3.costo_recargas_acum,0) ingreso_recargas_m3
,coalesce(b3.cant_recargas_acum,0) cantidad_recargas_m3
,coalesce(d.coste_paym_periodo,0) ingreso_bonos
,coalesce(d.cant_paym_periodo,0) cantidad_bonos
,coalesce(f.coste_paym_periodo,0) ingreso_combos
,coalesce(f.cant_paym_periodo,0) cantidad_combos
,coalesce(g.coste_paym_periodo,0) ingreso_bonos_dia
,coalesce(g.cant_paym_periodo,0) cantidad_bonos_dia
,coalesce(h.coste_paym_periodo,0) ingreso_combos_dia
,coalesce(h.cant_paym_periodo,0) cantidad_combos_dia
,coalesce(i.costo_recargas_acum,0) ingreso_recargas_30
,coalesce(i.cant_recargas_acum,0) cantidad_recargas_30
,coalesce(j.coste_paym_periodo,0) ingreso_bonos_30
,coalesce(j.cant_paym_periodo,0) cantidad_bonos_30
,coalesce(k.coste_paym_periodo,0) ingreso_combos_30
,coalesce(k.cant_paym_periodo,0) cantidad_combos_30
from $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_UNICOS a
left join $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_1 c 
on a.numero_telefono=c.numero_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_0 b
on a.numero_telefono=b.numero_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_1 b1
on a.numero_telefono=b1.numero_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_2 b2
on a.numero_telefono=b2.numero_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_3 b3
on a.numero_telefono=b3.numero_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_BONO d
on a.numero_telefono=d.num_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_COMBO f
on a.numero_telefono=f.num_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_BONO g
on a.numero_telefono=g.num_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_COMBO h
on a.numero_telefono=h.num_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_MENOS30 i
on a.numero_telefono=i.numero_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_BONO_MENOS30 j
on a.numero_telefono=j.num_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_COMBO_MENOS30 k
on a.numero_telefono=k.num_telefono;


--sql3
set hive.cli.print.header=false;	
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;

--(KV 303551) se agregan las temporales TMP_360_OTC_T_RECARGAS_ACUM_MENOS30,TMP_360_OTC_T_PAQUETES_BONO_MENOS30 y TMP_360_OTC_T_PAQUETES_PAYMENT_COMBO_MENOS30
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_PERIODO;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_1;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_1;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_1;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_PERIODO_1;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_UNICOS;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_MENOS30;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_BONO_MENOS30;
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_COMBO_MENOS30;