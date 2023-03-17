--- SQL 1
set hive.cli.print.header=false;	
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set tez.queue.name=$COLA_EJECUCION;

drop table $ESQUEMA_TEMP.tmp_360_alta_tmp$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_transfer_in_pp_tmp$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_transfer_in_pos_tmp$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_upsell_tmp$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_downsell_tmp$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_misma_tarifa_tmp$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_bajas_invo$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_otc_t_360_churn90_tmp$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_otc_t_parque_act$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_baja_tmp$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_parque_inactivo$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_otc_t_360_churn90_tmp1$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_otc_t_parque_inac$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_otc_t_parque_inact$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_OTC_T_TEMP_BANCO_CLIENTE360_TMP$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_1$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_otc_t_360_parque_1_tmp$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_motivos_suspension$ABREVIATURA_TEMP;
drop table $ESQUEMA_TEMP.tmp_360_base_preactivos$ABREVIATURA_TEMP;

--SE OBTIENEN LAS ALTAS DESDE EL INICIO DEL MES HASTA LA FECHA DE PROCESO	
create table $ESQUEMA_TEMP.tmp_360_alta_tmp$ABREVIATURA_TEMP as		
select a.telefono,a.numero_abonado,a.fecha_alta
from db_cs_altas.otc_t_altas_bi a	 
where a.p_fecha_proceso = $fecha_proc
and a.marca='TELEFONICA';

--SE OBTIENEN LAS TRANSFERENCIAS POS A PRE DESDE EL INICIO DEL MES HASTA LA FECHA DE PROCESO
create table $ESQUEMA_TEMP.tmp_360_transfer_in_pp_tmp$ABREVIATURA_TEMP as		
select a.telefono,a.fecha_transferencia
from db_cs_altas.otc_t_transfer_out_bi a
where a.p_fecha_proceso = $fecha_proc;

--SE OBTIENEN LAS TRANSFERENCIAS PRE A POS DESDE EL INICIO DEL MES HASTA LA FECHA DE PROCESO
create table $ESQUEMA_TEMP.tmp_360_transfer_in_pos_tmp$ABREVIATURA_TEMP as		
select a.telefono,a.fecha_transferencia
from db_cs_altas.otc_t_transfer_in_bi a	 
where a.p_fecha_proceso = $fecha_proc;

--SE OBTIENEN LOS CAMBIOS DE PLAN DE TIPO UPSELL
create table $ESQUEMA_TEMP.tmp_360_upsell_tmp$ABREVIATURA_TEMP as
select a.telefono,a.fecha_cambio_plan 
from db_cs_altas.otc_t_cambio_plan_bi a
where UPPER(A.tipo_movimiento)='UPSELL' AND 
a.p_fecha_proceso = $fecha_proc;

--SE OBTIENEN LOS CAMBIOS DE PLAN DE TIPO DOWNSELL
create table $ESQUEMA_TEMP.tmp_360_downsell_tmp$ABREVIATURA_TEMP as
select a.telefono,a.fecha_cambio_plan
from db_cs_altas.otc_t_cambio_plan_bi a
where UPPER(A.tipo_movimiento)='DOWNSELL' AND
a.p_fecha_proceso = $fecha_proc;

--SE OBTIENEN LOS CAMBIOS DE PLAN DE TIPO CROSSELL
create table $ESQUEMA_TEMP.tmp_360_misma_tarifa_tmp$ABREVIATURA_TEMP as
select a.telefono,a.fecha_cambio_plan 
from db_cs_altas.otc_t_cambio_plan_bi a
where UPPER(A.tipo_movimiento)='MISMA_TARIFA' AND 
a.p_fecha_proceso = $fecha_proc;

--SE OBTIENEN LAS BAJAS INVOLUNTARIAS, EN EL PERIODO DEL MES
drop table $ESQUEMA_TEMP.tmp_360_bajas_invo$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.tmp_360_bajas_invo$ABREVIATURA_TEMP as
select a.num_telefonico as telefono,a.fecha_proceso, count(1) as conteo
from db_cs_altas.OTC_T_BAJAS_INVOLUNTARIAS a
where a.proces_date between $fechaIniMes and '$FECHAEJE'
and a.marca='TELEFONICA'
group by a.num_telefonico,a.fecha_proceso;

--SE OBTIENEN EL PARQUE PREPAGO, DE ACUERDO A LA M?IMA FECHA DE CHURN MENOR A LA FECHA DE EJECUCI?
drop table $ESQUEMA_TEMP.tmp_360_otc_t_360_churn90_ori$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.tmp_360_otc_t_360_churn90_ori$ABREVIATURA_TEMP as
SELECT PHONE_ID num_telefonico,COUNTED_DAYS 
FROM db_cs_altas.OTC_T_CHURN_SP2 a
inner join (SELECT max(PROCES_DATE) PROCES_DATE FROM db_cs_altas.OTC_T_CHURN_SP2 where PROCES_DATE>$fechamenos5 AND PROCES_DATE < $fechamas1) b 
on a.PROCES_DATE = b.PROCES_DATE
where a.marca='TELEFONICA'
group by PHONE_ID,COUNTED_DAYS;

--SE OBTIENE POR CUENTA DE FACTURACI? EN BANCO ATADO
create table $ESQUEMA_TEMP.tmp_360_OTC_T_TEMP_BANCO_CLIENTE360_TMP$ABREVIATURA_TEMP as
select x.CTA_FACTURACION,
x.CLIENTE_FECHA_ALTA, 
x.BANCO_EMISOR 
from (SELECT 
		a.CTA_FACTURACION,
		A.CLIENTE_FECHA_ALTA,
		row_number() over (partition by A.CTA_FACTURACION order by A.CTA_FACTURACION, A.CLIENTE_FECHA_ALTA DESC) as rownum,
		B.MANDATE_ATTR_1 AS BANCO_EMISOR
		FROM db_rbm.otc_t_VW_CTA_FACTURACION A,db_rbm.otc_t_PRMANDATE B
		WHERE A.CTA_FACTURACION = B.ACCOUNT_NUM
		and to_date(b.active_from_dat)<='$fechaeje1') as x 
where rownum=1;

create table $ESQUEMA_TEMP.tmp_360_baja_tmp$ABREVIATURA_TEMP as		
select a.telefono,a.fecha_baja
from db_cs_altas.otc_t_bajas_bi a	 
where a.p_fecha_proceso = $fecha_proc
and a.marca='TELEFONICA';

create table $ESQUEMA_TEMP.tmp_360_parque_inactivo$ABREVIATURA_TEMP as
select telefono from $ESQUEMA_TEMP.tmp_360_baja_tmp$ABREVIATURA_TEMP
union all
select telefono from $ESQUEMA_TEMP.tmp_360_transfer_in_pp_tmp$ABREVIATURA_TEMP
union all
select telefono from $ESQUEMA_TEMP.tmp_360_transfer_in_pos_tmp$ABREVIATURA_TEMP;

create table $ESQUEMA_TEMP.tmp_360_otc_t_360_churn90_tmp1$ABREVIATURA_TEMP as
SELECT PHONE_ID num_telefonico,COUNTED_DAYS 
FROM db_cs_altas.OTC_T_CHURN_SP2 a 
where PROCES_DATE='$fecha_inac_1'
and a.marca='TELEFONICA'
group by PHONE_ID,COUNTED_DAYS ;


