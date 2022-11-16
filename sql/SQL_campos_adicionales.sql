--- sql 1
set hive.cli.print.header=false;	
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;


--SE OBTIENEN LOS MOTIVOS DE SUSPENSI?, POSTERIORMENTE ESTA TEMPORAL ES USADA EN EL PROCESO OTC_T_360_GENARAL.sh
drop TABLE $ESQUEMA_TEMP.tmp_360_motivos_suspension;
CREATE TABLE $ESQUEMA_TEMP.tmp_360_motivos_suspension as
SELECT NUM.NAME, D.NAME AS MOTIVO_SUSPENSION,D.SUSP_CODE_ID
FROM db_rdb.OTC_T_R_BOE_BSNS_PROD_INST A
INNER JOIN db_rdb.OTC_T_R_BOE_BSNS_PROD_INST B
ON (A.TOP_BPI = B.OBJECT_ID)
LEFT JOIN db_rdb.OTC_T_R_RI_MOBILE_PHONE_NUMBER NUM
ON (NUM.OBJECT_ID = A.PHONE_NUMBER)
INNER JOIN db_rdb.OTC_T_R_BOE_BSNS_PROD_INST_SUSP_RSN C
ON (A.OBJECT_ID = C.OBJECT_ID)
INNER JOIN db_rdb.OTC_T_R_PIM_STATUS_CHANGE D
ON (C.VALUE=D.OBJECT_ID)
WHERE A.PROD_INST_STATUS in ('9132639016013293421','9126143611313472393')
AND A.ACTUAL_END_DATE IS NULL
AND B.ACTUAL_END_DATE IS NULL
AND A.OBJECT_ID = B.OBJECT_ID
AND cast(A.modified_when as date) <= '$fechamas1_2'
ORDER BY NUM.NAME;

--SI, PERO MOVER A CAMPOS ADICIONALES
drop table $ESQUEMA_TEMP.otc_t_360_susp_cobranza;
create table $ESQUEMA_TEMP.otc_t_360_susp_cobranza as
select t2.* 
from
(select t1.*,
row_number() over (partition by t1.name order by t1.name, t1.orden_susp DESC) as orden
from (
select 
case 
when motivo_suspension= 'Por Cobranzas (bi-direccional)' then 3 --3
when motivo_suspension='Por Cobranzas (uni-direccional)' then 1 --2
when motivo_suspension like 'Suspensi%facturaci%'then 2 --1
end as orden_susp,
a.*
from $ESQUEMA_TEMP.tmp_360_motivos_suspension a
where (motivo_suspension in 
('Por Cobranzas (uni-direccional)',
'Suspensión por facturación',
'Por Cobranzas (bi-direccional)')
or motivo_suspension like 'Suspensi%facturaci%')
and a.name is not null and a.name <>'') as t1) as t2
where t2.orden=1;

--SI, PERO MOVER A CAMPOS ADICIONALES
drop table $ESQUEMA_TEMP.tmp_360_otras_suspensiones;
create table $ESQUEMA_TEMP.tmp_360_otras_suspensiones as
select 
a.name,
case when b.name is not null or c.name is not null then 'Abuso 911' else '' end as susp_911,
case when d.name is not null then d.motivo_suspension else '' end as susp_cobranza_puntual,
case when e.name is not null then e.motivo_suspension else '' end as susp_fraude,
case when f.name is not null then f.motivo_suspension else '' end as susp_robo,
case when g.name is not null then g.motivo_suspension else '' end as susp_voluntaria
from $ESQUEMA_TEMP.tmp_360_motivos_suspension a
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension b
on (a.name=b.name and (b.motivo_suspension like 'Abuso 911 - 180 d%'))
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension c
on (a.name=c.name and (c.motivo_suspension like 'Abuso 911 - 30 d%'))
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension d
on (a.name=d.name and d.motivo_suspension ='Cobranza puntual')
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension e
on (a.name=e.name and e.motivo_suspension ='Fraude')
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension f
on (a.name=f.name and f.motivo_suspension ='Robo')
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension g
on (a.name=g.name and g.motivo_suspension ='Voluntaria')
where (a.motivo_suspension in ('Abuso 911 - 180 días',
'Abuso 911 - 30 días',
'Cobranza puntual',
'Fraude',
'Robo',
'Voluntaria')
or a.motivo_suspension like 'Abuso 911 - 180 d%'
or a.motivo_suspension like 'Abuso 911 - 30 d%')
and a.name is not null and a.name <>'';	

--FECHA ALTA DE LA CUENTA
		--SI, ESTA FECHA ES LA MAXIMA QUE EXISTA, SE DEBE AÑADIR SUS DEPENDENCIAS, MISMAS QUE ESTAS A LAS 2AM
		drop table $ESQUEMA_TEMP.otc_t_360_cuenta_fecha;
		create table $ESQUEMA_TEMP.otc_t_360_cuenta_fecha as
		SELECT
		cast(A.ACTUAL_START_DATE as date) as SUSCRIPTOR_ACTUAL_START_DATE,
		ACCT.BILLING_ACCT_NUMBER as CTA_FACT
		FROM db_rdb.otc_t_R_BOE_BSNS_PROD_INST A
		INNER JOIN db_rdb.otc_t_R_CBM_BILLING_ACCT ACCT
		ON A.BILLING_ACCOUNT = ACCT.OBJECT_ID;

		--SI
		DROP TABLE $ESQUEMA_TEMP.otc_t_cuenta_num_tmp;
		CREATE TABLE $ESQUEMA_TEMP.otc_t_cuenta_num_tmp AS 
		SELECT Fecha_Alta_Cuenta,CTA_FACT
		from (
		SELECT
		SUSCRIPTOR_ACTUAL_START_DATE as Fecha_Alta_Cuenta,
		CTA_FACT,
		row_number() over (partition by CTA_FACT order by CTA_FACT, SUSCRIPTOR_ACTUAL_START_DATE) as orden
		FROM $ESQUEMA_TEMP.otc_t_360_cuenta_fecha) FF 
		WHERE orden=1;


--SE OBTIENE LAS RENOVACIONES DE TERMINALES A PARTIR DE LA FECHA DE LA SALIDA JANUS
drop table db_temporales.tmp_360_ultima_renovacion;
create table db_temporales.tmp_360_ultima_renovacion as
select t1.* FROM 
(SELECT a.p_fecha_factura as fecha_renovacion, 
a.TELEFONO, 
a.identificacion_cliente,
a.MOVIMIENTO,
row_number() over (partition by a.TELEFONO order by a.TELEFONO,a.p_fecha_factura desc) as orden
FROM db_cs_terminales.otc_t_terminales_simcards a where 
(a.p_fecha_factura >= 20171015 and a.p_fecha_factura <= $FECHAEJE )
and a.clasificacion = 'TERMINALES'
AND a.modelo_terminal NOT IN ('DIFERENCIA DE EQUIPOS','FINANCIAMIENTO')
and a.codigo_tipo_documento <> 25
AND a.MOVIMIENTO LIKE '%RENOVAC%N%') as t1
where t1.orden=1;

--SE OBTIENE LAS RENOVACIONES DE TERMINALES ANTES DE LA FECHA DE LA SALIDA JANUS
drop table db_temporales.tmp_360_ultima_renovacion_scl;
create table db_temporales.tmp_360_ultima_renovacion_scl as
select t2.* from (
SELECT
t1.FECHA_FACTURA as fecha_renovacion, 
t1.MIN as TELEFONO, 
t1.cedula_ruc_cliente as identificacion_cliente,
t1.MOVIMIENTO,
row_number() over (partition by t1.MIN order by t1.MIN,t1.FECHA_FACTURA desc) as orden
FROM db_cs_terminales.otc_t_facturacion_terminales_scl t1
where t1.CLASIFICACION_ARTICULO LIKE '%TERMINALES%' AND t1.MOVIMIENTO LIKE '%RENOVAC%N%' AND T1.codigo_tipo_documento <> 25) as t2
where t2.orden=1;

--SE CONSOLIDAN LAS DOS FUENTES, QUEDANDONOS CON LA ULTIMA RENOVACI? POR L?EA MOVIL
drop table db_temporales.tmp_360_ultima_renovacion_end;
CREATE TABLE db_temporales.tmp_360_ultima_renovacion_end as
select t2.* from
(SELECT t1.telefono,
t1.identificacion_cliente,
t1.fecha_renovacion,
row_number() over (partition by t1.telefono order by t1.telefono,t1.fecha_renovacion desc) as orden
FROM
(select TELEFONO,
identificacion_cliente,
cast(date_format(from_unixtime(unix_timestamp(cast(fecha_renovacion as string),'yyyyMMdd')),'yyyy-MM-dd') as date) as fecha_renovacion
from db_temporales.tmp_360_ultima_renovacion
where telefono is not null
union all
select cast(TELEFONO as string) as TELEFONO,
identificacion_cliente,
fecha_renovacion 
FROM db_temporales.tmp_360_ultima_renovacion_scl 
where telefono is not null) AS T1) as t2
where t2.orden=1;

--SE OBTIEN LA DIRECCIONES POR CLIENTE
drop table db_temporales.tmp_360_adress_ord;
create table db_temporales.tmp_360_adress_ord as
SELECT               
a.CUSTOMER_REF,
A.ADDRESS_SEQ,
A.ADDRESS_1,
A.ADDRESS_2,
A.ADDRESS_3,
A.ADDRESS_4 
from db_rbm.otc_t_ADDRESS a,
(SELECT                
b.CUSTOMER_REF,
max(b.ADDRESS_SEQ) as MAX_ADDRESS_SEQ
from db_rbm.otc_t_ADDRESS b
GROUP BY b.CUSTOMER_REF) as c
where a.CUSTOMER_REF=c.CUSTOMER_REF and A.ADDRESS_SEQ=c.MAX_ADDRESS_SEQ;

--SE ASIGNAN A LAS CUENTAS DE FACTURACI? LAS DIRECCIONES
drop table db_temporales.tmp_360_account_address;
create table db_temporales.tmp_360_account_address as
select a.ACCOUNT_NUM,
b.ADDRESS_2,
b.ADDRESS_3,
b.ADDRESS_4
from db_rbm.otc_t_ACCOUNT as a, db_temporales.tmp_360_adress_ord as b
where a.CUSTOMER_REF=b.CUSTOMER_REF;
		
--SE OBTIENE LA VIGENCIA DE CONTRATO
drop table db_temporales.tmp_360_vigencia_contrato;
create table db_temporales.tmp_360_vigencia_contrato as
SELECT 
H.NAME NUM_TELEFONICO,
A.VALID_FROM,
A.VALID_UNTIL,
A.INITIAL_TERM,
F.MODIFIED_WHEN IMEI_FEC_MODIFICACION,
cast(C.ACTUAL_START_DATE as date) SUSCRIPTOR_ACTUAL_START_DATE,
case when (F.MODIFIED_WHEN is null or F.MODIFIED_WHEN='') then cast(C.ACTUAL_START_DATE as date) else F.MODIFIED_WHEN end as FECHA_FIN_CONTRATO
FROM db_rdb.otc_t_R_CNTM_CONTRACT_ITEM A 
INNER JOIN db_rdb.otc_t_R_CNTM_COM_AGRM B
ON (A.PARENT_ID = B.OBJECT_ID) 
INNER JOIN db_rdb.otc_t_R_BOE_BSNS_PROD_INST C
ON (A.BSNS_PROD_INST = C.OBJECT_ID )
INNER JOIN db_rdb.otc_t_R_RI_MOBILE_PHONE_NUMBER H
ON (C.PHONE_NUMBER = H.OBJECT_ID)
LEFT JOIN db_rdb.otc_t_R_AM_CPE F 
ON (C.IMEI = F.OBJECT_ID)
AND cast(C.ACTUAL_START_DATE as date) <= '$fechamas1_2';

--NOS QUEDAMOS CON LA ?TIMA VIGENCIA DE CONTRATO
drop table db_temporales.tmp_360_vigencia_contrato_unicos;
create table db_temporales.tmp_360_vigencia_contrato_unicos as
select * from 
(select NUM_TELEFONICO,
VALID_FROM,
VALID_UNTIL,
INITIAL_TERM,
IMEI_FEC_MODIFICACION,
SUSCRIPTOR_ACTUAL_START_DATE,
FECHA_FIN_CONTRATO,
row_number() over (partition by NUM_TELEFONICO order by FECHA_FIN_CONTRATO desc) as id
from db_temporales.tmp_360_vigencia_contrato) as t1
where t1.id=1;

--SE OBTIENEN UN CATALOGO DE PLANES CON LA VIGENCIAS
drop table  db_temporales.tmp_360_PLANES_JANUS;
create table db_temporales.tmp_360_PLANES_JANUS as
SELECT  
PO.PROD_CODE,
PO.NAME, 
PO.AVAILABLE_FROM, 
PO.AVAILABLE_TO, 
PO.CREATED_WHEN, 
PO.MODIFIED_WHEN,
A.PROD_OFFERING,
count(1) as cant
FROM db_rdb.otc_t_R_PIM_PRD_OFF PO
INNER JOIN db_rdb.otc_t_R_BOE_BSNS_PROD_INST A
ON A.PROD_OFFERING=PO.OBJECT_ID
WHERE PO.IS_TOP_OFFER = '7777001'
AND PO.PROD_CODE IS NOT NULL
AND A.ACTUAL_END_DATE IS NULL
AND A.ACTUAL_START_DATE IS NOT NULL
GROUP BY PO.PROD_CODE,
PO.NAME, 
PO.AVAILABLE_FROM, 
PO.AVAILABLE_TO, 
PO.CREATED_WHEN, 
PO.MODIFIED_WHEN,
A.PROD_OFFERING
ORDER BY PO.PROD_CODE, PO.AVAILABLE_FROM,PO.AVAILABLE_TO;

--SE ASIGNA UN ID SECUENCIAL, QUE SERA LA VERSI? DEL PLAN, ORDENADO POR CODIGO DE PLAN Y SUS FECHAS DE VIGENCIA
drop table db_temporales.tmp_360_PLANES_JANUS_VERSION;
create table db_temporales.tmp_360_PLANES_JANUS_VERSION as
select *,
row_number() over (partition by PROD_CODE order by AVAILABLE_FROM,AVAILABLE_TO) as VERSION
from db_temporales.tmp_360_PLANES_JANUS;

--DEBIDO A QUE NO SE TIENEN FECHAS CONTINUAS EN LAS VIGENCIAS (ACTUAL_START_DATE Y ACTUAL_END_DATE),  SE REASIGNAN LAS VIGENCIAS PARA QUE TENGAN SECUENCIA EN EL TIEMPO
drop table db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC;
create table db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC AS
select CASE 
		WHEN A.VERSION=1 THEN A.available_from 
		else B.available_to END AS fecha_inicio,
A.AVAILABLE_TO as fecha_fin, A.*,b.VERSION AS ver_b
from db_temporales.tmp_360_PLANES_JANUS_VERSION a
left join db_temporales.tmp_360_PLANES_JANUS_VERSION b
on (a.PROD_CODE=b.PROD_CODE and a.version = b.version +1);

--OBTENEMOS EL CATALOGO SOLO PARA PRIMERA VERSION
DROP table db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC_VER_UNO;
create table db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC_VER_UNO as
select * from db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC
where VERSION=1;

--OBTENEMOS EL CATALOGO SOLO PARA LA ULTIMA VERSION
DROP table db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC_VER_ULTIMA;
create table db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC_VER_ULTIMA as
select *
from
(select *,
row_number() over (partition by PROD_CODE order by version desc) as orden
from db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC) t1
where t1.orden=1;

--OBTENEMOS LOS PLANES QUE POSEE EL ABONADO, ESTO GENERA TODOS LOS PLANES QUE TENGA EL ABONADO A LA FECHA DE EJECUCION
drop table db_temporales.tmp_360_abonado_plan;
create table db_temporales.tmp_360_abonado_plan as
SELECT NUM.NAME AS TELEFONO, 
A.SUBSCRIPTION_REF AS NUM_ABONADO, 
PO.PROD_CODE,
PO.OBJECT_ID AS OBJECT_ID_PLAN, 
PO.NAME AS DESCRIPCION_PAQUETE,
A.ACTUAL_START_DATE AS FECHAINICIO,
A.ACTUAL_END_DATE AS FECHA_DESACTIVACION, 
A.MODIFIED_WHEN
FROM db_rdb.OTC_T_R_PIM_PRD_OFF PO
INNER JOIN db_rdb.OTC_T_R_BOE_BSNS_PROD_INST A
ON (PO.OBJECT_ID = A.PROD_OFFERING)
INNER JOIN db_rdb.OTC_T_R_BOE_BSNS_PROD_INST B
ON (B.OBJECT_ID = A.TOP_BPI)
LEFT JOIN db_rdb.OTC_T_R_RI_MOBILE_PHONE_NUMBER NUM
ON (NUM.OBJECT_ID = B.PHONE_NUMBER)
WHERE A.ACTUAL_END_DATE IS NULL
AND A.OBJECT_ID = B.TOP_BPI
and cast(A.ACTUAL_START_DATE as date) <= '$fechamas1_2';

--NOS QUEDAMOS SOLO CON EL ?TIMO PLAN DEL ABONADO A LA FECHA DE EJECUCION
drop table db_temporales.tmp_360_abonado_plan_unico;
create table db_temporales.tmp_360_abonado_plan_unico as
select b.* from 
(select a.*,
row_number() over (partition by a.telefono order by a.fechainicio desc) as id
from db_temporales.tmp_360_abonado_plan a) as b
where b.id=1;


--SE ASIGNA LA VERIS? POR OBJECT ID, SI NO SE OBTIENE OR OBJECT ID POR LA VERSION MINIMA Y MAXIMA DEL PLAN
drop table db_temporales.tmp_360_vigencia_abonado_plan_prev;
CREATE TABLE db_temporales.tmp_360_vigencia_abonado_plan_prev AS 
SELECT a.NUM_TELEFONICO AS TELEFONO,
VALID_FROM,
VALID_UNTIL,
INITIAL_TERM AS INITIAL_TERM,
CASE WHEN (INITIAL_TERM IS NULL OR initial_term ='0') THEN 18 ELSE CAST(INITIAL_TERM AS INT) END AS INITIAL_TERM_NEW,
IMEI_FEC_MODIFICACION,
SUSCRIPTOR_ACTUAL_START_DATE,
cast(fechainicio as date) as FECHA_ACTIVACION_PLAN_ACTUAL,
CASE WHEN cast(fechainicio as date)  is null AND IMEI_FEC_MODIFICACION is null AND VALID_UNTIL is null THEN SUSCRIPTOR_ACTUAL_START_DATE
WHEN cast(fechainicio as date)  is null AND IMEI_FEC_MODIFICACION is null AND VALID_UNTIL is null THEN VALID_UNTIL
ELSE (CASE 
WHEN cast(fechainicio as date)  > FECHA_FIN_CONTRATO THEN cast(fechainicio as date)  
ELSE FECHA_FIN_CONTRATO END
) END AS FECHA_FIN_CONTRATO,
date_format(from_unixtime(unix_timestamp(cast($FECHAEJE as string),'yyyyMMdd')),'yyyy-MM-dd') AS fecha_hoy,
months_between(date_format(from_unixtime(unix_timestamp(cast($FECHAEJE as string),'yyyyMMdd')),'yyyy-MM-dd'),(CASE WHEN cast(fechainicio as date)  is null AND IMEI_FEC_MODIFICACION is null AND VALID_UNTIL is null THEN SUSCRIPTOR_ACTUAL_START_DATE
WHEN cast(fechainicio as date)  is null AND IMEI_FEC_MODIFICACION is null AND VALID_UNTIL is null THEN VALID_UNTIL
ELSE (CASE 
WHEN cast(fechainicio as date)  > FECHA_FIN_CONTRATO THEN cast(fechainicio as date)  
ELSE FECHA_FIN_CONTRATO END
) END)) AS MESES_DIFERENCIA,
CASE WHEN (C.VERSION IS NULL and (cast(b.fechainicio as date)<d.fecha_inicio or cast(b.fechainicio as date)<e.fecha_inicio)) then 1 else C.VERSION end AS VERSION_PLAN,
b.fechainicio,
cast(b.fechainicio as date) as fechainicio_date,
coalesce(c.fecha_inicio,d.fecha_inicio) as fecha_inicio,
C.VERSION as old,
B.PROD_CODE
FROM db_temporales.tmp_360_vigencia_contrato_unicos AS A
LEFT JOIN db_temporales.tmp_360_abonado_plan_unico AS B
ON (a.num_telefonico = B.telefono)
LEFT JOIN db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC AS C
ON (B.OBJECT_ID_PLAN= C.PROD_OFFERING AND B.PROD_CODE = c.PROD_CODE)
LEFT JOIN db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC_VER_UNO AS D
ON (B.PROD_CODE = D.PROD_CODE)
LEFT JOIN db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC_VER_ULTIMA AS E
ON (B.PROD_CODE = E.PROD_CODE);

--ASIGNACION DE VERSI? POR FECHAS SOLO PARA LOS QUE LA VERSION ES NULLL, ESTO VA CAUSAR DUPLICIDAD EN LOS REGISTROS CUYA VERSI? DE PLAN SEA NULL
drop table db_temporales.tmp_360_vigencia_abonado_plan_dup;
create table db_temporales.tmp_360_vigencia_abonado_plan_dup as
select
b.*,
case when b.VERSION_PLAN is null and B.fechainicio_date BETWEEN c.fecha_inicio and c.fecha_fin then c.version else b.version_plan end as version_plan_new
FROM db_temporales.tmp_360_vigencia_abonado_plan_prev AS B
LEFT JOIN db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC AS C
ON (B.PROD_CODE = c.PROD_CODE and b.VERSION_PLAN IS NULL);

--ELIMINAMOS LOS DUPLICADOS, ORDENANDO POR LA NUEVA VERSI? DE PLAN
drop table db_temporales.tmp_360_vigencia_abonado_plan;
CREATE TABLE db_temporales.tmp_360_vigencia_abonado_plan AS 
select t1.*
from
(select
b.*,
row_number() over(partition by telefono order by version_plan_new desc) as id
FROM db_temporales.tmp_360_vigencia_abonado_plan_dup AS B) as t1
where t1.id=1;

--CALCULAMOS LA FECHA DE FIN DE CONTRATO
drop table db_temporales.tmp_360_vigencia_abonado_plan_def;
CREATE TABLE db_temporales.tmp_360_vigencia_abonado_plan_def as
select a.telefono,
a.valid_from,
a.valid_until,
a.initial_term,
a.initial_term_new,
a.imei_fec_modificacion,
a.suscriptor_actual_start_date,
a.fecha_activacion_plan_actual,
a.fecha_fin_contrato,
a.fecha_hoy,
a.meses_diferencia,
a.version_plan_new as version_plan,
CAST(CEIL(MESES_DIFERENCIA/INITIAL_TERM_NEW) AS INT) AS FACTOR,
ADD_MONTHS(FECHA_FIN_CONTRATO,(CAST(CEIL(MESES_DIFERENCIA/INITIAL_TERM_NEW) AS INT))*INITIAL_TERM_NEW) AS FECHA_FIN_CONTRATO_DEFINITIVO
from db_temporales.tmp_360_vigencia_abonado_plan a;

drop table db_temporales.otc_t_360_parque_camp_ad;
create table db_temporales.otc_t_360_parque_camp_ad as
select t1.* from
(SELECT *,
row_number() over (partition by num_telefonico order by es_parque desc) as id
FROM db_temporales.otc_t_360_parque_1_tmp) as t1
where t1.id=1;

drop table db_temporales.tmp_360_campos_adicionales;
create table db_temporales.tmp_360_campos_adicionales as
select a.num_telefonico as telefono,a.account_num,
b.fecha_renovacion,
c.ADDRESS_2,c.ADDRESS_3,c.ADDRESS_4,
D.FECHA_FIN_CONTRATO_DEFINITIVO,d.initial_term_new AS VIGENCIA_CONTRATO,d.VERSION_PLAN,
d.imei_fec_modificacion as    FECHA_ULTIMA_RENOVACION_JN,
d.fecha_activacion_plan_actual as FECHA_ULTIMO_CAMBIO_PLAN
from db_temporales.otc_t_360_parque_camp_ad a
left join db_temporales.tmp_360_ultima_renovacion_end b
on (a.num_telefonico=b.telefono and a.identificacion_cliente=b.identificacion_cliente)
left join db_temporales.tmp_360_account_address c
on a.account_num =c.account_num
left join db_temporales.tmp_360_vigencia_abonado_plan_def d
on (a.num_telefonico = d.TELEFONO);

DROP TABLE $ESQUEMA_TEMP.otc_t_360_cartera_vencimiento;
CREATE TABLE $ESQUEMA_TEMP.otc_t_360_cartera_vencimiento as
select 
cuenta_facturacion,
case 
when t2.DDIAS_390 IS NOT NULL AND t2.DDIAS_390>=1 then '390'
when t2.DDIAS_360 IS NOT NULL AND t2.DDIAS_360>=1 then '360'
when t2.DDIAS_330 IS NOT NULL AND t2.DDIAS_330>=1 then '330'
when t2.DDIAS_300 IS NOT NULL AND t2.DDIAS_300>=1 then '300'
when t2.DDIAS_270 IS NOT NULL AND t2.DDIAS_270>=1 then '270'
when t2.DDIAS_240 IS NOT NULL AND t2.DDIAS_240>=1 then '240'
when t2.DDIAS_210 IS NOT NULL AND t2.DDIAS_210>=1 then '210'
when t2.DDIAS_180 IS NOT NULL AND t2.DDIAS_180>=1 then '180'
when t2.DDIAS_150 IS NOT NULL AND t2.DDIAS_150>=1 then '150'
when t2.DDIAS_120 IS NOT NULL AND t2.DDIAS_120>=1 then '120'
when t2.DDIAS_90 IS NOT NULL AND t2.DDIAS_90>=1 then '90'
when t2.DDIAS_60 IS NOT NULL AND t2.DDIAS_60>=1 then '60'
when t2.DDIAS_30 IS NOT NULL AND t2.DDIAS_30>=1 then '30'
when t2.DDIAS_0 IS NOT NULL AND t2.DDIAS_0>=1 then '0'
when t2.DDIAS_ACTUAL IS NOT NULL AND t2.DDIAS_ACTUAL>=1 then '0'
else (case when t2.ddias_total<0 then 'VNC'
	when (t2.ddias_total>=0 and t2.ddias_total<1) then 'PAGADO' end)
end as VENCIMIENTO,
t2.ddias_total,
t2.estado_cuenta,
t2.forma_pago ,
t2.tarjeta ,
t2.banco ,
t2.provincia ,
t2.ciudad ,
t2.lineas_activas ,
t2.lineas_desconectadas ,
t2.credit_class as sub_segmento,
t2.cr_cobranza ,
t2.ciclo_periodo ,
t2.tipo_cliente ,
t2.tipo_identificacion,
t2.fecha_carga
FROM db_rbm.reporte_cartera t2 WHERE fecha_carga=$fechamas1;