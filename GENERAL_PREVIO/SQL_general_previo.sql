----sql 1
set hive.cli.print.header=false;	
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set tez.queue.name=capa_semantica;					

--OBTIENE EL METODO O FORMA DE PAGO POR CUENTA
--SI 2AM YA SE EJECUTARIA
drop table $ESQUEMA_TEMP.otc_t_360_mop_defecto_tmp;
create table $ESQUEMA_TEMP.otc_t_360_mop_defecto_tmp as
select 
t.account_num,
t.payment_method_id,
t.payment_method_name,
t.start_dat,--cuando order =2 fecha_inicio_forma_pago_anterior ,cuando order=1 fecha_inicio_forma_pago_factura
t.end_dat,   --cuando order =2 fecha_fin_forma_pago_anterior ,cuando order=1 fecha_fin_forma_pago_factura
t.orden
from(
select a.account_num, a.payment_method_id,b.payment_method_name,a.start_dat,a.end_dat
,row_number() over (partition by account_num order by nvl(end_dat,CURRENT_DATE) desc) as orden
from db_rbm.otc_t_accountdetails a
inner join db_rbm.otc_t_paymentmethod b on b.payment_method_id= a.payment_method_id
) t
where t.orden in (1,2);

--SI 2:30
--SE OBTIENE EL CATALOGO DE SEGMENTO POR COMBINACI? ?ICA DE SEGMENTO Y SUBSEGMENTO
drop table $ESQUEMA_TEMP.otc_t_360_homo_segmentos_1_tmp;
create table $ESQUEMA_TEMP.otc_t_360_homo_segmentos_1_tmp as
select distinct
upper(segmentacion) segmentacion
,UPPER(segmento) segmento
from db_cs_altas.otc_t_homologacion_segmentos;
    
--SE OBTIENE LA EDAD Y SEXO CALCULADOS PARA CADA L?EA
--SI SIN HORA, LA FUENTE NO SE ACTUALIZA DESDE JUNIO 2019
drop table $ESQUEMA_TEMP.otc_t_360_parque_edad_tmp;
create table $ESQUEMA_TEMP.otc_t_360_parque_edad_tmp as
SELECT dd.user_id num_telefonico, dd.edad,dd.sexo
FROM $ESQUEMA_TABLA.otc_t_parque_edad20 dd
inner join (SELECT max(fecha_proceso) max_fecha FROM $ESQUEMA_TABLA.otc_t_parque_edad20 where fecha_proceso < $fechamas1) fm on fm.max_fecha = dd.fecha_proceso;

--SE OBTIENEN LA FECHA M?IMA DE CARGA DE LA TABLA DE USUARIO MOVISTAR PLAY, MENOR O IGUAL A LA FECHA DE EJECUCI?
--SI, NO IMPORTA LA DEPENEDENCIA PUES TOMA LA MAXIMA FECHA QUE TENGA DATOS
drop table $ESQUEMA_TEMP.tmp_360_fecha_mplay;
CREATE TABLE $ESQUEMA_TEMP.tmp_360_fecha_mplay as
SELECT MAX(FECHA_PROCESO) as fecha_proceso FROM DB_MPLAY.OTC_T_USERS_SEMANAL WHERE FECHA_PROCESO <= $FECHAEJE;

--SE OBTIENEN LOS USUARIOS QUE USAN MOVISTAR PLAY
--SI, NO IMPORTA LA DEPENEDENCIA PUES TOMA LA MAXIMA FECHA QUE TENGA DATOS
drop table $ESQUEMA_TEMP.otc_t_360_usa_app_movi_tmp;
create table $ESQUEMA_TEMP.otc_t_360_usa_app_movi_tmp as
SELECT
distinct
SUBSTR((CASE WHEN A.USERID = NULL OR A.USERID = '' THEN B.USERUNIQUEID ELSE A.USERID END),-9) AS numero_telefono
FROM DB_MPLAY.OTC_T_USERS_SEMANAL AS A
LEFT JOIN DB_MPLAY.OTC_T_USERS AS B ON (A.USERUNIQUEID = B.MIBID AND A.FECHA_PROCESO = B.FECHA_PROCESO)
inner join $ESQUEMA_TEMP.tmp_360_fecha_mplay c on (a.fecha_proceso=c.fecha_proceso)
WHERE UPPER (A.SUBSCRIPTIONNAME) = 'EC_INT_TV_U_ACT_SERV';

--SE OBTIENEN LOS USUARIOS REGISTRADOS EN MOVISTAR PLAY					
--SI, NO IMPORTA LA DEPENEDENCIA PUES TOMA LA MAXIMA FECHA QUE TENGA DATOS
drop table $ESQUEMA_TEMP.otc_t_360_usuario_app_movi_tmp;
create table $ESQUEMA_TEMP.otc_t_360_usuario_app_movi_tmp as
SELECT
distinct
SUBSTR((CASE WHEN A.USERID = NULL OR A.USERID = ''  THEN B.USERUNIQUEID ELSE A.USERID END),-9) AS numero_telefono
FROM DB_MPLAY.OTC_T_USERS_SEMANAL AS A
LEFT JOIN DB_MPLAY.OTC_T_USERS AS B ON (A.USERUNIQUEID = B.MIBID AND A.FECHA_PROCESO = B.FECHA_PROCESO)
inner join $ESQUEMA_TEMP.tmp_360_fecha_mplay c on (a.fecha_proceso=c.fecha_proceso)
WHERE UPPER (A.SUBSCRIPTIONNAME) = 'EC_INT_TV_U_REG';

--SI, FUENTE DESACTUALIZADA DESDE AGOSTO 2020
drop table $ESQUEMA_TEMP.otc_t_360_prob_churn_pre_temp;
create table $ESQUEMA_TEMP.otc_t_360_prob_churn_pre_temp as	
select gen.num_telefonico, pre.prob_churn
from $ESQUEMA_TEMP.otc_t_360_parque_1_tmp gen
inner join db_rdb.otc_t_churn_prepago pre on pre.telefono = gen.num_telefonico
inner join (SELECT max(fecha) max_fecha FROM db_rdb.otc_t_churn_prepago where fecha < $fechamas1) fm on fm.max_fecha = pre.fecha
where upper(gen.linea_negocio) like 'PRE%'
group by gen.num_telefonico, pre.prob_churn;

--SI, se actualiza la fuente con tabla de MO
drop table $ESQUEMA_TEMP.otc_t_360_prob_churn_pos_temp;
create table $ESQUEMA_TEMP.otc_t_360_prob_churn_pos_temp as	
select gen.num_telefonico, pos.probability_label_1 as prob_churn
from $ESQUEMA_TEMP.otc_t_360_parque_1_tmp gen
inner join db_thebox.pred_portabilidad2022 pos on pos.num_telefonico = gen.num_telefonico
where upper(gen.linea_negocio) not like 'PRE%'
group by gen.num_telefonico, pos.probability_label_1;

--SI, FUENTE ESTA A LAS 2Y10 AM
drop table $ESQUEMA_TEMP.otc_t_360_homologacion_segmentos_1;
create table $ESQUEMA_TEMP.otc_t_360_homologacion_segmentos_1 as
select distinct
upper(segmentacion) segmentacion
,UPPER(segmento) segmento
,UPPER(segmento_fin) segmento_fin
from db_cs_altas.otc_t_homologacion_segmentos
union
select 'CANALES CONSIGNACION','OTROS','OTROS';

--SI, CATALOGO CON ACTUALIZACIÓN MENSUAL
drop table $ESQUEMA_TEMP.otc_t_360_catalogo_celdas_dpa_tmp;
create table $ESQUEMA_TEMP.otc_t_360_catalogo_celdas_dpa_tmp as
select cc.*
from db_ipaccess.catalogo_celdas_dpa cc 
inner join (SELECT max(fecha_proceso) max_fecha FROM db_ipaccess.catalogo_celdas_dpa where fecha_proceso < $fechamas1) cfm on cfm.max_fecha = cc.fecha_proceso;					

--SI TOMA PARTICIONES DE INICIO DE MES DE 3 MESES PASADOS
drop table $ESQUEMA_TEMP.tmp_360_ticket_recarga;
create table $ESQUEMA_TEMP.tmp_360_ticket_recarga as
select
fecha_proceso as mes,
num_telefonico as telefono,
sum(ingreso_recargas_m0) as total_rec_bono,
sum(cantidad_recargas_m0) as total_cantidad
from db_reportes.otc_t_360_ingresos
where fecha_proceso in ($fechaInimenos3mes,$fechaInimenos2mes,$fechaInimenos1mes)
group by fecha_proceso,
num_telefonico;

--SI, TOMA LA MAXIMA PARTICION
drop table $ESQUEMA_TEMP.otc_t_fecha_scoring_tx;
create table $ESQUEMA_TEMP.otc_t_fecha_scoring_tx as
select max(fecha_carga) as fecha_carga
from db_reportes.otc_t_scoring_tiaxa 
where fecha_carga>=$fechamenos5 and fecha_carga<=$FECHAEJE;

--SI, TOMA LA MAXIMA PARTICION
drop table $ESQUEMA_TEMP.otc_t_scoring_tiaxa_tmp;
create table $ESQUEMA_TEMP.otc_t_scoring_tiaxa_tmp as
select substr(a.msisdn,4,9) as numero_telefono, max(a.score1) as score1, max(a.score2) as score2, max(a.limite_credito) as limite_credito
from db_reportes.otc_t_scoring_tiaxa a, $ESQUEMA_TEMP.otc_t_fecha_scoring_tx b
where a.fecha_carga = b.fecha_carga
group by substr(a.msisdn,4,9);

--SI, TOMA INFORMACION EN UN RANGO DE 6 MESES Y EL CATALOGO SE EJECUTA SEMANALMENTE A LAS 0 HORAS
drop table $ESQUEMA_TEMP.otc_t_360_num_bancos_tmp;
create table $ESQUEMA_TEMP.otc_t_360_num_bancos_tmp as
SELECT a.numerodestinosms AS telefono,COUNT(*) AS conteo
FROM default.otc_t_xdrcursado_sms a
inner join db_rdb.otc_t_numeros_bancos_sms b
on b.sc=a.numeroorigensms
WHERE 1=1
AND a.fechasms >= $fechamenos6mes AND a.fechasms < $fechamas1
GROUP BY a.numerodestinosms;

--SI, SE TRABAJA CON LA INFORMACIÓN EXISTENTE EN TABLA ADENDUM
drop table $ESQUEMA_TEMP.otc_t_360_general_temp_adendum;
create table $ESQUEMA_TEMP.otc_t_360_general_temp_adendum as
select x.* from (select a.phone_number , a.penaltyamount as adendum, a.report_date, ROW_NUMBER () OVER 
(PARTITION BY a.phone_number
    ORDER BY a.report_date DESC) AS IDE from db_rdb.otc_t_adendum as a
where a.penaltyamount <> '0' 
and a.report_date <= '$fecha_eje4') x where x.IDE = 1;

--SI
DROP TABLE $ESQUEMA_TEMP.tmp_otc_t_user_sin_duplicados;
DROP TABLE $ESQUEMA_TEMP.tmp_360_web;
DROP TABLE $ESQUEMA_TEMP.tmp_360_app_mi_movistar;

--SI, SU DEPENDENCIA ESTA ANTES DE LAS 2AM
CREATE TABLE $ESQUEMA_TEMP.tmp_otc_t_user_sin_duplicados AS
SELECT firstname,
'SI' as usuario_web,
MIN(cast(from_unixtime(unix_timestamp(web.createdate,'yyyy-MM-dd HH:mm:ss.SSS')) as timestamp)) as fecha_registro_web
FROM db_lportal.otc_t_user web
WHERE web.pt_fecha_creacion >= 20200827 AND web.pt_fecha_creacion <= $FECHAEJE
AND LENGTH(firstname)=19
GROUP BY firstname;

--SI, SUS DEPENDENCIAS ESTAN ANTES DE LAS 2AM
CREATE TABLE $ESQUEMA_TEMP.tmp_360_web AS
SELECT 
web.usuario_web,
web.fecha_registro_web,
cst.cust_ext_ref
FROM $ESQUEMA_TEMP.tmp_otc_t_user_sin_duplicados web
INNER JOIN db_rdb.otc_t_r_cim_res_cust_acct cst
ON CAST(firstname AS bigint)=cst.object_id;

--SI, SUS DEPENDENCIAS ESTAN ANTES DE LAS 2AM
CREATE TABLE $ESQUEMA_TEMP.tmp_360_app_mi_movistar AS
SELECT
num_telefonico,
usuario_app,
fecha_registro_app,
perfil,
usa_app
FROM (
SELECT
reg.celular AS num_telefonico,
'SI' AS usuario_app,
reg.fecha_creacion AS fecha_registro_app,
reg.perfil,
(CASE WHEN trx.activo IS NULL THEN 'NO' ELSE trx.activo END) AS usa_app,
(ROW_NUMBER() OVER (PARTITION BY reg.celular ORDER BY reg.fecha_creacion DESC)) AS rnum
FROM db_trxdb.otc_t_registro_usuario reg
LEFT JOIN (SELECT 'SI' AS activo, 
min_mines_wv,
MAX(fecha_mines_wv)
FROM db_trxdb.otc_t_mines_wv
WHERE id_action_wv=2005 
AND pt_mes = SUBSTRING($FECHAEJE,1,6)
GROUP BY min_mines_wv) trx
ON reg.celular=trx.min_mines_wv
WHERE reg.pt_fecha_creacion<=$FECHAEJE) x
WHERE x.rnum=1;

--20210629 - EJECUTA EL BORRADO DE LAS TABLAS TEMPORALES
--SI
DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_otc_t_r_cim_cont;
DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_thebox_base_censo;
DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_cim_cont_cedula_duplicada;
DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_cim_cont_sin_duplicados;
DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_cim_cont_con_fecha_ok;
DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_data_total_sin_duplicados;
DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_principal_min_fecha;
DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_fecha_nacimiento_mvp;

--20210629 - CREA TABLA TEMPORAL CON LA INFORMACION DE LA FUENTE otc_t_r_cim_cont
--SI, ESA FUENTE ES MENSUAL, ESTA LISTA ANTES DE LAS 2AM SIN EMBARGO SE PUEDE CAMBIAR SU RECUERRENCIA A DIARIA
CREATE TABLE $ESQUEMA_TEMP.tmp_otc_t_r_cim_cont AS
SELECT DISTINCT doc_number AS cedula, 
birthday AS fecha_nacimiento
FROM db_rdb.otc_t_r_cim_cont
WHERE doc_number IS NOT NULL 
AND birthday IS NOT NULL;

--20210629 - CREA TABLA TEMPORAL CON LA INFORMACION DE LA FUENTE base_censo
--SI, ESTE AÑO SE ACTUALIZARÁ ESTA BASE
CREATE TABLE $ESQUEMA_TEMP.tmp_thebox_base_censo AS
SELECT DISTINCT cedula, 
fecha_nacimiento
FROM db_thebox.base_censo
WHERE cedula IS NOT NULL 
AND fecha_nacimiento IS NOT NULL;

--20210629 - CREA TABLA CON SOLO LA INFORMACION DE LAS CEDULAS DUPLICADOS 
--SI
CREATE TABLE $ESQUEMA_TEMP.tmp_cim_cont_cedula_duplicada AS
SELECT DISTINCT x.cedula FROM(SELECT cedula,count(1) 
FROM $ESQUEMA_TEMP.tmp_otc_t_r_cim_cont 
GROUP BY cedula HAVING COUNT(1)>1) x;

--20210629 - CREA TABLA CON SOLO LA INFORMACIÓN DE LAS CEDULAS CON FECHA SIN DUPLICADOS
--SI
CREATE TABLE $ESQUEMA_TEMP.tmp_cim_cont_sin_duplicados AS
SELECT a.cedula,a.fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_otc_t_r_cim_cont a
LEFT JOIN (SELECT cedula FROM $ESQUEMA_TEMP.tmp_cim_cont_cedula_duplicada) b
ON a.cedula=b.cedula
WHERE b.cedula IS NULL;

--20210629 - CREA TABLA PRINCIPAL CON LA INFORMACION DE otc_t_r_cim_cont CON FECHA OK SIN DUPLICADOS
--SI
CREATE TABLE $ESQUEMA_TEMP.tmp_cim_cont_con_fecha_ok AS
SELECT DISTINCT a.cedula,b.fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_cim_cont_cedula_duplicada a
INNER JOIN (SELECT cedula,fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_thebox_base_censo) b
ON a.cedula=b.cedula;

--20210629 - CREA TABLA PRINCIPAL CON LA INFORMACION DE otc_t_r_cim_cont CON FECHA MIN SIN DUPLICADOS
--SI
CREATE TABLE $ESQUEMA_TEMP.tmp_principal_min_fecha AS
SELECT a.cedula,MIN(a.fecha_nacimiento) AS fecha_nacimiento
FROM $ESQUEMA_TEMP.tmp_otc_t_r_cim_cont a
INNER JOIN (SELECT a.cedula FROM $ESQUEMA_TEMP.tmp_cim_cont_cedula_duplicada a
LEFT JOIN (SELECT cedula FROM $ESQUEMA_TEMP.tmp_cim_cont_con_fecha_ok) b
ON a.cedula=b.cedula
WHERE b.cedula IS NULL) c
ON a.cedula=c.cedula
GROUP BY a.cedula;

--20210629 - CREA TABLA PRINCIPAL CON LA INFORMACION TOTAL DE otc_t_r_cim_cont Y base_censo SIN DUPLICADOS
--SI
CREATE TABLE $ESQUEMA_TEMP.tmp_data_total_sin_duplicados AS
SELECT cedula,fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_cim_cont_sin_duplicados
UNION
SELECT cedula,fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_cim_cont_con_fecha_ok
UNION
SELECT cedula,fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_principal_min_fecha;

--20210629 - CREA TABLA CON LA INFORMACION DE TODOS LAS CEDULAS CON SU FECHA, ANTES DE CRUZAR CON LA MOVIPARQUE
--SI
CREATE TABLE $ESQUEMA_TEMP.tmp_fecha_nacimiento_mvp AS

--20210629 - OBTIENE LA INFORMACION DE LOS REGISTROS COMUNES
SELECT COALESCE(a.cedula,b.cedula) AS cedula,
COALESCE(a.fecha_nacimiento,b.fecha_nacimiento) AS fecha_nacimiento
FROM (SELECT cedula, fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_data_total_sin_duplicados) a
INNER JOIN (SELECT DISTINCT CAST(cedula AS string) AS cedula, 
fecha_nacimiento
FROM db_thebox.base_censo
WHERE cedula IS NOT NULL 
AND fecha_nacimiento IS NOT NULL) b
ON a.cedula=b.cedula

UNION

--20210629 - OBTIENE LA INFORMACION DE SOLO LOS REGISTROS DE LA TABLA PRINCIPAL otc_t_r_cim_cont
SELECT a.cedula,
a.fecha_nacimiento
FROM (SELECT cedula, fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_data_total_sin_duplicados) a
LEFT JOIN (SELECT DISTINCT CAST(cedula AS string) AS cedula, 
fecha_nacimiento
FROM db_thebox.base_censo
WHERE cedula IS NOT NULL 
AND fecha_nacimiento IS NOT NULL) b
ON a.cedula=b.cedula
WHERE b.cedula IS NULL

UNION

--20210629 - OBTIENE LA INFORMACION DE SOLO LOS REGISTROS DE LA TABLA SECUNDARIA base_censo
SELECT a.cedula,
a.fecha_nacimiento
FROM (SELECT DISTINCT CAST(cedula AS string) AS cedula, 
fecha_nacimiento
FROM db_thebox.base_censo
WHERE cedula IS NOT NULL 
AND fecha_nacimiento IS NOT NULL) a
LEFT JOIN (SELECT cedula, fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_data_total_sin_duplicados) b
ON a.cedula=b.cedula
WHERE b.cedula IS NULL;