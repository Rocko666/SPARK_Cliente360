---SQL 1
set hive.cli.print.header=false;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set tez.queue.name=$COLA_EJECUCION;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_DEV_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_DEV_$ABREVIATURA_TEMP as
select a.marca,a.telefono
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_$ABREVIATURA_TEMP a 
union all
select b.marca,b.telefono from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_$ABREVIATURA_TEMP b
union all
select c.marca,c.telefono from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_$ABREVIATURA_TEMP c
union ALL 
select d.marca,d.telefono from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_$ABREVIATURA_TEMP d
union all
select h.marca,h.telefono from $ESQUEMA_TEMP.tmp_360_adelanto_saldo_$ABREVIATURA_TEMP h
union all
select e.marca,e.num_telefono as telefono from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_ACUMULADO_$ABREVIATURA_TEMP e
union all
select e.marca,e.num_telefono as telefono from $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_acumulado_$ABREVIATURA_TEMP e
union all
select f.marca,f.num_telefono as telefono from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_ACUM_$ABREVIATURA_TEMP f
union all
select g.marca,g.num_telefono as telefono from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_TOTAL_$ABREVIATURA_TEMP g;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_DEV_UNICOS_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_DEV_UNICOS_$ABREVIATURA_TEMP as
select distinct upper(marca) as marca,telefono from $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_DEV_$ABREVIATURA_TEMP;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_COMBO_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_COMBO_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor, sum(cantidad) cantidad 
from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_TOTAL_$ABREVIATURA_TEMP 
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_COMBO_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_COMBO_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor, sum(cantidad) cantidad 
from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_TOTAL_$ABREVIATURA_TEMP 
where fec_alta= '$fecha_eje1'
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor_bono_periodo, sum(cantidad) cant_bono_periodo 
from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_ACUM_$ABREVIATURA_TEMP 
where tipo='BONO'
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor_bono_diario, sum(cantidad) cant_bono_diario 
from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_ACUM_$ABREVIATURA_TEMP 
where fec_alta= '$fecha_eje1'
and tipo='BONO'
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_COMBO_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_COMBO_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor_combo_periodo, sum(cantidad) cant_combo_periodo 
from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_ACUM_$ABREVIATURA_TEMP 
where tipo='COMBO'
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_COMBO_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_COMBO_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor_combo_diario, sum(cantidad) cant_combo_diario 
from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_ACUM_$ABREVIATURA_TEMP 
where fec_alta= '$fecha_eje1'
and tipo='COMBO'
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(od_sms) coste_sms_periodo, sum(cantidad) cant_sms_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_$ABREVIATURA_TEMP 
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(od_sms) coste_sms_diario, sum(cantidad) cant_sms_diario 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_$ABREVIATURA_TEMP
where fecha_proceso= $fecha_eje2
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(od_voz) coste_voz_periodo, sum(cant_minutos) cant_min_periodo
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_$ABREVIATURA_TEMP 
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(od_voz) coste_voz_diario, sum(cant_minutos) cant_min_diario
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_$ABREVIATURA_TEMP
where fecha_proceso= $fecha_eje2
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(od_datos) coste_datos_periodo, sum(cantidad_megas) cant_megas_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_$ABREVIATURA_TEMP 
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(od_datos) coste_datos_diario, sum(cantidad_megas) cant_megas_diario 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_$ABREVIATURA_TEMP 
where fecha_proceso= $fecha_eje2
group by upper(marca),telefono;


DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(cobrado) contenido_periodo, sum(cantidad_eventos) cant_eventos_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_$ABREVIATURA_TEMP 
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(cobrado) contenido_diario, sum(cantidad_eventos) cant_eventos_diario 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_$ABREVIATURA_TEMP
where fecha_proceso= $fecha_eje2
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_ADELANTO_SALDO_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_ADELANTO_SALDO_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(cobrado) adelanto_periodo, sum(cantidad_eventos) cant_adelantos_periodo 
from $ESQUEMA_TEMP.tmp_360_adelanto_saldo_$ABREVIATURA_TEMP
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_ADELANTO_SALDO_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_ADELANTO_SALDO_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(cobrado) adelanto_diario, sum(cantidad_eventos) cant_adelantos_diario 
from $ESQUEMA_TEMP.tmp_360_adelanto_saldo_$ABREVIATURA_TEMP
where fecha= $fecha_eje2
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_ACUMULADO_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_ACUMULADO_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono, sum(valor_sin_iva) coste_buzon_periodo, sum(cantidad) cant_buzon_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_ACUMULADO_$ABREVIATURA_TEMP 
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_DIARIO_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_DIARIO_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono, sum(valor_sin_iva) coste_buzon_diario, sum(cantidad) cant_buzon_diario 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_DIARIO_$ABREVIATURA_TEMP
where fecha=$fecha_eje2
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_acumulado_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_acumulado_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono, sum(valor_sin_iva) coste_llamada_periodo, sum(cantidad) cant_llamada_periodo 
from $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_acumulado_$ABREVIATURA_TEMP 
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_diario_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_diario_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono, sum(valor_sin_iva) coste_llamada_diario, sum(cantidad) cant_llamada_diario 
from $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_diario_$ABREVIATURA_TEMP
where fecha=$fecha_eje2
group by upper(marca),num_telefono;


DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_OTC_T_360_DEVENGOS_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_OTC_T_360_DEVENGOS_$ABREVIATURA_TEMP as
select upper(a.marca) as marca,a.telefono
,coalesce(b.coste_sms_periodo,0) valor_sms_periodo
,coalesce(b.cant_sms_periodo,0) cantidad_sms_periodo
,coalesce(c.coste_sms_diario,0) valor_sms_diario
,coalesce(c.cant_sms_diario,0) cantidad_sms_diario
,coalesce(d.coste_voz_periodo,0) valor_voz_periodo
,coalesce(d.cant_min_periodo,0) cantidad_min_periodo
,coalesce(e.coste_voz_diario,0) valor_voz_diario
,coalesce(e.cant_min_diario,0) cant_min_diario
,coalesce(f.coste_datos_periodo,0) valor_datos_periodo
,coalesce(f.cant_megas_periodo,0) cantidad_megas_periodo
,coalesce(g.coste_datos_diario,0) valor_datos_diario
,coalesce(g.cant_megas_diario,0) cant_megas_diario
,coalesce(h.contenido_periodo,0) valor_contenido_periodo
,coalesce(h.cant_eventos_periodo,0) cantidad_eventos_periodo
,coalesce(i.contenido_diario,0) valor_contenido_diario
,coalesce(i.cant_eventos_diario,0) cant_eventos_diario
,coalesce(j.coste_buzon_periodo,0) valor_buzon_voz_periodo
,coalesce(j.cant_buzon_periodo,0) cantidad_buzon_voz_periodo
,coalesce(k.coste_buzon_diario,0) valor_buzon_diario
,coalesce(k.cant_buzon_diario,0) cantidad_buzon_diario
,coalesce(lla.coste_llamada_periodo,0) valor_llamada_espera_periodo
,coalesce(lla.cant_llamada_periodo,0) cantidad_llamada_espera_periodo
,coalesce(lld.coste_llamada_diario,0) valor_llamada_diario
,coalesce(lld.cant_llamada_diario,0) cantidad_llamada_diario
,coalesce(l.valor_bono_periodo,0) valor_bono_periodo
,coalesce(l.cant_bono_periodo,0) cant_bono_periodo
,coalesce(m.valor_bono_diario,0) valor_bono_diario
,coalesce(m.cant_bono_diario,0) cant_bono_diario
,coalesce(n.valor_combo_periodo,0) valor_combo_periodo
,coalesce(n.cant_combo_periodo,0) cant_combo_periodo
,coalesce(o.valor_combo_diario,0) valor_combo_diario
,coalesce(o.cant_combo_diario,0) cant_combo_diario
,coalesce(p.valor,0) valor_bono_combo_pdv_rec_periodo
,coalesce(p.cantidad,0) cant_bono_combo_pdv_rec_periodo
,coalesce(q.valor,0) valor_bono_combo_pdv_rec_diario
,coalesce(q.cantidad,0) cant_bono_combo_pdv_rec_diario
,(
	coalesce(c.coste_sms_diario,0)
	+coalesce(e.coste_voz_diario,0)
	+coalesce(g.coste_datos_diario,0)
	+coalesce(i.contenido_diario,0)
	+coalesce(k.coste_buzon_diario,0)
	+coalesce(lld.coste_llamada_diario,0)
	+coalesce(q.valor,0)
	+coalesce(s.adelanto_diario,0)
) as total_devengo_diario
,(
	coalesce(b.coste_sms_periodo,0) --OD SMS
	+coalesce(d.coste_voz_periodo,0) --OD VOZ
	+coalesce(f.coste_datos_periodo,0) --OD DATOS
	+coalesce(h.contenido_periodo,0) --OD CONTENIDOS
	+coalesce(j.coste_buzon_periodo,0) --OD BUZON VOZ
	+coalesce(lla.coste_llamada_periodo,0) --OD LLAMADA EN ESPERA
	+coalesce(p.valor,0) --BONOS Y COMBOS TOTALES (PDV + DEVENGADOS)
	+coalesce(r.adelanto_periodo,0) --OD ADELANTO DE SALDO
) as total_devengo_periodo
,coalesce(r.adelanto_periodo,0) valor_adelanto_saldo_periodo
,coalesce(r.cant_adelantos_periodo,0) cantidad_adelantos_saldo_periodo
,coalesce(s.adelanto_diario,0) valor_adelanto_saldo_diario
,coalesce(s.cant_adelantos_diario,0) cantidad_adelantos_saldo_diario
from $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_DEV_UNICOS_$ABREVIATURA_TEMP a 
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_1_$ABREVIATURA_TEMP b 
on a.telefono=b.telefono and upper(a.marca)=upper(b.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_2_$ABREVIATURA_TEMP c
on a.telefono=c.telefono and upper(a.marca)=upper(c.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_1_$ABREVIATURA_TEMP d
on a.telefono=d.telefono and upper(a.marca)=upper(d.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_2_$ABREVIATURA_TEMP e
on a.telefono=e.telefono and upper(a.marca)=upper(e.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_1_$ABREVIATURA_TEMP f
on a.telefono=f.telefono and upper(a.marca)=upper(f.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_2_$ABREVIATURA_TEMP g
on a.telefono=g.telefono and upper(a.marca)=upper(g.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_1_$ABREVIATURA_TEMP h
on a.telefono=h.telefono and upper(a.marca)=upper(h.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_2_$ABREVIATURA_TEMP i
on a.telefono=i.telefono and upper(a.marca)=upper(i.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_ACUMULADO_1_$ABREVIATURA_TEMP j
on a.telefono=j.num_telefono and upper(a.marca)=upper(j.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_DIARIO_1_$ABREVIATURA_TEMP k
on a.telefono=k.num_telefono and upper(a.marca)=upper(k.marca)
left join $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_acumulado_1_$ABREVIATURA_TEMP lla
on a.telefono=lla.num_telefono and upper(a.marca)=upper(lla.marca)
left join $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_diario_1_$ABREVIATURA_TEMP lld
on a.telefono=lld.num_telefono and upper(a.marca)=upper(lld.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_1_$ABREVIATURA_TEMP l
on a.telefono=l.telefono and upper(a.marca)=upper(l.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_2_$ABREVIATURA_TEMP m
on a.telefono=m.telefono and upper(a.marca)=upper(m.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_COMBO_1_$ABREVIATURA_TEMP n
on a.telefono=n.telefono and upper(a.marca)=upper(n.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_COMBO_2_$ABREVIATURA_TEMP o
on a.telefono=o.telefono and upper(a.marca)=upper(o.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_COMBO_1_$ABREVIATURA_TEMP p
on a.telefono=p.telefono and upper(a.marca)=upper(p.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_COMBO_2_$ABREVIATURA_TEMP q
on a.telefono=q.telefono and upper(a.marca)=upper(q.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_ADELANTO_SALDO_1_$ABREVIATURA_TEMP r
on a.telefono=r.telefono and upper(a.marca)=upper(r.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_ADELANTO_SALDO_2_$ABREVIATURA_TEMP s
on a.telefono=s.telefono and upper(a.marca)=upper(s.marca);



--SQL 2 
set hive.cli.print.header=false;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set tez.queue.name=$COLA_EJECUCION;

insert overwrite table db_reportes.otc_t_360_devengos partition(fecha_proceso)
select 
t1.telefono,
t1.valor_sms_periodo,
t1.cantidad_sms_periodo,
t1.valor_sms_diario,
t1.cantidad_sms_diario,
t1.valor_voz_periodo,
t1.cantidad_min_periodo,
t1.valor_voz_diario,
t1.cant_min_diario,
t1.valor_datos_periodo,
t1.cantidad_megas_periodo,
t1.valor_datos_diario,
t1.cant_megas_diario,
t1.valor_contenido_periodo,
t1.cantidad_eventos_periodo,
t1.valor_contenido_diario,
t1.cant_eventos_diario,
t1.valor_buzon_voz_periodo,
t1.cantidad_buzon_voz_periodo,
t1.valor_buzon_diario,
t1.cantidad_buzon_diario,
t1.valor_bono_periodo,
t1.cant_bono_periodo,
t1.valor_bono_diario,
t1.cant_bono_diario,
t1.valor_combo_periodo,
t1.cant_combo_periodo,
t1.valor_combo_diario,
t1.cant_combo_diario,
t1.valor_bono_combo_pdv_rec_periodo,
t1.cant_bono_combo_pdv_rec_periodo,
t1.valor_bono_combo_pdv_rec_diario,
t1.cant_bono_combo_pdv_rec_diario,
t1.total_devengo_diario,
t1.total_devengo_periodo,
t1.valor_adelanto_saldo_periodo,
t1.cantidad_adelantos_saldo_periodo,
t1.valor_adelanto_saldo_diario,
t1.cantidad_adelantos_saldo_diario,
case when upper(t1.marca) = 'MOVISTAR' then 'TELEFONICA' else t1.marca end AS marca,
t1.valor_llamada_espera_periodo,
t1.cantidad_llamada_espera_periodo,
t1.valor_llamada_diario,
t1.cantidad_llamada_diario,
$FECHAEJE AS fecha_proceso
from $ESQUEMA_TEMP.TMP_OTC_T_360_DEVENGOS_$ABREVIATURA_TEMP t1;