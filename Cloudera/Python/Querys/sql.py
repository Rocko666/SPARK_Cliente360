from Funciones.funcion import *

@cargar_consulta
def fun_extraer_catalogos_plan(bdd_llamadas, tabla_llamadas):
    qry = '''
           SELECT 
           marca
           ,codigo
           FROM {bdd_llamadas}.{tabla_llamadas}  
      '''.format(bdd_llamadas=bdd_llamadas, tabla_llamadas=tabla_llamadas)
    return qry

@cargar_consulta
def fun_extraer_catalogos_plan_operadora(bdd_altamira, tabla_plan_operadora):
    qry = '''
           SELECT 
           marca
           ,id_plan
           FROM {bdd_altamira}.{tabla_plan_operadora}  
      '''.format(bdd_altamira=bdd_altamira, tabla_plan_operadora=tabla_plan_operadora)
    return qry

@cargar_consulta
def fun_extraer_catalogo_bonos_pdv(bdd, tabla_bonos_pdv):
    qry = '''
           SELECT 
           bono
           ,tipo
           ,marca
           ,valor_con_iva
           FROM {bdd}.{tabla_bonos_pdv}  
      '''.format(bdd=bdd, tabla_bonos_pdv=tabla_bonos_pdv)
    return qry

@cargar_consulta
def fun_extraer_datos_diameter(bdd, tabla, fecha_inicial, fecha_final):
    qry = '''
       SELECT 
           MSISDN
           ,FEH_LLAMADA
           ,TIP_PREPAGO
           ,(sum(coste/1000.000)/1.12) od_datos
           ,sum(volumen_up+volumen_down)/(1024*1024) cantidad_megas
           FROM {bdd}.{tabla}
           WHERE FEH_LLAMADA BETWEEN  {fecha_inicial} and {fecha_final}
           and tip_llamada not in ('PQTFEE','PQTNET','SOSFEE','SOSNET')
           group by MSISDN
            ,FEH_LLAMADA
            ,TIP_PREPAGO
      '''.format(bdd=bdd, tabla=tabla, fecha_inicial=fecha_inicial, fecha_final=fecha_final)
    return qry

@cargar_consulta
def fun_extraer_datos_mecorig(bdd, tabla, fecha_inicial, fecha_final):
    qry = '''
       SELECT 
           MSISDN
           ,FECHA
           ,TIP_PREPAGO
           ,(sum(coste/1000.000)/1.12) od_sms
           ,count(*) cantidad
           FROM {bdd}.{tabla}
           WHERE FECHA BETWEEN  {fecha_inicial} and {fecha_final}
           and tip_llamada not in ('PQTFEE','PQTNET','SOSFEE','SOSNET')
           group by MSISDN
            ,FECHA
            ,TIP_PREPAGO
      '''.format(bdd=bdd, tabla=tabla, fecha_inicial=fecha_inicial, fecha_final=fecha_final)
    return qry

@cargar_consulta
def fun_extraer_datos_llamadas(bdd, tabla, fecha_inicial, fecha_final):
    qry = '''
       SELECT 
           MSISDN
           ,FECHA
           ,TIP_PREPAGO
           ,(sum(coste/1000.000)/1.12) od_voz
           ,(sum(duracion_total/60)) cant_minutos
           FROM {bdd}.{tabla}
           WHERE FECHA BETWEEN  {fecha_inicial} and {fecha_final}
           and tip_llamada not in ('PQTFEE','PQTNET','SOSFEE','SOSNET')
           group by MSISDN
            ,FECHA
            ,TIP_PREPAGO
      '''.format(bdd=bdd, tabla=tabla, fecha_inicial=fecha_inicial, fecha_final=fecha_final)
    return qry

@cargar_consulta
def fun_extraer_datos_contenidos(bdd, tabla, fecha_inicial, fecha_final):
    qry = '''
       SELECT 
           MSISDN
           ,FECHA
           ,TIP_PREPAGO
           ,sum(coste/1000)/1.12 cobrado
           ,count (*) cantidad_eventos
           FROM {bdd}.{tabla}
           WHERE FECHA BETWEEN  {fecha_inicial} and {fecha_final}
           and tip_llamada not in ('PQTFEE','PQTNET','SOSFEE','SOSNET')
           group by MSISDN
            ,FECHA
            ,TIP_PREPAGO
      '''.format(bdd=bdd, tabla=tabla, fecha_inicial=fecha_inicial, fecha_final=fecha_final)
    return qry

@cargar_consulta
def fun_extraer_datos_adelanto_saldo(bdd, tabla, fecha_inicial, fecha_final):
    qry = '''
       SELECT 
           MSISDN
           ,FECHA
           ,TIP_PREPAGO
           ,sum(coste/1000)/1.12 cobrado
           ,count (*) cantidad_eventos
           FROM {bdd}.{tabla}
           WHERE FECHA BETWEEN  {fecha_inicial} and {fecha_final}
           and tip_llamada in ('SOSFEE','PQTFEE')
           group by MSISDN
            ,FECHA
            ,TIP_PREPAGO
      '''.format(bdd=bdd, tabla=tabla, fecha_inicial=fecha_inicial, fecha_final=fecha_final)
    return qry
  
@cargar_consulta
def fun_extraer_datos_buzon_voz_diario(bdd, tabla, fecha_inicial, fecha_final, fec_cambio_buzon, fec_cambio_buzon_co, codigo_act, codigo_us):
    qry = '''
       SELECT 
           num_telefono
           ,FECHA
           ,COD_TIPPREPA
           ,sum(imp_coste) as valor
           ,count(*) as cantidad
           FROM {bdd}.{tabla}
           WHERE FECHA BETWEEN  {fecha_inicial} and {fecha_final}
           and cod_actuacio = (case 
				when fecha <= {fec_cambio_buzon} and cod_usuario='OLYMPUS' then 'SE'
	  			when (fecha > {fec_cambio_buzon} AND fecha < {fec_cambio_buzon_co})  and cod_usuario='BUZONVOZ' then '9A'
				when fecha >= {fec_cambio_buzon_co} and cod_usuario='{codigo_us}' then '{codigo_act}' end)
           and cod_estarec = 'EJ'
           and imp_coste = 18
           and cod_usuario in ('OLYMPUS','{codigo_us}')
           group by num_telefono
            ,FECHA
            ,COD_TIPPREPA
      '''.format(bdd=bdd, tabla=tabla, fecha_inicial=fecha_inicial, fecha_final=fecha_final, fec_cambio_buzon=fec_cambio_buzon,fec_cambio_buzon_co=fec_cambio_buzon_co, codigo_act=codigo_act, codigo_us=codigo_us)
    return qry

@cargar_consulta
def fun_extraer_datos_llamada_espera_diario(bdd, tabla, fecha_inicial, fecha_final, codigo_act, codigo_us):
    qry = '''
       SELECT 
           num_telefono
           ,FECHA
           ,COD_TIPPREPA
           ,sum(imp_coste) as valor
           ,count(*) as cantidad
           FROM {bdd}.{tabla}
           WHERE FECHA BETWEEN  {fecha_inicial} and {fecha_final}
           and cod_actuacio = '{codigo_act}'
           and cod_estarec = 'EJ'
           and imp_coste = 70
           and cod_usuario in ('{codigo_us}')
           group by num_telefono
            ,FECHA
            ,COD_TIPPREPA
      '''.format(bdd=bdd, tabla=tabla, fecha_inicial=fecha_inicial, fecha_final=fecha_final, codigo_act=codigo_act, codigo_us=codigo_us)
    return qry

@cargar_consulta
def fun_extraer_combos_bonos(fecha_inicial, fecha_final):
    qry = '''
        SELECT 
            a.cod_bono
            ,a.num_telefono
            ,a.fec_alta
            ,ab.fecha as fecha_actabopre
            ,a.fecha as fecha_adqui
            ,ab.cod_usuario
            ,case when (op.marca is null or op.marca like '%TELEFONICA%') then 'Movistar' else op.marca end as marca
            ,(ab.imp_coste/1000)/1.12 as valor_bono
            from db_rdb.otc_t_ppga_adquisiciones a
            inner join db_reportes.cat_bonos_pdv b
            on a.cod_bono=b.bono
            inner join db_rdb.otc_t_ppga_actabopre ab
            on a.sec_actuacion=ab.sec_actuacion 
            and a.num_telefono=ab.num_telefono
            and (ab.fecha>= {fecha_inicial} and ab.fecha<= {fecha_final})
            and ab.COD_ESTAREC='EJ'
            left join db_altamira.otc_t_plan_operadora op 
            on op.ID_PLAN=ab.COD_TIPPREPA
            where a.fecha >='{fecha_inicial}' and a.fecha <='{fecha_final}'
      '''.format(fecha_inicial=fecha_inicial, fecha_final=fecha_final)
    return qry

@cargar_consulta
def fun_extraer_movi_parque(fecha_alta_inicial, fecha_alta_final, fecha_proc, fecha_eje_pv, condicion_abonado):
    qry = '''
        SELECT DISTINCT
            t.num_telefonico,
            t.plan_codigo codigo_plan,
            t.fecha_alta,
            t.fecha_last_status,
            t.estado_abonado,
            t.fecha_proceso,
            t.numero_abonado,
            t.linea_negocio,
            t.account_num,
            t.sub_segmento,
            t.tipo_doc_cliente,
            t.identificacion_cliente,
            t.cliente,
            case 
            	when upper(linea_negocio) = 'PREPAGO' then 'PREPAGO'
            	when plan_codigo ='PMH' then 'HOME'
            	else 'POSPAGO' end linea_negocio_homologado,
            t.marca,
            t.ciclo_fact,
            t.correo_cliente_pr,
            t.telefono_cliente_pr,
            t.imei,
            t.orden
            from(
            	SELECT num_telefonico,
            	plan_codigo,
            	fecha_alta,
            	fecha_baja,
            	nvl(fecha_modif,fecha_alta) fecha_last_status,
            	case when (fecha_baja is null or fecha_baja = '') then current_timestamp() else fecha_baja end as fecha_baja_new,
            	estado_abonado,
            	{fecha_eje_pv} fecha_proceso, 
            	numero_abonado,
            	linea_negocio,
            	account_num,
            	sub_segmento,
            	documento_cliente identificacion_cliente,
            	marca,
            	tipo_doc_cliente,
            	cliente,
            	ciclo_fact,
            	correo_cliente_pr,
            	telefono_cliente_pr,
            	imei,
            	row_number() over (partition by num_telefonico order by (case when (fecha_baja is null or fecha_baja = '') then current_timestamp() else fecha_baja end) desc,fecha_alta desc,nvl(fecha_modif,fecha_alta) desc) as orden
            	FROM db_cs_altas.otc_t_nc_movi_parque_v1
            	WHERE fecha_proceso = {fecha_proc}
            ) t
            where t.orden=1
            and upper(t.marca) = 'TELEFONICA'
            {condicion_abonado}
            --and t.estado_abonado not in ('BAA')
            and t.fecha_alta<'{fecha_alta_inicial}' and (t.fecha_baja>'{fecha_alta_final}' or t.fecha_baja is null)
      '''.format(fecha_alta_inicial=fecha_alta_inicial, fecha_alta_final=fecha_alta_final, fecha_proc=fecha_proc, fecha_eje_pv=fecha_eje_pv, condicion_abonado=condicion_abonado)
    return qry


@cargar_consulta
def fun_extraer_cuenta_cliente():
    qry = '''
        SELECT cliente_id,
			cta_facturacion
            from db_rbm.otc_t_vw_cta_facturacion
            where cta_facturacion is not null
            and cta_facturacion != ''
            group by cliente_id,
            cta_facturacion
      '''
    return qry

@cargar_consulta
def fun_extraer_planes_categoria():
    qry = '''
        SELECT cod_plan_activo,
                des_plan_tarifario,
                cod_categoria,
                categoria,
                tarifa_basica,
                comercial,
                id_tipo_linea
            from db_cs_altas.otc_t_ctl_planes_categoria_tarifa
      '''
    return qry

@cargar_consulta
def fun_extraer_churn(fechamenos5, fechamas1):
    qry = '''
        SELECT
            a.num_telefonico,
            a.counted_days,
            'churn' as fuente
            from db_temporales.tmp_360_otc_t_360_churn90_ori_prod a
      '''.format(fechamenos5=fechamenos5, fechamas1=fechamas1)
    return qry

@cargar_consulta
def fun_extraer_churn_dia():
    qry = '''
        select distinct numero_telefono as num_telefonico,0 as counted_days
                from db_temporales.tmp_otc_t_360_recargas
                where ingreso_recargas_dia>0 or
                cantidad_recarga_dia>0 or
                ingreso_bonos_dia>0 or
                cantidad_bonos_dia>0 or
                ingreso_combos_dia>0 or
                cantidad_combos_dia>0
      '''
    return qry

@cargar_consulta
def fun_extraer_churn_inac(fecha_inac_1):
    qry = '''
        SELECT num_telefonico,counted_days 
        FROM db_temporales.tmp_360_otc_t_360_churn90_tmp1_prod
      '''.format(fecha_inac_1=fecha_inac_1)
    return qry

@cargar_consulta
def fun_extraer_parque_inac():
    qry = '''
        SELECT telefono
        FROM db_temporales.tmp_360_parque_inactivo_prod
        group by telefono
      '''
    return qry


@cargar_consulta
def fun_extraer_cur_t2(fecha_menos_2_mes, fecha_eje):
    qry = '''
        select numeroorigen as telefono
		  ,sum(vol_total_2g/(1024*1024)) as total_2g
		  ,sum(vol_total_3g/(1024*1024)) as total_3g
		  ,sum(vol_total_lte/(1024*1024)) as total_4g
		  ,activity_start_dt as fecha
          from db_cmd.otc_t_dm_cur_t2
          where 1=1
          and activity_start_dt > {fecha_menos_2_mes} and activity_start_dt <= {fecha_eje}
          group by numeroorigen,activity_start_dt
      '''.format(fecha_menos_2_mes=fecha_menos_2_mes, fecha_eje=fecha_eje)
    return qry

@cargar_consulta
def fun_extraer_xdrcursado_sms(fecha_menos_1_mes, fecha_eje):
    qry = '''
        select numeroorigensms as telefono,count(1) as cantidad_sms
          from default.otc_t_xdrcursado_sms
          where fechasms > {fecha_menos_1_mes}
		  and fechasms <= {fecha_eje}
          and sentidotrafico like '%S%'
		  group by numeroorigensms
      '''.format(fecha_menos_1_mes=fecha_menos_1_mes, fecha_eje=fecha_eje)
    return qry


@cargar_consulta
def fun_extraer_ppcs_llamadas(fecha_menos_2_mes, fecha_eje):
    qry = '''
        select  nvl(case when length(a.msisdn)>10 then substr(a.msisdn,-9) else a.msisdn end,'') as  numeroorigenllamada 
         ,cast(nvl(a.duracion/60,0) as float) cantidad_minutos
		 ,cast(a.fecha as string) as fecha_proceso
         from db_altamira.otc_t_ppcs_llamadas a
         where a.fecha > {fecha_menos_2_mes} and a.fecha <= {fecha_eje}
      '''.format(fecha_menos_2_mes=fecha_menos_2_mes, fecha_eje=fecha_eje)
    return qry

@cargar_consulta
def fun_extraer_costed_event_type_1(fecha_menos_2_mes, fecha_eje):
    qry = '''
        select
        a.event_source as numeroorigenllamada
         ,cast(nvl(a.event_attr_4/60,0) as float) as cantidad_minutos
		 ,a.fecha as fecha_proceso
         from db_rbm.otc_t_costedevent_x_dia a
         where a.fecha > '{fecha_menos_2_mes}' and a.fecha <= '{fecha_eje}'
         and a.event_type_id =1
      '''.format(fecha_menos_2_mes=fecha_menos_2_mes, fecha_eje=fecha_eje)
    return qry

@cargar_consulta
def fun_extraer_costed_event_type_3(fecha_menos_1_mes, fecha_eje):
    qry = '''
        select a.event_source num_telefono,
	     sum(cast(a.EVENT_ATTR_4 as float)/1024) total_mb,
         cast(a.fecha as bigint) as fecha 
         from db_rbm.otc_t_costedevent_x_dia a
         where a.event_type_id in (3)
         and cast(a.fecha as bigint) > '{fecha_menos_1_mes}' and cast(a.fecha as bigint) <= '{fecha_eje}'
         and length(a.event_source)<10
		 group by a.event_source,fecha,event_attr_30
      '''.format(fecha_menos_1_mes=fecha_menos_1_mes, fecha_eje=fecha_eje)
    return qry


@cargar_consulta
def fun_extraer_ppcs_diameter(fecha_menos_1_mes, fecha_eje):
    qry = '''
        select   nvl(case when length(msisdn)>10 then substr(msisdn,-9) else msisdn end,'') as num_telefono,
         sum(cast((nvl((volumen_up/1024/1024),0)+nvl((volumen_down/1024/1024),0))as float)) as total_mb,
		 cast(feh_llamada as bigint) as fecha 
         from db_altamira.otc_t_ppcs_diameter
		 where feh_llamada > '{fecha_menos_1_mes}' and feh_llamada <= '{fecha_eje}'
		 group by msisdn,feh_llamada
      '''.format(fecha_menos_1_mes=fecha_menos_1_mes, fecha_eje=fecha_eje)
    return qry


@cargar_consulta
def fun_extraer_parque_trafico(esquema_temp, tbl_parque_temp, fecha_eje):
    qry = '''
        select t1.num_telefonico,
		t1.estado_abonado,					
		t1.fecha_proceso,
		t1.numero_abonado,
		t1.account_num,
		t1.linea_negocio,
        T1.linea_negocio_homologado,
		t1.marca,
		t1.plan_codigo,
        UPPER(t2.segmento) segmento,
		upper(t2.segmento_fin) segmento_fin,
		t1.orden 
        from 
		(SELECT num_telefonico,
		estado_abonado,					
		{fecha_eje} fecha_proceso,
		numero_abonado,
		account_num,
		linea_negocio,
        linea_negocio_homologado,
        sub_segmento,
		marca,
		codigo_plan as plan_codigo,
		row_number() over (partition by num_telefonico order by es_parque desc) as orden
		FROM {esquema_temp}.{tbl_parque_temp}) as t1 left join
        (select distinct
		upper(segmentacion) segmentacion
		,UPPER(segmento) segmento
		,UPPER(segmento_fin) segmento_fin
		from db_cs_altas.otc_t_homologacion_segmentos
		union
		select 'CANALES CONSIGNACION','OTROS','OTROS') as t2
        on (t2.segmentacion = (case when UPPER(t1.sub_segmento) = 'ROAMING' then 'ROAMING XDR'
											when UPPER(t1.sub_segmento) like 'PEQUE%' then 'PEQUENAS'
											when UPPER(t1.sub_segmento) like 'TELEFON%P%BLICA' then 'TELEFONIA PUBLICA'
											when UPPER(t1.sub_segmento) like 'CANALES%CONSIGNACI%' then 'CANALES CONSIGNACION'
											when UPPER(t1.sub_segmento) like '%CANALES%SIMCARDS%(FRANQUICIAS)%' then 'CANALES SIMCARDS (FRANQUICIAS)'
											else UPPER(t1.sub_segmento) end))
		where t1.orden=1
      '''.format(esquema_temp=esquema_temp, tbl_parque_temp=tbl_parque_temp, fecha_eje=fecha_eje)
    return qry


@cargar_consulta
def fun_extraer_datos_preferecnia():
    qry = '''
        SELECT t1.telefono as num_telefonico,
        t1.linea_negocio_homologado, 
        t1.segmento, 
        t1.mb_totales_cobrados_60 as mb60, 
        t1.cantidad_minutos_60 as minutos60
        from db_temporales.otc_t_trafico_tmp1_prod as t1
      '''.format()
    return qry

@cargar_consulta
def fun_extraer_dev_tmp(esquema_temp, abreviatura_temp, tabla, camposl):
    campos=",".join(camposl)
    qry = '''
    SELECT {campos}
    FROM {esquema_temp}.{tabla}{abreviatura_temp} 
    '''.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp, tabla=tabla, campos=campos)
    return qry

@cargar_consulta
def fun_tmp_360_otc_t_dev_bono_combo_1(esquema_temp,abreviatura_temp):
    qry="""
    select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor, sum(cantidad) cantidad 
    from {esquema_temp}.TMP_360_DEV_COMBOS_BONOS_TOTAL_{abreviatura_temp}
    group by upper(marca),num_telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp)
    return qry

@cargar_consulta
def fun_tmp_360_otc_t_dev_bono_combo_2(esquema_temp,abreviatura_temp,fecha_eje1):
    qry="""
    select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor, sum(cantidad) cantidad 
    from {esquema_temp}.TMP_360_DEV_COMBOS_BONOS_TOTAL_{abreviatura_temp} 
    where fec_alta= '{fecha_eje1}'
    group by upper(marca),num_telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp,fecha_eje1=fecha_eje1)
    return qry

@cargar_consulta
def fun_tmp_360_otc_t_dev_bono_1(esquema_temp,abreviatura_temp):
    qry="""
    select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor_bono_periodo, sum(cantidad) cant_bono_periodo 
    from {esquema_temp}.TMP_360_DEV_COMBOS_BONOS_ACUM_{abreviatura_temp}  
    where tipo='BONO'
    group by upper(marca),num_telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp)
    return qry


@cargar_consulta
def fun_tmp_360_otc_t_dev_bono_2(esquema_temp,abreviatura_temp,fecha_eje1):
    qry="""
    select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor_bono_diario, sum(cantidad) cant_bono_diario 
    from {esquema_temp}.TMP_360_DEV_COMBOS_BONOS_ACUM_{abreviatura_temp} 
    where fec_alta= '{fecha_eje1}'
    and tipo='BONO'
    group by upper(marca),num_telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp,fecha_eje1=fecha_eje1)
    return qry

@cargar_consulta
def fun_tmp_360_otc_t_dev_combo_1(esquema_temp,abreviatura_temp):
    qry="""
    select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor_combo_periodo, sum(cantidad) cant_combo_periodo 
    from {esquema_temp}.TMP_360_DEV_COMBOS_BONOS_ACUM_{abreviatura_temp} 
    where tipo='COMBO'
    group by upper(marca),num_telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp)
    return qry

    
@cargar_consulta
def fun_tmp_360_otc_t_dev_combo_2(esquema_temp,abreviatura_temp,fecha_eje1):
    qry="""
    select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor_combo_diario, sum(cantidad) cant_combo_diario 
    from {esquema_temp}.TMP_360_DEV_COMBOS_BONOS_ACUM_{abreviatura_temp} 
    where fec_alta= '{fecha_eje1}'
    and tipo='COMBO'
    group by upper(marca),num_telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp,fecha_eje1=fecha_eje1)
    return qry

@cargar_consulta
def fun_tmp_360_otc_t_dev_combo_1(esquema_temp,abreviatura_temp):
    qry="""
    select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor_combo_periodo, sum(cantidad) cant_combo_periodo 
    from {esquema_temp}.TMP_360_DEV_COMBOS_BONOS_ACUM_{abreviatura_temp} 
    where tipo='COMBO'
    group by upper(marca),num_telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp)
    return qry

@cargar_consulta
def fun_tmp_360_otc_t_dev_sms_1(esquema_temp,abreviatura_temp):
    qry="""
    select upper(marca) as marca,telefono, sum(od_sms) coste_sms_periodo, sum(cantidad) cant_sms_periodo 
    from {esquema_temp}.TMP_360_OTC_T_DEV_SMS_{abreviatura_temp} 
    group by upper(marca),telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp)
    return qry
    
@cargar_consulta
def fun_tmp_360_otc_t_dev_sms_2(esquema_temp,abreviatura_temp,fecha_eje2):
    qry="""
    select upper(marca) as marca,telefono, sum(od_sms) coste_sms_diario, sum(cantidad) cant_sms_diario 
    from {esquema_temp}.TMP_360_OTC_T_DEV_SMS_{abreviatura_temp} 
    where fecha_proceso= {fecha_eje2}
    group by upper(marca),telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp,fecha_eje2=fecha_eje2)
    return qry    

@cargar_consulta
def fun_tmp_360_otc_t_dev_sms_1(esquema_temp,abreviatura_temp):
    qry="""
    select upper(marca) as marca,telefono, sum(od_sms) coste_sms_periodo, sum(cantidad) cant_sms_periodo 
    from {esquema_temp}.TMP_360_OTC_T_DEV_SMS_{abreviatura_temp} 
    group by upper(marca),telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp)
    return qry

@cargar_consulta
def fun_tmp_360_otc_t_dev_voz_1(esquema_temp,abreviatura_temp):
    qry="""
    select upper(marca) as marca,telefono, sum(od_voz) coste_voz_periodo, sum(cant_minutos) cant_min_periodo
    from {esquema_temp}.TMP_360_OTC_T_DEV_VOZ_{abreviatura_temp} 
    group by upper(marca),telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp)
    return qry

@cargar_consulta
def fun_tmp_360_otc_t_dev_voz_2(esquema_temp,abreviatura_temp,fecha_eje2):
    qry="""
    select upper(marca) as marca,telefono, sum(od_voz) coste_voz_diario, sum(cant_minutos) cant_min_diario
    from {esquema_temp}.TMP_360_OTC_T_DEV_VOZ_{abreviatura_temp}
    where fecha_proceso= {fecha_eje2}
    group by upper(marca),telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp,fecha_eje2=fecha_eje2)
    return qry       

@cargar_consulta
def fun_tmp_360_otc_t_dev_datos_1(esquema_temp,abreviatura_temp):
    qry="""
    select upper(marca) as marca,telefono, sum(od_datos) coste_datos_periodo, sum(cantidad_megas) cant_megas_periodo 
    from {esquema_temp}.TMP_360_OTC_T_DEV_DATOS_{abreviatura_temp} 
    group by upper(marca),telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp)
    return qry
    
@cargar_consulta
def fun_tmp_360_otc_t_dev_datos_2(esquema_temp,abreviatura_temp,fecha_eje2):
    qry="""
    select upper(marca) as marca,telefono, sum(od_datos) coste_datos_diario, sum(cantidad_megas) cant_megas_diario 
    from {esquema_temp}.TMP_360_OTC_T_DEV_DATOS_{abreviatura_temp}
    where fecha_proceso= {fecha_eje2}
    group by upper(marca),telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp,fecha_eje2=fecha_eje2)
    return qry         

@cargar_consulta
def fun_tmp_360_otc_t_dev_contenidos_1(esquema_temp,abreviatura_temp):
    qry="""
    select upper(marca) as marca,telefono, sum(cobrado) contenido_periodo, sum(cantidad_eventos) cant_eventos_periodo 
    from {esquema_temp}.TMP_360_OTC_T_DEV_CONTENIDOS_{abreviatura_temp} 
    group by upper(marca),telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp)
    return qry

@cargar_consulta
def fun_tmp_360_otc_t_dev_contenidos_2(esquema_temp,abreviatura_temp,fecha_eje2):
    qry="""
    select upper(marca) as marca,telefono, sum(cobrado) contenido_diario, sum(cantidad_eventos) cant_eventos_diario 
    from {esquema_temp}.TMP_360_OTC_T_DEV_CONTENIDOS_{abreviatura_temp}
    where fecha_proceso= {fecha_eje2}
    group by upper(marca),telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp,fecha_eje2=fecha_eje2)
    return qry         

@cargar_consulta
def fun_tmp_360_otc_t_dev_adelanto_saldo_1(esquema_temp,abreviatura_temp):
    qry="""
    select upper(marca) as marca,telefono, sum(cobrado) adelanto_periodo, sum(cantidad_eventos) cant_adelantos_periodo 
    from {esquema_temp}.tmp_360_adelanto_saldo_{abreviatura_temp}
    group by upper(marca),telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp)
    return qry

@cargar_consulta
def fun_tmp_360_otc_t_dev_adelanto_saldo_2(esquema_temp,abreviatura_temp,fecha_eje2):
    qry="""
    select upper(marca) as marca,telefono, sum(cobrado) adelanto_diario, sum(cantidad_eventos) cant_adelantos_diario 
    from {esquema_temp}.tmp_360_adelanto_saldo_{abreviatura_temp}
    where fecha= {fecha_eje2}
    group by upper(marca),telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp,fecha_eje2=fecha_eje2)
    return qry     
 
@cargar_consulta
def fun_tmp_360_otc_t_dev_buzon_voz_acumulado_1(esquema_temp,abreviatura_temp):
    qry="""
    select upper(marca) as marca,num_telefono, sum(valor_sin_iva) coste_buzon_periodo, sum(cantidad) cant_buzon_periodo 
    from {esquema_temp}.TMP_360_OTC_T_DEV_BUZON_VOZ_ACUMULADO_{abreviatura_temp} 
    group by upper(marca),num_telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp)
    return qry 
 
@cargar_consulta
def fun_tmp_360_otc_t_dev_buzon_voz_diario_1(esquema_temp,abreviatura_temp,fecha_eje2):
    qry="""
    select upper(marca) as marca,num_telefono, sum(valor_sin_iva) coste_buzon_diario, sum(cantidad) cant_buzon_diario 
    from {esquema_temp}.TMP_360_OTC_T_DEV_BUZON_VOZ_DIARIO_{abreviatura_temp}
    where fecha= {fecha_eje2}
    group by upper(marca),num_telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp,fecha_eje2=fecha_eje2)
    return qry      

@cargar_consulta
def fun_tmp_360_otc_t_dev_llamada_espera_acumulado_1(esquema_temp,abreviatura_temp):
    qry="""
    select upper(marca) as marca,num_telefono, sum(valor_sin_iva) coste_llamada_periodo, sum(cantidad) cant_llamada_periodo 
    from {esquema_temp}.tmp_360_otc_t_dev_llamada_espera_acumulado_{abreviatura_temp} 
    group by upper(marca),num_telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp)
    return qry 

@cargar_consulta 
def fun_tmp_360_otc_t_dev_llamada_espera_diario_1(esquema_temp,abreviatura_temp,fecha_eje2):
    qry="""
    select upper(marca) as marca,num_telefono, sum(valor_sin_iva) coste_llamada_diario, sum(cantidad) cant_llamada_diario 
    from {esquema_temp}.tmp_360_otc_t_dev_llamada_espera_diario_{abreviatura_temp}
    where fecha= {fecha_eje2}
    group by upper(marca),num_telefono
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp,fecha_eje2=fecha_eje2)
    return qry       

@cargar_consulta
def fun_tmp_otc_t_360_devengos(esquema_temp,abreviatura_temp):
    qry="""
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
    from {esquema_temp}.TMP_360_OTC_T_UNIVERSO_DEV_UNICOS_{abreviatura_temp} a 
    left join {esquema_temp}.TMP_360_OTC_T_DEV_SMS_1_{abreviatura_temp} b 
    on a.telefono=b.telefono and upper(a.marca)=upper(b.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_SMS_2_{abreviatura_temp} c
    on a.telefono=c.telefono and upper(a.marca)=upper(c.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_VOZ_1_{abreviatura_temp} d
    on a.telefono=d.telefono and upper(a.marca)=upper(d.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_VOZ_2_{abreviatura_temp} e
    on a.telefono=e.telefono and upper(a.marca)=upper(e.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_DATOS_1_{abreviatura_temp} f
    on a.telefono=f.telefono and upper(a.marca)=upper(f.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_DATOS_2_{abreviatura_temp} g
    on a.telefono=g.telefono and upper(a.marca)=upper(g.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_CONTENIDOS_1_{abreviatura_temp} h
    on a.telefono=h.telefono and upper(a.marca)=upper(h.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_CONTENIDOS_2_{abreviatura_temp} i
    on a.telefono=i.telefono and upper(a.marca)=upper(i.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_BUZON_VOZ_ACUMULADO_1_{abreviatura_temp} j
    on a.telefono=j.num_telefono and upper(a.marca)=upper(j.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_BUZON_VOZ_DIARIO_1_{abreviatura_temp} k
    on a.telefono=k.num_telefono and upper(a.marca)=upper(k.marca)
    left join {esquema_temp}.tmp_360_otc_t_dev_llamada_espera_acumulado_1_{abreviatura_temp} lla
    on a.telefono=lla.num_telefono and upper(a.marca)=upper(lla.marca)
    left join {esquema_temp}.tmp_360_otc_t_dev_llamada_espera_diario_1_{abreviatura_temp} lld
    on a.telefono=lld.num_telefono and upper(a.marca)=upper(lld.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_BONO_1_{abreviatura_temp} l
    on a.telefono=l.telefono and upper(a.marca)=upper(l.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_BONO_2_{abreviatura_temp} m
    on a.telefono=m.telefono and upper(a.marca)=upper(m.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_COMBO_1_{abreviatura_temp} n
    on a.telefono=n.telefono and upper(a.marca)=upper(n.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_COMBO_2_{abreviatura_temp} o
    on a.telefono=o.telefono and upper(a.marca)=upper(o.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_BONO_COMBO_1_{abreviatura_temp} p
    on a.telefono=p.telefono and upper(a.marca)=upper(p.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_BONO_COMBO_2_{abreviatura_temp} q
    on a.telefono=q.telefono and upper(a.marca)=upper(q.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_ADELANTO_SALDO_1_{abreviatura_temp} r
    on a.telefono=r.telefono and upper(a.marca)=upper(r.marca)
    left join {esquema_temp}.TMP_360_OTC_T_DEV_ADELANTO_SALDO_2_{abreviatura_temp} s
    on a.telefono=s.telefono and upper(a.marca)=upper(s.marca)
    """.format(esquema_temp=esquema_temp, abreviatura_temp=abreviatura_temp)
    return qry       
 
@cargar_consulta
def fun_otc_t_360_devengos(esquema_temp,tabla_tmp):
    qry="""
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
    t1.cantidad_llamada_diario
    from {esquema_temp}.{tabla_tmp} t1
    """.format(esquema_temp=esquema_temp,tabla_tmp=tabla_tmp)
    return qry