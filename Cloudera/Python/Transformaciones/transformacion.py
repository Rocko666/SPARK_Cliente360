from Configuraciones.configuracion import *
from Querys.sql import *
from Funciones.funcion import *
from pyspark.sql.functions import col, substring, max, min, when, count, sum, lit, unix_timestamp, last_day, upper, date_format, to_date
from pyspark.sql.types import StringType
import pandas as pd
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime, timedelta


@seguimiento_transformacion
# Cargamos las llamadas entrantes de la Competencia
def fun_cargar_devengos_diameter(sqlContext, fecha_incial, fecha_final):

    # extraemos los datos de la tabla bloqueo de llamadas
    df_datos_diameter = fun_extraer_datos_diameter(sqlContext, val_base_diameter, val_tabla_diameter,
                                                               fecha_incial, fecha_final)

    # extraemos los datos de la tabla numeracion
    df_catalogo_plan = fun_extraer_catalogos_plan(sqlContext, val_base_reportes, val_tabla_plan)

    # join entre los registros que no tienen operadora y la tabla de numeracion y anadimos el campo operadora
    t1 = df_datos_diameter.alias('t1')
    t2 = df_catalogo_plan.alias('t2')

    df_diamter = t1.join(t2, t1.TIP_PREPAGO == t2.codigo, how='inner').\
        selectExpr('t1.MSISDN as telefono', 't1.feh_llamada as Fecha_proceso', 't2.marca', 't1.od_datos', 't1.cantidad_megas')

    # Borrar tabla
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_tmp_datos + val_abreviatura_temp)
    # Cargar LLamdas Entrates
    fun_cargar_datos(sqlContext, df_diamter, val_esquema_temp, val_tabla_tmp_datos + val_abreviatura_temp)
    print("INFO: Total registros tabla "+val_tabla_tmp_datos + val_abreviatura_temp +": "+str(df_diamter.count()))
    return 0
	
def fun_cargar_devengos_mecorig(sqlContext, fecha_incial, fecha_final):

    # extraemos los datos de la tabla bloqueo de llamadas
    df_datos_mecorig = fun_extraer_datos_mecorig(sqlContext, val_base_altamira, val_tabla_mecorig,
                                                               fecha_incial, fecha_final)

    # extraemos los datos de la tabla numeracion
    df_catalogo_plan = fun_extraer_catalogos_plan(sqlContext, val_base_reportes, val_tabla_plan)

    # join entre los registros que no tienen operadora y la tabla de numeracion y anadimos el campo operadora
    t1 = df_datos_mecorig.alias('t1')
    t2 = df_catalogo_plan.alias('t2')

    df_mecorig = t1.join(t2, t1.TIP_PREPAGO == t2.codigo, how='inner').\
        selectExpr('t1.MSISDN as telefono', 't1.fecha as Fecha_proceso', 't2.marca', 't1.od_sms', 't1.cantidad')

    # Borrar tabla
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_tmp_sms + val_abreviatura_temp)

    # Cargar LLamdas Entrates
    fun_cargar_datos(sqlContext, df_mecorig, val_esquema_temp, val_tabla_tmp_sms + val_abreviatura_temp)
    print("INFO: Total registros tabla "+val_tabla_tmp_sms + val_abreviatura_temp +": "+str(df_mecorig.count()))
    return 0

def fun_cargar_devengos_llamadas(sqlContext, fecha_incial, fecha_final):

    # extraemos los datos de la tabla bloqueo de llamadas
    df_datos_llamadas = fun_extraer_datos_llamadas(sqlContext, val_base_altamira, val_tabla_llamadas,
                                                               fecha_incial, fecha_final)

    # extraemos los datos de la tabla numeracion
    df_catalogo_plan = fun_extraer_catalogos_plan(sqlContext, val_base_reportes, val_tabla_plan)

    # join entre los registros que no tienen operadora y la tabla de numeracion y anadimos el campo operadora
    t1 = df_datos_llamadas.alias('t1')
    t2 = df_catalogo_plan.alias('t2')

    df_llamadas = t1.join(t2, t1.TIP_PREPAGO == t2.codigo, how='inner').\
        selectExpr('t1.MSISDN as telefono', 't1.fecha as Fecha_proceso', 't2.marca', 't1.od_voz', 't1.cant_minutos')

    # Borrar tabla
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_tmp_voz + val_abreviatura_temp)

    # Cargar LLamdas Entrates
    fun_cargar_datos(sqlContext, df_llamadas, val_esquema_temp, val_tabla_tmp_voz + val_abreviatura_temp)
    print("INFO: Total registros tabla "+val_tabla_tmp_voz + val_abreviatura_temp +": "+str(df_llamadas.count()))
    return 0

def fun_cargar_devengos_contenidos(sqlContext, fecha_incial, fecha_final):

    # extraemos los datos de la tabla bloqueo de llamadas
    df_datos_contenidos = fun_extraer_datos_contenidos(sqlContext, val_base_altamira, val_tabla_content,
                                                               fecha_incial, fecha_final)

    # extraemos los datos de la tabla numeracion
    df_catalogo_plan = fun_extraer_catalogos_plan(sqlContext, val_base_reportes, val_tabla_plan)

    # join entre los registros que no tienen operadora y la tabla de numeracion y anadimos el campo operadora
    t1 = df_datos_contenidos.alias('t1')
    t2 = df_catalogo_plan.alias('t2')

    df_contenidos = t1.join(t2, t1.TIP_PREPAGO == t2.codigo, how='inner').\
        selectExpr('t1.MSISDN as telefono', 't1.fecha as Fecha_proceso', 't2.marca', 't1.cobrado', 't1.cantidad_eventos')

    # Borrar tabla
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_tmp_contenidos + val_abreviatura_temp)

    # Cargar LLamdas Entrates
    fun_cargar_datos(sqlContext, df_contenidos, val_esquema_temp, val_tabla_tmp_contenidos + val_abreviatura_temp)
    print("INFO: Total registros tabla "+val_tabla_tmp_contenidos + val_abreviatura_temp +": "+str(df_contenidos.count()))
    return 0

def fun_cargar_devengos_adelanto_saldo(sqlContext, fecha_incial, fecha_final):

    # extraemos los datos de la tabla bloqueo de llamadas
    df_datos_adelanto_saldo = fun_extraer_datos_adelanto_saldo(sqlContext, val_base_altamira, val_tabla_content,
                                                               fecha_incial, fecha_final)

    # extraemos los datos de la tabla numeracion
    df_catalogo_plan = fun_extraer_catalogos_plan(sqlContext, val_base_reportes, val_tabla_plan)

    # join entre los registros que no tienen operadora y la tabla de numeracion y anadimos el campo operadora
    t1 = df_datos_adelanto_saldo.alias('t1')
    t2 = df_catalogo_plan.alias('t2')

    df_adelanto_saldo = t1.join(t2, t1.TIP_PREPAGO == t2.codigo, how='inner').\
        selectExpr('t1.MSISDN as telefono', 't1.fecha', 't2.marca', 't1.cobrado', 't1.cantidad_eventos')

    # Borrar tabla
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_tmp_adelanto_saldo + val_abreviatura_temp)

    # Cargar LLamdas Entrates
    fun_cargar_datos(sqlContext, df_adelanto_saldo, val_esquema_temp, val_tabla_tmp_adelanto_saldo + val_abreviatura_temp)
    print("INFO: Total registros tabla "+val_tabla_tmp_adelanto_saldo + val_abreviatura_temp +": "+str(df_adelanto_saldo.count()))
    return 0

def fun_cargar_devengos_buzon_voz_diario(sqlContext, fecha_incial, fecha_final, fecha_ejecucion, fecha_cmb_co, cod_act, cod_us):

    # extraemos los datos de la tabla bloqueo de llamadas
    df_datos_buzon_voz_diario = fun_extraer_datos_buzon_voz_diario(sqlContext, val_base_rdb, val_tabla_actabopre,
                                                               fecha_incial, fecha_final, val_fecha_cambio_buzon, 
                                                               fecha_cmb_co, cod_act, cod_us)

    # extraemos los datos de la tabla numeracion
    df_catalogo_plan_operadora = fun_extraer_catalogos_plan_operadora(sqlContext, val_base_altamira, val_tabla_plan_operadora)

    # join entre los registros que no tienen operadora y la tabla de numeracion y anadimos el campo operadora
    t1 = df_datos_buzon_voz_diario.alias('t1')
    t2 = df_catalogo_plan_operadora.alias('t2')

    df_buzon_voz_diario = t1.join(t2, t1.COD_TIPPREPA == t2.id_plan, how='left').\
        selectExpr('t1.num_telefono as num_telefono','t1.fecha as fecha', 'case when t2.marca is null then "Movistar" else t2.marca end as marca', 't1.cantidad as cantidad','((t1.valor/1000)/1.12) as valor_sin_iva')

    t3 = df_buzon_voz_diario.alias('t3')
    df_buzon_voz_acumulado = t3.groupBy('t3.num_telefono', 't3.marca').\
            agg(sum(t3.cantidad).alias('cantidad'),sum(t3.valor_sin_iva).alias('valor_sin_iva'))

    # Borrar tabla
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_tmp_buzon_voz + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_tmp_buzon_voz_acum + val_abreviatura_temp)
    #fun_borrar_tabla(sqlContext, val_esquema_temp, 'bb_buzon_voz')

    # Cargar LLamdas Entrates
    fun_cargar_datos(sqlContext, df_buzon_voz_diario, val_esquema_temp, val_tabla_tmp_buzon_voz + val_abreviatura_temp)
    fun_cargar_datos(sqlContext, df_buzon_voz_acumulado, val_esquema_temp, val_tabla_tmp_buzon_voz_acum + val_abreviatura_temp)
    print("INFO: Total registros tabla "+val_tabla_tmp_buzon_voz + val_abreviatura_temp +": "+str(df_buzon_voz_diario.count()))
    print("INFO: Total registros tabla "+val_tabla_tmp_buzon_voz_acum + val_abreviatura_temp +": "+str(df_buzon_voz_acumulado.count()))
    #fun_cargar_datos(sqlContext, df_datos_buzon_voz_diario, val_esquema_temp, 'bb_buzon_voz')
    return 0
    
def fun_cargar_devengos_llamada_espera_diario(sqlContext, fecha_incial, fecha_final, fecha_ejecucion, cod_act, cod_us):

    # extraemos los datos de la tabla bloqueo de llamadas
    df_datos_llamada_espera_diario = fun_extraer_datos_llamada_espera_diario(sqlContext, val_base_rdb, val_tabla_actabopre,
                                                               fecha_incial, fecha_final, cod_act, cod_us)

    # extraemos los datos de la tabla numeracion
    df_catalogo_plan_operadora = fun_extraer_catalogos_plan_operadora(sqlContext, val_base_altamira, val_tabla_plan_operadora)

    # join entre los registros que no tienen operadora y la tabla de numeracion y anadimos el campo operadora
    t1 = df_datos_llamada_espera_diario.alias('t1')
    t2 = df_catalogo_plan_operadora.alias('t2')

    df_llamada_espera_diario = t1.join(t2, t1.COD_TIPPREPA == t2.id_plan, how='left').\
        selectExpr('t1.num_telefono as num_telefono','t1.fecha as fecha', 'case when t2.marca is null then "Movistar" else t2.marca end as marca', 't1.cantidad as cantidad','((t1.valor/1000)/1.12) as valor_sin_iva')

    t3 = df_llamada_espera_diario.alias('t3')
    df_llamada_espera_acumulado = t3.groupBy('t3.num_telefono', 't3.marca').\
            agg(sum(t3.cantidad).alias('cantidad'),sum(t3.valor_sin_iva).alias('valor_sin_iva'))

    # Borrar tabla
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_tmp_llamada_espera + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_tmp_llamada_espera_acum + val_abreviatura_temp)
    #fun_borrar_tabla(sqlContext, val_esquema_temp, 'bb_buzon_voz')

    # Cargar LLamdas Entrates
    fun_cargar_datos(sqlContext, df_llamada_espera_diario, val_esquema_temp, val_tabla_tmp_llamada_espera + val_abreviatura_temp)
    fun_cargar_datos(sqlContext, df_llamada_espera_acumulado, val_esquema_temp, val_tabla_tmp_llamada_espera_acum + val_abreviatura_temp)
    #fun_cargar_datos(sqlContext, df_datos_buzon_voz_diario, val_esquema_temp, 'bb_buzon_voz')
    print("INFO: Total registros tabla "+val_tabla_tmp_llamada_espera + val_abreviatura_temp +": "+str(df_llamada_espera_diario.count()))
    print("INFO: Total registros tabla "+val_tabla_tmp_llamada_espera_acum + val_abreviatura_temp +": "+str(df_llamada_espera_acumulado.count()))
    return 0

def fun_cargar_devengos_combos_bonos(sqlContext, fecha_incial, fecha_final):

    # extraemos los datos de la tabla bloqueo de llamadas
    df_datos_combos_bonos = fun_extraer_combos_bonos(sqlContext, fecha_incial, fecha_final)
    
    df_catalogo_bonos_pdv = fun_extraer_catalogo_bonos_pdv(sqlContext, val_base_reportes, val_tabla_cat_bonos_pdv)

    # join entre los registros que no tienen operadora y la tabla de numeracion y anadimos el campo operadora
    t1 = df_datos_combos_bonos.alias('t1')
    
    df_combos_bonos = t1.selectExpr('t1.cod_bono','t1.num_telefono','t1.fec_alta','t1.cod_usuario','t1.marca','t1.valor_bono')
    
    t2 = df_combos_bonos.alias('t2')
    t3 = df_catalogo_bonos_pdv.alias('t3')
    
    df_combos_bonos_acum = t2.join(t3, t3.bono == t2.cod_bono, how='inner').filter("t2.cod_usuario <> 'PROM'").\
        selectExpr('t2.cod_bono','t2.num_telefono','t2.fec_alta','t2.cod_usuario','t2.valor_bono','t3.tipo','t3.valor_con_iva',\
                       'case when (t3.marca is null or t3.marca LIKE "%TELEFONICA%") then "Movistar" else t3.marca end as marca',\
                           'case when t2.valor_bono>0 then "DEVENGO" else "DEVENGO CANALES ELECTRONICOS" end as fuente')
        
    t4 = df_combos_bonos_acum.alias('t4')
    
    df_combos_bonos_acum_aggr = t4.groupBy('t4.fec_alta', 't4.num_telefono','t4.tipo','t4.marca','t4.fuente')\
        .agg(count('t4.cod_bono').alias('cantidad'),sum(t4.valor_con_iva/1.12).alias('valor'))
        
    t5 = df_combos_bonos_acum_aggr.alias('t5')
    
    df_combos_bonos_total = t5.groupBy('t5.fec_alta', 't5.num_telefono','t5.marca')\
        .agg(sum(t5.cantidad).alias('cantidad'),sum(t5.valor).alias('valor'))

    # Borrar tabla
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_tmp_combos_bonos + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_tmp_combos_bonos_acum + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_tmp_combos_bonos_total + val_abreviatura_temp)

    # Cargar LLamdas Entrates
    fun_cargar_datos(sqlContext, df_combos_bonos, val_esquema_temp, val_tabla_tmp_combos_bonos + val_abreviatura_temp)
    fun_cargar_datos(sqlContext, df_combos_bonos_acum_aggr, val_esquema_temp, val_tabla_tmp_combos_bonos_acum + val_abreviatura_temp)
    fun_cargar_datos(sqlContext, df_combos_bonos_total, val_esquema_temp, val_tabla_tmp_combos_bonos_total + val_abreviatura_temp)
    print("INFO: Total registros tabla "+val_tabla_tmp_combos_bonos + val_abreviatura_temp +": "+str(df_combos_bonos.count()))
    print("INFO: Total registros tabla "+val_tabla_tmp_combos_bonos_acum + val_abreviatura_temp +": "+str(df_combos_bonos_acum_aggr.count()))
    print("INFO: Total registros tabla "+val_tabla_tmp_combos_bonos_total + val_abreviatura_temp +": "+str(df_combos_bonos_total.count()))
    return 0

def fun_cargar_parque(sqlContext, fecha_alt_ini, fecha_alt_fin, fecha_proc, fecha_eje_pv, fecha_menos_5, fecha_mas_1, \
                      fecha_alt_dos_meses_ant_fin, fecha_alt_dos_meses_ant_ini, fecha_ini_mes, fecha_inac_1):

    # extraemos los datos de la tabla bloqueo de llamadas
    
    df_datos_parque = fun_extraer_movi_parque(sqlContext, fecha_alt_ini, fecha_alt_fin, fecha_proc, fecha_eje_pv, "and t.estado_abonado not in ('BAA')")
    
    df_datos_parque_inac = fun_extraer_movi_parque(sqlContext, fecha_alt_dos_meses_ant_fin, fecha_alt_dos_meses_ant_ini, fecha_ini_mes, fecha_eje_pv, "and 1=1")
    
    df_datos_cuenta_cliente = fun_extraer_cuenta_cliente(sqlContext)
    
    df_datos_planes_categoria = fun_extraer_planes_categoria(sqlContext)
    
    df_datos_churn = fun_extraer_churn(sqlContext, fecha_menos_5, fecha_mas_1)
    df_datos_churn_dia = fun_extraer_churn_dia(sqlContext)
    
    df_datos_churn_inac = fun_extraer_churn_inac(sqlContext, fecha_inac_1)
    

    t1 = df_datos_parque.alias('t1')
    t2 = df_datos_cuenta_cliente.alias('t2')
    t3 = df_datos_planes_categoria.alias('t3')
    t4 = df_datos_churn.alias('t4')
    t5 = df_datos_churn_dia.alias('t5')
    
    
    df_churn_dia_unico = t5.join(t4, t5.num_telefonico == t4.num_telefonico, how='left').filter('t4.num_telefonico is null')\
        .selectExpr('t5.num_telefonico','t5.counted_days','"churn" as fuente')
        
    t6 = df_churn_dia_unico.alias('t6')      
        
    df_union_churn = t6.union(t4)
    
    t7 = df_union_churn.alias('t7')
            
    df_parque_act = t1.join(t2, t1.account_num == t2.cta_facturacion, how='left').\
        join(t3, t1.codigo_plan==t3.cod_plan_activo, how='left').\
        join(t7, t1.num_telefonico == t7.num_telefonico, how='left').\
        selectExpr('t1.num_telefonico','t1.codigo_plan','t1.fecha_alta','t1.fecha_last_status','t1.estado_abonado',\
                   't1.fecha_proceso','t1.numero_abonado','t1.linea_negocio','t1.account_num','t1.sub_segmento',\
                   't1.tipo_doc_cliente','t1.identificacion_cliente','t1.cliente','coalesce(t2.cliente_id,"") as customer_ref',\
				   't1.linea_negocio_homologado','t1.marca','t1.ciclo_fact','t1.correo_cliente_pr',\
                   't1.telefono_cliente_pr','t1.imei','t1.orden',\
                   't3.categoria as categoria_plan','t3.tarifa_basica as tarifa','t3.des_plan_tarifario as nombre_plan',\
                   't7.counted_days')
              
    t9 = df_datos_parque_inac.alias('t9')
    t8 = df_datos_churn_inac.alias('t8')
    
    df_parque_inac_prev = t9.join(t2, t9.account_num == t2.cta_facturacion, how='left').\
        join(t3, t9.codigo_plan==t3.cod_plan_activo, how='left').\
        join(t8, t9.num_telefonico == t8.num_telefonico, how='left').\
        selectExpr('t9.num_telefonico','t9.codigo_plan','t9.fecha_alta','t9.fecha_last_status','"BAA" as estado_abonado',\
                   't9.fecha_proceso','t9.numero_abonado','t9.linea_negocio','t9.account_num','t9.sub_segmento',\
                   't9.tipo_doc_cliente','t9.identificacion_cliente','t9.cliente','coalesce(t2.cliente_id,"") as customer_ref',\
				   't9.linea_negocio_homologado','t9.marca','t9.ciclo_fact','t9.correo_cliente_pr',\
                   't9.telefono_cliente_pr','t9.imei','t9.orden',\
                   't3.categoria as categoria_plan','t3.tarifa_basica as tarifa','t3.des_plan_tarifario as nombre_plan',\
                   't8.counted_days')
            
    t10 = df_parque_inac_prev.alias('t10')
    
    df_parque_inac_bajas_trf = fun_extraer_parque_inac(sqlContext);
    
    t11 = df_parque_inac_bajas_trf.alias('t11')
    
    df_parque_inac = t10.join(t11, t10.num_telefonico==t11.telefono, how='inner').\
        selectExpr('t10.num_telefonico','t10.codigo_plan','t10.fecha_alta','t10.fecha_last_status','t10.estado_abonado',\
                   't10.fecha_proceso','t10.numero_abonado','t10.linea_negocio','t10.account_num','t10.sub_segmento',\
                   't10.tipo_doc_cliente','t10.identificacion_cliente','t10.cliente','t10.customer_ref',\
				   't10.linea_negocio_homologado','t10.marca','t10.ciclo_fact','t10.correo_cliente_pr',\
                   't10.telefono_cliente_pr','t10.imei','t10.orden',\
                   't10.categoria_plan','t10.tarifa','t10.nombre_plan',\
                   't10.counted_days')
                
    # Borrar tabla                  
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_parque_2 + val_abreviatura_temp)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_parque_inac + val_abreviatura_temp)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_churn_dia + val_abreviatura_temp)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_churn_unida + val_abreviatura_temp)
    #fun_borrar_tabla(sqlContext, val_esquema_temp, 'bb_prueba_parque_inac')
    #fun_cargar_datos(sqlContext, df_parque_inac_prev, val_esquema_temp, 'bb_prueba_parque_inac')
    
    #fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_movi_parque + val_abreviatura_temp)
    
    #fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_cuenta_cliente + val_abreviatura_temp)
    
    #fun_borrar_tabla(sqlContext, val_esquema_temp, val_tabla_planes_categoria + val_abreviatura_temp)

    # Cargar LLamdas Entrates
    fun_cargar_datos(sqlContext, df_parque_act, val_esquema_temp, val_tabla_parque_2 + val_abreviatura_temp)
    
    fun_cargar_datos(sqlContext, df_parque_inac, val_esquema_temp, val_tabla_parque_inac + val_abreviatura_temp)
    
    fun_cargar_datos(sqlContext, df_datos_churn_dia, val_esquema_temp, val_tabla_churn_dia + val_abreviatura_temp)
    
    fun_cargar_datos(sqlContext, df_union_churn, val_esquema_temp, val_tabla_churn_unida + val_abreviatura_temp)
    
    print("INFO: Total registros tabla "+val_tabla_parque_2 + val_abreviatura_temp +": "+str(df_parque_act.count()))
    print("INFO: Total registros tabla "+val_tabla_parque_inac + val_abreviatura_temp +": "+str(df_parque_inac.count()))
    print("INFO: Total registros tabla "+val_tabla_churn_dia + val_abreviatura_temp +": "+str(df_datos_churn_dia.count()))
    print("INFO: Total registros tabla "+val_tabla_churn_unida + val_abreviatura_temp +": "+str(df_union_churn.count()))
    
    #fun_cargar_datos(sqlContext, df_datos_parque, val_esquema_temp, val_tabla_movi_parque + val_abreviatura_temp)
    
    #fun_cargar_datos(sqlContext, df_datos_cuenta_cliente, val_esquema_temp, val_tabla_cuenta_cliente + val_abreviatura_temp)
    
    #fun_cargar_datos(sqlContext, df_datos_planes_categoria, val_esquema_temp, val_tabla_planes_categoria + val_abreviatura_temp)
    return 0

def fun_cargar_trafico(sqlContext, fecha_menos_1_mes, fecha_menos_2_mes, fecha_eje, fecha_ini_mes):

    # extraemos los datos de la tabla bloqueo de llamadas
    df_cur_t2_tecno = fun_extraer_cur_t2(sqlContext, fecha_menos_2_mes, fecha_eje)
    
    df_cursado_sms = fun_extraer_xdrcursado_sms(sqlContext, fecha_menos_1_mes, fecha_eje)
    
    df_ppcs_llamadas = fun_extraer_ppcs_llamadas(sqlContext, fecha_menos_2_mes, fecha_eje)
    
    df_costed_event_1 = fun_extraer_costed_event_type_1(sqlContext, fecha_menos_2_mes, fecha_eje)
    
    df_costed_event_3 = fun_extraer_costed_event_type_3(sqlContext, fecha_menos_1_mes, fecha_eje)
    
    df_ppcs_diameter = fun_extraer_ppcs_diameter(sqlContext, fecha_menos_1_mes, fecha_eje)

    # join entre los registros que no tienen operadora y la tabla de numeracion y anadimos el campo operadora
    t1 = df_cur_t2_tecno.alias('t1').cache()
    t2 = df_cursado_sms.alias('t2')
    t3 = df_ppcs_llamadas.alias('t3')
    t4 = df_costed_event_1.alias('t4')
    t5 = df_costed_event_3.alias('t5')
    t6 = df_ppcs_diameter.alias('t6')
    
    df_total_mb_tecno = t1.selectExpr('t1.telefono','t1.fecha','total_2g','total_3g','total_4g')
    
    df_cantidad_minutos = t3.union(t4)
    
    t7 = df_cantidad_minutos.alias('t7')
    
    df_agrupa_minutos = t7.groupBy('t7.numeroorigenllamada', 't7.fecha_proceso')\
        .agg(sum(t7.cantidad_minutos).alias('cantidad_minutos'))
        
    df_cantidad_megas = t5.union(t6)
    
    t8 = df_cantidad_megas.alias('t8').cache()
    
    t9 = df_agrupa_minutos.alias('t9').cache()
    
    df_parque_trafico = fun_extraer_parque_trafico(sqlContext, val_esquema_temp, 'otc_t_360_parque_1_tmp' , fecha_eje)
    
    df_agrupa_megas_dia = t8.filter(col('t8.fecha') == fecha_eje).groupBy('t8.num_telefono').agg(sum(t8.total_mb).alias('total_mb'))
    
    df_agrupa_megas_mes = t8.filter(col('t8.fecha') > fecha_menos_1_mes).groupBy('t8.num_telefono').agg(sum(t8.total_mb).alias('total_mb'))
    
    df_agrupa_megas_2_mes = t8.groupBy('t8.num_telefono').agg(sum(t8.total_mb).alias('total_mb'))
    
    df_agrupa_megas_mes_curso = t8.filter(col('t8.fecha') >= fecha_ini_mes).groupBy('t8.num_telefono').agg(sum(t8.total_mb).alias('total_mb'))    
    
    agrupa_tecno_dia = t1.filter(col('t1.fecha') == fecha_eje).groupBy('t1.telefono').agg(sum(t1.total_2g).alias('total_2g_dia'),sum(t1.total_3g).alias('total_3g_dia'),sum(t1.total_4g).alias('total_4g_dia'))
        
    agrupa_tecno_mes = t1.filter(col('t1.fecha') > fecha_menos_1_mes).groupBy('t1.telefono').agg(sum(t1.total_2g).alias('total_2g_mes'),sum(t1.total_3g).alias('total_3g_mes'),sum(t1.total_4g).alias('total_4g_mes'))
        
    agrupa_tecno_2_mes = t1.groupBy('t1.telefono').agg(sum(t1.total_2g).alias('total_2g_mes_60'),sum(t1.total_3g).alias('total_3g_mes_60'),sum(t1.total_4g).alias('total_4g_mes_60'))
    
    agrupa_tecno_mes_curso = t1.filter(col('t1.fecha') >= fecha_ini_mes).groupBy('t1.telefono').agg(sum(t1.total_2g).alias('total_2g'),sum(t1.total_3g).alias('total_3g'),sum(t1.total_4g).alias('total_4g'))
    
    agrupa_voz_dia = t9.filter(col('t9.fecha_proceso') == fecha_eje).groupBy('t9.numeroorigenllamada').agg(sum(t9.cantidad_minutos).alias('total_min'))
        
    agrupa_voz_mes = t9.filter(col('t9.fecha_proceso') > fecha_menos_1_mes).groupBy('t9.numeroorigenllamada').agg(sum(t9.cantidad_minutos).alias('total_min'))
        
    agrupa_voz_2_mes = t9.groupBy('t9.numeroorigenllamada').agg(sum(t9.cantidad_minutos).alias('total_min'))
    
    agrupa_voz_mes_curso = t9.filter(col('t9.fecha_proceso') >= fecha_ini_mes).groupBy('t9.numeroorigenllamada').agg(sum(t9.cantidad_minutos).alias('total_min'))
       
    #bba db_temporales db_desarrollo2021
    # Borrar tabla    
    fun_borrar_tabla(sqlContext, val_esquema_temp, 'tmp_otc_t_360_parque_trafico_' + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, 'tmp_otc_t_360_megas_dia_' + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, 'tmp_otc_t_360_megas_mes_' + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, 'tmp_otc_t_360_megas_2_mes_' + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, 'tmp_otc_t_360_megas_mes_curso_' + val_abreviatura_temp)    
    fun_borrar_tabla(sqlContext, val_esquema_temp, 'tmp_otc_t_360_trafico_tecno_dia_' + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, 'tmp_otc_t_360_trafico_tecno_mes_' + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, 'tmp_otc_t_360_trafico_tecno_2_mes_' + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, 'tmp_otc_t_360_trafico_tecno_mes_curso_' + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, 'tmp_otc_t_360_voz_dia_' + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, 'tmp_otc_t_360_voz_mes_' + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, 'tmp_otc_t_360_voz_2_mes_' + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, 'tmp_otc_t_360_voz_mes_curso_' + val_abreviatura_temp)
    fun_borrar_tabla(sqlContext, val_esquema_temp, 'tmp_otc_t_360_sms_' + val_abreviatura_temp)

    # Cargar a hive
    fun_cargar_datos(sqlContext, df_parque_trafico, val_esquema_temp, 'tmp_otc_t_360_parque_trafico_' + val_abreviatura_temp)
    
    fun_cargar_datos(sqlContext, df_agrupa_megas_dia, val_esquema_temp, 'tmp_otc_t_360_megas_dia_' + val_abreviatura_temp)
    fun_cargar_datos(sqlContext, df_agrupa_megas_mes, val_esquema_temp, 'tmp_otc_t_360_megas_mes_' + val_abreviatura_temp)
    fun_cargar_datos(sqlContext, df_agrupa_megas_2_mes, val_esquema_temp, 'tmp_otc_t_360_megas_2_mes_' + val_abreviatura_temp)
    fun_cargar_datos(sqlContext, df_agrupa_megas_mes_curso, val_esquema_temp, 'tmp_otc_t_360_megas_mes_curso_' + val_abreviatura_temp)
    
    fun_cargar_datos(sqlContext, agrupa_tecno_dia, val_esquema_temp, 'tmp_otc_t_360_trafico_tecno_dia_' + val_abreviatura_temp)
    fun_cargar_datos(sqlContext, agrupa_tecno_mes, val_esquema_temp, 'tmp_otc_t_360_trafico_tecno_mes_' + val_abreviatura_temp)
    fun_cargar_datos(sqlContext, agrupa_tecno_2_mes, val_esquema_temp, 'tmp_otc_t_360_trafico_tecno_2_mes_' + val_abreviatura_temp)
    fun_cargar_datos(sqlContext, agrupa_tecno_mes_curso, val_esquema_temp, 'tmp_otc_t_360_trafico_tecno_mes_curso_' + val_abreviatura_temp)
        
    fun_cargar_datos(sqlContext, agrupa_voz_dia, val_esquema_temp, 'tmp_otc_t_360_voz_dia_' + val_abreviatura_temp)
    fun_cargar_datos(sqlContext, agrupa_voz_mes, val_esquema_temp, 'tmp_otc_t_360_voz_mes_' + val_abreviatura_temp)
    fun_cargar_datos(sqlContext, agrupa_voz_2_mes, val_esquema_temp, 'tmp_otc_t_360_voz_2_mes_' + val_abreviatura_temp)
    fun_cargar_datos(sqlContext, agrupa_voz_mes_curso, val_esquema_temp, 'tmp_otc_t_360_voz_mes_curso_' + val_abreviatura_temp)
    
    fun_cargar_datos(sqlContext, df_cursado_sms, val_esquema_temp, 'tmp_otc_t_360_sms_' + val_abreviatura_temp)
    
    print("INFO: Total registros tabla tmp_otc_t_360_parque_trafico_"+ val_abreviatura_temp +": "+str(df_parque_trafico.count()))
    print("INFO: Total registros tabla tmp_otc_t_360_megas_dia_"+ val_abreviatura_temp +": "+str(df_agrupa_megas_dia.count()))
    print("INFO: Total registros tabla tmp_otc_t_360_megas_mes_"+ val_abreviatura_temp +": "+str(df_agrupa_megas_mes.count()))
    print("INFO: Total registros tabla tmp_otc_t_360_megas_2_mes_"+ val_abreviatura_temp +": "+str(df_agrupa_megas_2_mes.count()))
    print("INFO: Total registros tabla tmp_otc_t_360_megas_mes_curso_"+ val_abreviatura_temp +": "+str(df_agrupa_megas_mes_curso.count()))
    print("INFO: Total registros tabla tmp_otc_t_360_trafico_tecno_dia_"+ val_abreviatura_temp +": "+str(agrupa_tecno_dia.count()))
    print("INFO: Total registros tabla tmp_otc_t_360_trafico_tecno_mes_"+ val_abreviatura_temp +": "+str(agrupa_tecno_mes.count()))
    print("INFO: Total registros tabla tmp_otc_t_360_trafico_tecno_2_mes_"+ val_abreviatura_temp +": "+str(agrupa_tecno_2_mes.count()))
    print("INFO: Total registros tabla tmp_otc_t_360_trafico_tecno_mes_curso_"+ val_abreviatura_temp +": "+str(agrupa_tecno_mes_curso.count()))
    print("INFO: Total registros tabla tmp_otc_t_360_voz_dia_"+ val_abreviatura_temp +": "+str(agrupa_voz_dia.count()))
    print("INFO: Total registros tabla tmp_otc_t_360_voz_mes_"+ val_abreviatura_temp +": "+str(agrupa_voz_mes.count()))
    print("INFO: Total registros tabla tmp_otc_t_360_voz_2_mes_"+ val_abreviatura_temp +": "+str(agrupa_voz_2_mes.count()))
    print("INFO: Total registros tabla tmp_otc_t_360_voz_mes_curso_"+ val_abreviatura_temp +": "+str(agrupa_voz_mes_curso.count()))
    print("INFO: Total registros tabla tmp_otc_t_360_sms_"+ val_abreviatura_temp +": "+str(df_cursado_sms.count()))

    return 0

def fun_extraer_preferencia_consumo(sqlContext, fecha_eje):

    # extraemos los datos de la tabla bloqueo de llamadas
    datos_minutos = fun_extraer_datos_preferecnia(sqlContext)
    
    datos_minutos=datos_minutos.na.fill(value=0)

    df = datos_minutos.withColumnRenamed("num_telefonico","telefono")\
    	.withColumnRenamed("linea_negocio_homologado","linea_negocio")\
    	.withColumnRenamed("segmento","segmento")\
    	.withColumnRenamed("mb60","mb")\
    	.withColumnRenamed("minutos60","minutos")
    
    #PREPAGO#
    
    prepago = df[(df.linea_negocio == 'PREPAGO')]
    prepago = prepago.drop('linea_negocio', 'segmento')
    
    #REMOVER OUTLIERS
    
    prepago = prepago.drop('linea_negocio', 'segmento')
    prepago = prepago.select("*").toPandas()
    
    df_out=prepago[['mb','minutos']]
    Q1 = df_out.quantile(0.25)
    Q3 = df_out.quantile(0.75)
    IQR = Q3 - Q1
    print(IQR)
    
    df_out = df_out[~((df_out < (Q1 - 1.5 * IQR)) |(df_out > (Q3 + 1.5 * IQR))).any(axis=1)]
    
    #PERCENTILES
    
    df_out['percent_mb'] = pd.cut(df_out['mb'], 20, labels=False)
    df_out['percent_min'] = pd.cut(df_out['minutos'], 20, labels=False)
    df_out.groupby('percent_min').min()
    
    def f(row):
        if row['percent_min'] > row['percent_mb']:
            val = 'MINUTOS'
        else:
            val = 'DATOS'
        return val
    
    df_out['datos_minutos'] = df_out.apply(f, axis=1)
    
    df_out.groupby('datos_minutos').count()
    #bb aqui me quede
    #MERGE LABELS 
    
    df_out['index1'] = df_out.index
    prepago['index1'] = prepago.index
    
    final_percentil= pd.merge(prepago, df_out, on='index1')
    final_prepago=final_percentil[['telefono','datos_minutos']]
    
    #POSTPAGO INDIVIDUAL#
    
    postpago = df[(df.segmento == 'INDIVIDUAL') & (df.linea_negocio == 'POSPAGO')]
    
    #REMOVER OUTLIERS
    
    postpago = postpago.drop('linea_negocio', 'segmento')
    postpago = postpago.select("*").toPandas()
    
    df_out=postpago[['mb','minutos']]
    Q1 = df_out.quantile(0.25)
    Q3 = df_out.quantile(0.75)
    IQR = Q3 - Q1
    print(IQR)
    
    df_out = df_out[~((df_out < (Q1 - 1.5 * IQR)) |(df_out > (Q3 + 1.5 * IQR))).any(axis=1)]
    
    #PERCENTILES
    
    df_out['percent_mb'] = pd.cut(df_out['mb'], 20, labels=False)
    df_out['percent_min'] = pd.cut(df_out['minutos'], 20, labels=False)
    df_out.groupby('percent_min').min()
    
    df_out['datos_minutos'] = df_out.apply(f, axis=1)
    
    df_out.groupby('datos_minutos').count()
    
    #MERGE LABELS 
    
    df_out['index1'] = df_out.index
    postpago['index1'] = postpago.index
    
    final_percentil= pd.merge(postpago, df_out, on='index1')
    final_postpago=final_percentil[['telefono','datos_minutos']]
    
    #POSTPAGO NEGOCIOS#
    
    postnegocios = df[(df.segmento == 'NEGOCIOS') & (df.linea_negocio == 'POSPAGO')]
    
    #REMOVER OUTLIERS
    
    postnegocios = postnegocios.drop('linea_negocio', 'segmento')
    postnegocios = postnegocios.select("*").toPandas()
    
    df_out=postnegocios[['mb','minutos']]
    Q1 = df_out.quantile(0.25)
    Q3 = df_out.quantile(0.75)
    IQR = Q3 - Q1
    print(IQR)
    
    df_out = df_out[~((df_out < (Q1 - 1.5 * IQR)) |(df_out > (Q3 + 1.5 * IQR))).any(axis=1)]
    
    #PERCENTILES
    
    df_out['percent_mb'] = pd.cut(df_out['mb'], 20, labels=False)
    df_out['percent_min'] = pd.cut(df_out['minutos'], 20, labels=False)
    df_out.groupby('percent_min').min()
    
    df_out['datos_minutos'] = df_out.apply(f, axis=1)
    
    df_out.groupby('datos_minutos').count()
    
    #MERGE LABELS 
    
    df_out['index1'] = df_out.index
    postnegocios['index1'] = postnegocios.index
    
    final_percentil= pd.merge(postnegocios, df_out, on='index1')
    final_postnegocios=final_percentil[['telefono','datos_minutos']]
    
    #POSTPAGO EMPRESAS#
    
    postempresas = df[(df.segmento == 'GGCC') & (df.linea_negocio == 'POSPAGO')]
    
    #REMOVER OUTLIERS
    
    postempresas = postempresas.drop('linea_negocio', 'segmento')
    postempresas = postempresas.select("*").toPandas()
    
    df_out=postempresas[['mb','minutos']]
    Q1 = df_out.quantile(0.25)
    Q3 = df_out.quantile(0.75)
    IQR = Q3 - Q1
    print(IQR)
    
    df_out = df_out[~((df_out < (Q1 - 1.5 * IQR)) |(df_out > (Q3 + 1.5 * IQR))).any(axis=1)]
    
    #PERCENTILES
    
    df_out['percent_mb'] = pd.cut(df_out['mb'], 20, labels=False)
    df_out['percent_min'] = pd.cut(df_out['minutos'], 20, labels=False)
    df_out.groupby('percent_min').min()
    
    df_out['datos_minutos'] = df_out.apply(f, axis=1)
    
    df_out.groupby('datos_minutos').count()
    
    #MERGE LABELS 
    
    df_out['index1'] = df_out.index
    postempresas['index1'] = postempresas.index
    
    final_percentil= pd.merge(postempresas, df_out, on='index1')
    final_postempresas=final_percentil[['telefono','datos_minutos']]
    
    #MERGE DE TODAS LAS BASES
    
    base_final=final_prepago.append(final_postpago, ignore_index = True)
    base_final=base_final.append(final_postnegocios, ignore_index = True)
    base_final=base_final.append(final_postempresas, ignore_index = True)
    base_completa = df.select("telefono").toPandas()
    
    base = base_completa.merge(base_final,  how='left')
    base=base.fillna(value='DATOS')
    
    base_spark = sqlContext.createDataFrame(base)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, 'tmp_otc_t_360_preferencia_' + val_abreviatura_temp)
    fun_cargar_datos(sqlContext, base_spark, val_esquema_temp, 'tmp_otc_t_360_preferencia_' + val_abreviatura_temp)
    print("INFO: Total registros tabla tmp_otc_t_360_preferencia_"+ val_abreviatura_temp +": "+str(base_spark.count()))
        
    return 0

#FUNCIONES NUEVAS REFACTORING

def fun_dev_tmp (df, fec_ini, fec_fin, campo_fecha, campo_valor, campo_cantidad):     
    fec_1 = "".join(str(fec_fin).split('-'))
    df_1 = df.filter((col(campo_fecha)>=fec_ini) & (col(campo_fecha)<= fec_fin))
    #print(str(fec_ini)+"-"+str(fec_fin))
    df_2 = df_1.groupBy('marca', 'telefono').agg(sum(campo_valor).alias('valor'), sum(campo_cantidad).alias('cantidad'))
    df_2 = df_2.withColumn('fecha_proceso', lit(str(fec_1)))
    return df_2

def fun_cargar_universo_tmp(sqlContext):
    print("Extrayendo tablas temporales universo...")
    df_sms = fun_extraer_dev_tmp(sqlContext,val_esquema_temp, val_abreviatura_temp, val_tabla_tmp_sms, list('marca,telefono'.split(",")))
    df_voz = fun_extraer_dev_tmp(sqlContext,val_esquema_temp, val_abreviatura_temp, val_tabla_tmp_voz, list('marca,telefono'.split(",")))
    df_dat = fun_extraer_dev_tmp(sqlContext,val_esquema_temp, val_abreviatura_temp, val_tabla_tmp_datos,list("marca,telefono".split(",")))
    df_cont = fun_extraer_dev_tmp(sqlContext,val_esquema_temp, val_abreviatura_temp, val_tabla_tmp_contenidos,list("marca,telefono".split(",")))
    df_adl = fun_extraer_dev_tmp(sqlContext,val_esquema_temp, val_abreviatura_temp, val_tabla_tmp_adelanto_saldo,list("marca,telefono".split(",")))
    df_bzond = fun_extraer_dev_tmp(sqlContext,val_esquema_temp, val_abreviatura_temp, val_tabla_tmp_buzon_voz_acum,list("marca,num_telefono".split(",")))
    df_llamd = fun_extraer_dev_tmp(sqlContext,val_esquema_temp, val_abreviatura_temp, val_tabla_tmp_llamada_espera_acum,list("marca,num_telefono".split(",")))
    df_cbna = fun_extraer_dev_tmp(sqlContext,val_esquema_temp, val_abreviatura_temp, val_tabla_tmp_combos_bonos_acum, list("marca,num_telefono".split(",")))
    df_cbnt = fun_extraer_dev_tmp(sqlContext,val_esquema_temp, val_abreviatura_temp, val_tabla_tmp_combos_bonos_total, list("marca,num_telefono".split(",")))
        
    df_sms = df_sms.withColumn('marca', upper(col('marca')))
    df_voz = df_voz.withColumn('marca', upper(col('marca')))
    df_dat = df_dat.withColumn('marca', upper(col('marca')))
    df_cont = df_cont.withColumn('marca', upper(col('marca')))
    df_adl = df_adl.withColumn('marca', upper(col('marca')))
    df_bzond = df_bzond.withColumnRenamed('num_telefono','telefono').withColumn('marca', upper(col('marca')))
    df_llamd = df_llamd.withColumnRenamed('num_telefono','telefono').withColumn('marca', upper(col('marca')))
    df_cbna = df_cbna.withColumnRenamed('num_telefono','telefono').withColumn('marca', upper(col('marca')))
    df_cbnt = df_cbnt.withColumnRenamed('num_telefono','telefono').withColumn('marca', upper(col('marca')))
    
    dfs = [df_sms,df_voz,df_dat,df_cont,df_adl,df_bzond,df_llamd,df_cbna,df_cbnt]
    df_universo = reduce(DataFrame.unionAll, dfs)
    del df_sms,df_voz,df_dat,df_cont,df_adl,df_bzond,df_llamd,df_cbna,df_cbnt
       
    df_universo = df_universo.withColumn('marca', upper(col('marca')))
      
    print("Extrayendo universo unicos...")
    df_univ_unic = df_universo.select('marca','telefono').distinct()
    del df_universo
    
    # Borrar tabla
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_UNIVERSO_DEV_UNICOS_"+ val_abreviatura_temp)
    
    fun_sobreescribir_datos(sqlContext, df_univ_unic, val_esquema_temp, "TMP_360_OTC_T_UNIVERSO_DEV_UNICOS_"+ val_abreviatura_temp)
    print("INFO: Total registros tabla TMP_360_OTC_T_UNIVERSO_DEV_UNICOS_"+ val_abreviatura_temp +": "+str(df_univ_unic.count()))
    del df_univ_unic
    return 0

def fun_cargar_cmcb_tmp(sqlContext,fecha_eje1):
    df_cbnt = fun_tmp_360_otc_t_dev_bono_combo_1(sqlContext,val_esquema_temp, val_abreviatura_temp)
   
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_BONO_COMBO_1_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_cbnt, val_esquema_temp, "TMP_360_OTC_T_DEV_BONO_COMBO_1_"+ val_abreviatura_temp)  
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_BONO_COMBO_1_"+ val_abreviatura_temp +": "+str(df_cbnt.count()))
    
    df_tmp_360_otc_t_dev_bono_combo_2 = fun_tmp_360_otc_t_dev_bono_combo_2(sqlContext,val_esquema_temp,val_abreviatura_temp,fecha_eje1)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_BONO_COMBO_2_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_tmp_360_otc_t_dev_bono_combo_2, val_esquema_temp, "TMP_360_OTC_T_DEV_BONO_COMBO_2_"+ val_abreviatura_temp)
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_BONO_COMBO_2_"+ val_abreviatura_temp +": "+str(df_tmp_360_otc_t_dev_bono_combo_2.count()))
    del df_cbnt,df_tmp_360_otc_t_dev_bono_combo_2
    
    return 0

def fun_cargar_bono_tmp(sqlContext, fecha_eje1):
    print("Extrayendo bonos periodo...")
    df_bona = fun_tmp_360_otc_t_dev_bono_1(sqlContext,val_esquema_temp, val_abreviatura_temp)   
      
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_BONO_1_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_bona, val_esquema_temp, "TMP_360_OTC_T_DEV_BONO_1_"+ val_abreviatura_temp)  
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_BONO_1_"+ val_abreviatura_temp +": "+str(df_bona.count()))
    
    df_tmp_360_otc_t_dev_bono_2 = fun_tmp_360_otc_t_dev_bono_2(sqlContext,val_esquema_temp,val_abreviatura_temp,fecha_eje1)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_BONO_2_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_tmp_360_otc_t_dev_bono_2, val_esquema_temp, "TMP_360_OTC_T_DEV_BONO_2_"+ val_abreviatura_temp)
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_BONO_2_"+ val_abreviatura_temp +": "+str(df_tmp_360_otc_t_dev_bono_2.count()))
    
    del df_tmp_360_otc_t_dev_bono_2,df_bona
    return 0

def fun_cargar_combo_tmp(sqlContext, fecha_eje1):
    print("Extrayendo combos periodo...")    
    df_comba = fun_tmp_360_otc_t_dev_combo_1(sqlContext,val_esquema_temp, val_abreviatura_temp)   
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_COMBO_1_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_comba, val_esquema_temp, "TMP_360_OTC_T_DEV_COMBO_1_"+ val_abreviatura_temp)  
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_COMBO_1_"+ val_abreviatura_temp +": "+str(df_comba.count()))
    
    df_tmp_360_otc_t_dev_combo_2 = fun_tmp_360_otc_t_dev_combo_2(sqlContext,val_esquema_temp,val_abreviatura_temp,fecha_eje1)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_COMBO_2_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_tmp_360_otc_t_dev_combo_2, val_esquema_temp, "TMP_360_OTC_T_DEV_COMBO_2_"+ val_abreviatura_temp)
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_COMBO_2_"+ val_abreviatura_temp +": "+str(df_tmp_360_otc_t_dev_combo_2.count()))
   
    del df_comba,df_tmp_360_otc_t_dev_combo_2
    return 0

def fun_cargar_sms_tmp(sqlContext, fecha_eje2):
    df_sms = fun_tmp_360_otc_t_dev_sms_1(sqlContext,val_esquema_temp, val_abreviatura_temp)
       
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_SMS_1_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_sms, val_esquema_temp, "TMP_360_OTC_T_DEV_SMS_1_"+ val_abreviatura_temp)  
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_SMS_1_"+ val_abreviatura_temp +": "+str(df_sms.count()))
    
    df_tmp_360_otc_t_dev_sms_2 = fun_tmp_360_otc_t_dev_sms_2(sqlContext,val_esquema_temp,val_abreviatura_temp,fecha_eje2)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_SMS_2_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_tmp_360_otc_t_dev_sms_2, val_esquema_temp, "TMP_360_OTC_T_DEV_SMS_2_"+ val_abreviatura_temp)
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_SMS_2_"+ val_abreviatura_temp +": "+str(df_tmp_360_otc_t_dev_sms_2.count()))
   
    del df_sms,df_tmp_360_otc_t_dev_sms_2
    
    return 0


def fun_cargar_voz_tmp(sqlContext,fecha_eje2):
    df_voz = fun_tmp_360_otc_t_dev_voz_1(sqlContext,val_esquema_temp, val_abreviatura_temp)
        
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_VOZ_1_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_voz, val_esquema_temp, "TMP_360_OTC_T_DEV_VOZ_1_"+ val_abreviatura_temp)  
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_VOZ_1_"+ val_abreviatura_temp +": "+str(df_voz.count()))
    
    df_tmp_360_otc_t_dev_voz_2 = fun_tmp_360_otc_t_dev_voz_2(sqlContext,val_esquema_temp,val_abreviatura_temp,fecha_eje2)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_VOZ_2_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_tmp_360_otc_t_dev_voz_2, val_esquema_temp, "TMP_360_OTC_T_DEV_VOZ_2_"+ val_abreviatura_temp)
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_VOZ_2_"+ val_abreviatura_temp +": "+str(df_tmp_360_otc_t_dev_voz_2.count()))
    
    del df_voz, df_tmp_360_otc_t_dev_voz_2
    return 0

def fun_cargar_dat_tmp(sqlContext, fecha_eje2):
    df_dat = fun_tmp_360_otc_t_dev_datos_1(sqlContext,val_esquema_temp, val_abreviatura_temp)    
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_DATOS_1_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_dat, val_esquema_temp, "TMP_360_OTC_T_DEV_DATOS_1_"+ val_abreviatura_temp)  
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_DATOS_1_"+ val_abreviatura_temp +": "+str(df_dat.count()))
    
    df_tmp_360_otc_t_dev_datos_2 = fun_tmp_360_otc_t_dev_datos_2(sqlContext,val_esquema_temp,val_abreviatura_temp,fecha_eje2)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_DATOS_2_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_tmp_360_otc_t_dev_datos_2, val_esquema_temp, "TMP_360_OTC_T_DEV_DATOS_2_"+ val_abreviatura_temp)
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_DATOS_2_"+ val_abreviatura_temp +": "+str(df_tmp_360_otc_t_dev_datos_2.count()))

    del df_dat, df_tmp_360_otc_t_dev_datos_2
    return 0

def fun_cargar_cont_tmp(sqlContext, fecha_eje2):
    df_cont = fun_tmp_360_otc_t_dev_contenidos_1(sqlContext,val_esquema_temp, val_abreviatura_temp)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_CONTENIDOS_1_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_cont, val_esquema_temp, "TMP_360_OTC_T_DEV_CONTENIDOS_1_"+ val_abreviatura_temp)  
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_CONTENIDOS_1_"+ val_abreviatura_temp +": "+str(df_cont.count()))
    
    df_tmp_360_otc_t_dev_contenidos_2 = fun_tmp_360_otc_t_dev_contenidos_2(sqlContext,val_esquema_temp,val_abreviatura_temp,fecha_eje2)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_CONTENIDOS_2_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_tmp_360_otc_t_dev_contenidos_2, val_esquema_temp, "TMP_360_OTC_T_DEV_CONTENIDOS_2_"+ val_abreviatura_temp)
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_CONTENIDOS_2_"+ val_abreviatura_temp +": "+str(df_tmp_360_otc_t_dev_contenidos_2.count()))
    
    del df_cont, df_tmp_360_otc_t_dev_contenidos_2    
    return 0

def fun_cargar_adl_tmp(sqlContext, fecha_eje2):
    df_adl = fun_tmp_360_otc_t_dev_adelanto_saldo_1(sqlContext,val_esquema_temp, val_abreviatura_temp)
        
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_ADELANTO_SALDO_1_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_adl, val_esquema_temp, "TMP_360_OTC_T_DEV_ADELANTO_SALDO_1_"+ val_abreviatura_temp)  
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_ADELANTO_SALDO_1_"+ val_abreviatura_temp +": "+str(df_adl.count()))
    
    df_tmp_360_otc_t_dev_adelanto_saldo_2 = fun_tmp_360_otc_t_dev_adelanto_saldo_2(sqlContext,val_esquema_temp,val_abreviatura_temp,fecha_eje2)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_ADELANTO_SALDO_2_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_tmp_360_otc_t_dev_adelanto_saldo_2, val_esquema_temp, "TMP_360_OTC_T_DEV_ADELANTO_SALDO_2_"+ val_abreviatura_temp)
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_ADELANTO_SALDO_2_"+ val_abreviatura_temp +": "+str(df_tmp_360_otc_t_dev_adelanto_saldo_2.count()))
    
    del df_adl, df_tmp_360_otc_t_dev_adelanto_saldo_2
    return 0

def fun_cargar_bzn_tmp(sqlContext, fecha_eje2):
    df_bzond = fun_tmp_360_otc_t_dev_buzon_voz_acumulado_1(sqlContext,val_esquema_temp, val_abreviatura_temp)
        
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_BUZON_VOZ_ACUMULADO_1_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_bzond, val_esquema_temp, "TMP_360_OTC_T_DEV_BUZON_VOZ_ACUMULADO_1_"+ val_abreviatura_temp)  
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_BUZON_VOZ_ACUMULADO_1_"+ val_abreviatura_temp +": "+str(df_bzond.count()))
    
    df_tmp_360_otc_t_dev_buzon_voz_diario_1 = fun_tmp_360_otc_t_dev_buzon_voz_diario_1(sqlContext,val_esquema_temp,val_abreviatura_temp,fecha_eje2)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_BUZON_VOZ_DIARIO_1_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_tmp_360_otc_t_dev_buzon_voz_diario_1, val_esquema_temp, "TMP_360_OTC_T_DEV_BUZON_VOZ_DIARIO_1_"+ val_abreviatura_temp)
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_BUZON_VOZ_DIARIO_1_"+ val_abreviatura_temp +": "+str(df_tmp_360_otc_t_dev_buzon_voz_diario_1.count()))
        
    del df_bzond, df_tmp_360_otc_t_dev_buzon_voz_diario_1
    return 0

def fun_cargar_llmd_tmp(sqlContext, fecha_eje2):
    df_llamd = fun_tmp_360_otc_t_dev_llamada_espera_acumulado_1(sqlContext,val_esquema_temp, val_abreviatura_temp)
        
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_360_OTC_T_DEV_LLAMADA_ESPERA_ACUMULADO_1_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_llamd, val_esquema_temp, "TMP_360_OTC_T_DEV_LLAMADA_ESPERA_ACUMULADO_1_"+ val_abreviatura_temp)  
    print("INFO: Total registros tabla TMP_360_OTC_T_DEV_LLAMADA_ESPERA_ACUMULADO_1_"+ val_abreviatura_temp +": "+str(df_llamd.count()))
    
    df_tmp_360_otc_t_dev_llamada_espera_diario_1 = fun_tmp_360_otc_t_dev_llamada_espera_diario_1(sqlContext,val_esquema_temp,val_abreviatura_temp,fecha_eje2)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, "tmp_360_otc_t_dev_llamada_espera_diario_1_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_tmp_360_otc_t_dev_llamada_espera_diario_1, val_esquema_temp, "tmp_360_otc_t_dev_llamada_espera_diario_1_"+ val_abreviatura_temp)
    print("INFO: Total registros tabla tmp_360_otc_t_dev_llamada_espera_diario_1_"+ val_abreviatura_temp +": "+str(df_tmp_360_otc_t_dev_llamada_espera_diario_1.count()))
    
    del df_llamd, df_tmp_360_otc_t_dev_llamada_espera_diario_1   
    return 0

def fun_cargar_tmp_univ(sqlContext):
    print("Extrayendo datos diario y periodo parte 1...")
    df_temporal360dev = fun_tmp_otc_t_360_devengos(sqlContext,val_esquema_temp,val_abreviatura_temp)
    
    fun_borrar_tabla(sqlContext, val_esquema_temp, "TMP_OTC_T_360_DEVENGOS_"+ val_abreviatura_temp)
    fun_sobreescribir_datos(sqlContext, df_temporal360dev, val_esquema_temp, "TMP_OTC_T_360_DEVENGOS_"+ val_abreviatura_temp)  
    print("INFO: Total registros tabla TMP_OTC_T_360_DEVENGOS_"+ val_abreviatura_temp +": "+str(df_temporal360dev.count()))
    return 0

def fun_cargar_devengos(sqlContext, esquema_dev, tabla_dev,fecha_eje):
    print("Insertando en tabla final")
    df_final = fun_otc_t_360_devengos(sqlContext,val_esquema_temp, "TMP_OTC_T_360_DEVENGOS_"+ val_abreviatura_temp)
    
    df_final = df_final.fillna(0)
    
    df_final = df_final.withColumn('fecha_proceso', lit(fecha_eje).cast('int'))
    columns = sqlContext.table(esquema_dev+"."+tabla_dev).columns
    cols = []
    for column in columns:
       cols.append(column)
                
    df_final = df_final.select(cols)
    fun_sobreescribir_datos(sqlContext, df_final, esquema_dev, tabla_dev)
    print("INFO: Total registros tabla "+esquema_dev+"."+tabla_dev +": "+str(df_final.orderBy('fecha_proceso').groupBy('fecha_proceso').count().show()))
    return 0

