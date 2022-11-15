# -*- coding: utf-8 -*-
from Configuraciones.configuracion_movimientos_parque import *


from Querys.sql import *
from Funciones.funcion import *
import pyspark.sql.functions as sql_fun
from pyspark.sql.functions import col, substring, max, min, when, count, sum, lit, unix_timestamp, desc, asc, length, expr, datediff, row_number, coalesce, split, trim, concat, concat_ws, to_date, last_day
from pyspark.sql.types import StringType, DateType, IntegerType, StructType, TimestampType
from pyspark.sql.window import Window
import pandas as pd



@seguimiento_transformacion
# Funcion Principal del Proceso Transfer
def func_proceso_cambioplan_principal(sqlContext, val_fecha_ejecucion, val_fecha_hoy_int, val_fecha_proc, val_fecha_dia_bigint, val_fecha_proc_bigint, val_fecha_proc_mes_anio_bigint, val_ini_mes_despues_01, val_ini_mes_actual_ini, val_ini_mes_actual_bigint_end, val_ini_mes_actual_bigint_ini, val_fecha_dia_antes_actual, val_fecha_mes_ini2, val_fecha_mes_fin2, val_ini_mes_anterior_date, val_fin_mes_anterior_date):
    
    # controlpoint1    
    val_str_controlpoint1, df_plan_1, df_plan, df_parque_actual_cp_1, df_parque_actual_cp_2, df_parque_actual_cp, df_parque_actual_cp_3 \
    , df_parque_actual_1, df_parque_actual_2, df_parque_actual = fun_controlpoint1(sqlContext, val_ini_mes_actual_ini, val_fecha_dia_antes_actual, val_ini_mes_actual_bigint_end, val_fecha_proc_bigint)

    # controlpoint2
    val_str_controlpoint2, df_cambios_numero_1, df_cambios_numero_2, df_parque_actual_final, df_parque_anterior_1 \
    , df_parque_anterior_2, df_parque_anterior, df_ctl_pos_usr_nc_cambio_plan, df_ctl_pos_usr_nc_cambio_plan_union, df_usuario \
    , df_tmp_cambio_plan_1_1_2, df_tmp_cambio_plan_1_1 = fun_controlpoint2(sqlContext, df_parque_actual, df_parque_actual_cp_3, df_plan, val_fecha_mes_ini2, val_fecha_mes_fin2, val_ini_mes_actual_bigint_ini, val_ini_mes_actual_ini, val_ini_mes_despues_01, val_fecha_proc)
    
    
    # controlpoint3
    val_str_controlpoint3, df_cp_validar_pa, df_cp_validar_nc, df_cp_cambio_plan, df_tmp_cambio_plan_fnl, df_override_actual \
    , df_otc_t_override_planes_ant, df_override_anterior = fun_controlpoint3(sqlContext, df_tmp_cambio_plan_1_1, df_parque_anterior, val_fecha_dia_bigint, val_fecha_proc_mes_anio_bigint, val_fecha_proc, val_ini_mes_actual_ini, val_fecha_dia_antes_actual, val_ini_mes_anterior_date, val_fin_mes_anterior_date)
    
    
    # controlpoint4
    val_str_controlpoint4, df_otc_t_descuentos_planes_act, df_descuento_actual, df_tmp_ov_desc_plan_new \
    , df_descuento_anterior, df_cambio_plan_fnl_int, df_otc_t_cambio_plan_bi = fun_controlpoint4(sqlContext, df_tmp_cambio_plan_fnl, df_parque_anterior, df_override_actual, df_override_anterior, val_fecha_dia_bigint, val_ini_mes_actual_ini, val_fecha_proc, val_ini_mes_anterior_date, val_fin_mes_anterior_date)

    # Mensajes
    
    val_str_resultado = val_str_controlpoint1
    
    val_str_resultado = val_str_resultado + "\n" + val_str_controlpoint2
    
    val_str_resultado = val_str_resultado + "\n" + val_str_controlpoint3

    val_str_resultado = val_str_resultado + "\n" + val_str_controlpoint4
        
    return val_str_resultado


# FUNCIONES PRINCIPALES DE CONTROL

@seguimiento_transformacion
def fun_controlpoint1(sqlContext, val_ini_mes_actual_ini, val_fecha_dia_antes_actual, val_ini_mes_actual_bigint_end, val_fecha_proc_bigint):
    
    print(msg_succ('================================================================'))
    print(msg_succ('======== FUNCION DE CONTROL => fun_controlpoint1 ==============='))
    print(msg_succ('================================================================'))
    
    df_plan_1, val_str_df_plan_1 = fun_cargar_df_plan_1(sqlContext)    
    df_plan, val_str_df_plan = fun_cargar_df_plan(sqlContext, df_plan_1)
    df_parque_actual_cp_1, val_str_df_parque_actual_cp_1 = fun_cargar_df_parque_actual_cp_1(sqlContext, val_ini_mes_actual_bigint_end, val_fecha_proc_bigint, val_ini_mes_actual_ini, val_fecha_dia_antes_actual)
    df_parque_actual_cp_2, val_str_df_parque_actual_cp_2 = fun_cargar_df_parque_actual_cp_2(sqlContext,df_parque_actual_cp_1)
    df_parque_actual_cp, val_str_df_parque_actual_cp = fun_cargar_df_parque_actual_cp(sqlContext,df_parque_actual_cp_1,df_parque_actual_cp_2)
    df_parque_actual_cp_3, val_str_df_parque_actual_cp_3 = fun_cargar_df_parque_actual_cp_3(sqlContext, df_parque_actual_cp)
    df_parque_actual_1, val_str_df_parque_actual_1 = fun_cargar_df_parque_actual_1(sqlContext, val_fecha_proc_bigint, val_ini_mes_actual_ini)
    df_parque_actual_2, val_str_df_parque_actual_2 = fun_cargar_df_parque_actual_2(sqlContext, df_parque_actual_1)
    df_parque_actual, val_str_df_parque_actual = fun_cargar_df_parque_actual(sqlContext, df_parque_actual_2)
    
    val_str_resultado_cp1 = val_str_df_plan_1 + "\n" + val_str_df_plan + "\n" + val_str_df_parque_actual_cp_1 + "\n" + val_str_df_parque_actual_cp_2
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_parque_actual_cp + "\n" + val_str_df_parque_actual_cp_3 + "\n" + val_str_df_parque_actual_1
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_parque_actual_2 + "\n" + val_str_df_parque_actual
    
    return val_str_resultado_cp1, df_plan_1, df_plan, df_parque_actual_cp_1, df_parque_actual_cp_2, df_parque_actual_cp, df_parque_actual_cp_3, df_parque_actual_1, df_parque_actual_2, df_parque_actual
    
    
@seguimiento_transformacion
def fun_controlpoint2(sqlContext, df_parque_actual, df_parque_actual_cp_3, df_plan, val_fecha_mes_ini2, val_fecha_mes_fin2, val_ini_mes_actual_bigint_ini, val_ini_mes_actual_ini, val_ini_mes_despues_01, val_fecha_proc):
    print(msg_succ('================================================================'))
    print(msg_succ('======== FUNCION DE CONTROL => fun_controlpoint2 ==============='))
    print(msg_succ('================================================================'))
    
    df_cambios_numero_1, val_str_df_cambios_numero_1 = fun_cargar_df_cambios_numero_1(sqlContext, val_fecha_mes_ini2, val_fecha_mes_fin2)
    df_cambios_numero_2, val_str_df_cambios_numero_2 = fun_cargar_df_cambios_numero_2(sqlContext, df_cambios_numero_1)
    df_parque_actual_final, val_str_df_parque_actual_final = fun_cargar_df_parque_actual_final(sqlContext, df_parque_actual, df_cambios_numero_2)
    df_parque_anterior_1, val_str_df_parque_anterior_1 = fun_cargar_df_parque_anterior_1(sqlContext, df_parque_actual_final, val_ini_mes_actual_bigint_ini, val_ini_mes_actual_ini)
    df_parque_anterior_2, val_str_df_parque_anterior_2 = fun_cargar_df_parque_anterior_2(sqlContext, df_parque_anterior_1)
    df_parque_anterior, val_str_df_parque_anterior = fun_cargar_df_parque_anterior(sqlContext, df_parque_anterior_2)
    df_ctl_pos_usr_nc_cambio_plan, val_str_df_ctl_pos_usr_nc_cambio_plan = fun_cargar_df_ctl_pos_usr_nc_cambio_plan(sqlContext, val_ini_mes_despues_01)
    df_ctl_pos_usr_nc_cambio_plan_union, val_str_df_ctl_pos_usr_nc_cambio_plan_union = fun_cargar_df_ctl_pos_usr_nc_cambio_plan_union(sqlContext, df_ctl_pos_usr_nc_cambio_plan)
    df_usuario, val_str_df_usuario = fun_cargar_df_usuario(sqlContext, df_ctl_pos_usr_nc_cambio_plan_union, val_fecha_proc)
    df_tmp_cambio_plan_1_1_2, val_str_df_tmp_cambio_plan_1_1_2 = fun_cargar_df_tmp_cambio_plan_1_1_2(sqlContext, df_parque_actual_final, df_parque_anterior)
    df_tmp_cambio_plan_1_1, val_str_df_tmp_cambio_plan_1_1 = fun_cargar_df_tmp_cambio_plan_1_1(sqlContext, df_tmp_cambio_plan_1_1_2, df_parque_actual_cp_3, df_plan, df_usuario)
    
    val_str_resultado_cp2 = val_str_df_cambios_numero_1 + "\n" + val_str_df_cambios_numero_2 + "\n" + val_str_df_parque_actual_final
    val_str_resultado_cp2 = val_str_resultado_cp2 + "\n" + val_str_df_parque_anterior_1 + "\n" + val_str_df_parque_anterior_2
    val_str_resultado_cp2 = val_str_resultado_cp2 + "\n" + val_str_df_parque_anterior + "\n" + val_str_df_ctl_pos_usr_nc_cambio_plan
    val_str_resultado_cp2 = val_str_resultado_cp2 + "\n" + val_str_df_ctl_pos_usr_nc_cambio_plan_union + "\n" + val_str_df_usuario
    val_str_resultado_cp2 = val_str_resultado_cp2 + "\n" + val_str_df_tmp_cambio_plan_1_1_2 + "\n" + val_str_df_tmp_cambio_plan_1_1
    val_str_resultado_cp2 = val_str_resultado_cp2 + "\n"
    
    return val_str_resultado_cp2, df_cambios_numero_1, df_cambios_numero_2, df_parque_actual_final, df_parque_anterior_1, df_parque_anterior_2 \
            , df_parque_anterior, df_ctl_pos_usr_nc_cambio_plan, df_ctl_pos_usr_nc_cambio_plan_union, df_usuario, df_tmp_cambio_plan_1_1_2, df_tmp_cambio_plan_1_1


@seguimiento_transformacion
def fun_controlpoint3(sqlContext, df_tmp_cambio_plan_1_1, df_parque_anterior, val_fecha_dia_bigint, val_fecha_proc_mes_anio_bigint, val_fecha_proc, val_ini_mes_actual_ini, val_fecha_dia_antes_actual, val_ini_mes_anterior_date, val_fin_mes_anterior_date):
    
    print(msg_succ('================================================================'))
    print(msg_succ('======== FUNCION DE CONTROL => fun_controlpoint3 ==============='))
    print(msg_succ('================================================================'))
    
    df_cp_validar_pa, val_str_cp_validar_pa = fun_cargar_df_cp_validar_pa(sqlContext, df_tmp_cambio_plan_1_1)
    df_cp_validar_nc, val_str_cp_validar_nc = fun_cargar_df_cp_validar_nc(sqlContext, df_tmp_cambio_plan_1_1)
    df_cambio_plan, val_str_cambio_plan = fun_cargar_df_cambio_plan(sqlContext, df_tmp_cambio_plan_1_1)
    df_tmp_cambio_plan_fnl, val_str_tmp_cambio_plan_fnl = fun_cargar_df_tmp_cambio_plan_fnl(sqlContext, df_cambio_plan, val_fecha_proc_mes_anio_bigint, val_fecha_proc, val_ini_mes_actual_ini, val_fecha_dia_antes_actual)
    df_override_actual, val_str_override_actual = fun_cargar_df_override_actual(sqlContext, val_fecha_dia_bigint, val_ini_mes_actual_ini, val_fecha_proc)
    df_otc_t_override_planes_ant, val_str_override_planes_ant = fun_cargar_df_override_planes_ant(sqlContext, val_fecha_dia_bigint, val_ini_mes_anterior_date, val_fin_mes_anterior_date)
    df_override_anterior, val_str_override_anterior = fun_cargar_df_override_anterior(sqlContext, df_tmp_cambio_plan_fnl, df_parque_anterior, df_otc_t_override_planes_ant)
    
    val_str_resultado_cp3 = val_str_cp_validar_pa + "\n" + val_str_cp_validar_nc + "\n" + val_str_cambio_plan + "\n" + val_str_tmp_cambio_plan_fnl
    val_str_resultado_cp3 = val_str_resultado_cp3 + "\n" + val_str_override_actual + "\n" + val_str_override_planes_ant + "\n" + val_str_override_anterior
    val_str_resultado_cp3 = val_str_resultado_cp3 + "\n"
    
    return val_str_resultado_cp3, df_cp_validar_pa, df_cp_validar_nc, df_cambio_plan, df_tmp_cambio_plan_fnl, df_override_actual \
            , df_otc_t_override_planes_ant, df_override_anterior


@seguimiento_transformacion
def fun_controlpoint4(sqlContext, df_tmp_cambio_plan_fnl, df_parque_anterior, df_override_actual, df_override_anterior, val_fecha_dia_bigint, val_ini_mes_actual_ini, val_fecha_proc, val_ini_mes_anterior_date, val_fin_mes_anterior_date):
    
    print(msg_succ('================================================================'))
    print(msg_succ('======== FUNCION DE CONTROL => fun_controlpoint4 ==============='))
    print(msg_succ('================================================================'))
    
    df_otc_t_descuentos_planes_act, val_str_descuentos_planes_act = fun_cargar_df_otc_t_descuentos_planes_act(sqlContext, val_fecha_dia_bigint, val_ini_mes_actual_ini, val_fecha_proc)
    df_descuento_actual, val_str_descuento_actual = fun_cargar_df_descuento_actual(sqlContext, df_tmp_cambio_plan_fnl, df_otc_t_descuentos_planes_act)
    df_tmp_ov_desc_plan_new, val_str_tmp_ov_desc_plan_new = fun_cargar_df_tmp_ov_desc_plan_new(sqlContext, df_tmp_cambio_plan_fnl, val_ini_mes_anterior_date, val_fin_mes_anterior_date)
    df_descuento_anterior, val_str_descuento_anterior = fun_cargar_df_descuento_anterior(sqlContext, df_tmp_cambio_plan_fnl, df_parque_anterior, df_tmp_ov_desc_plan_new)
    df_cambio_plan_fnl_int, val_str_cambio_plan_fnl_int = fun_cargar_df_cambio_plan_fnl_int(sqlContext, df_tmp_cambio_plan_fnl, df_parque_anterior, df_override_actual, df_override_anterior, df_descuento_actual)
    df_otc_t_cambio_plan_bi, val_str_df_otc_t_cambio_plan_bi = fun_cargar_df_otc_t_cambio_plan_bi(sqlContext, df_cambio_plan_fnl_int, val_fecha_dia_bigint)
    
    val_str_resultado_cp4 = val_str_descuentos_planes_act + "\n" + val_str_descuento_actual + "\n" + val_str_tmp_ov_desc_plan_new + "\n"
    val_str_resultado_cp4 = val_str_resultado_cp4 + "\n" + val_str_descuento_anterior + "\n" + val_str_cambio_plan_fnl_int + "\n" + val_str_df_otc_t_cambio_plan_bi
    
    return val_str_resultado_cp4, df_otc_t_descuentos_planes_act, df_descuento_actual, df_tmp_ov_desc_plan_new, df_descuento_anterior, df_cambio_plan_fnl_int, df_otc_t_cambio_plan_bi


# FUNCIONES QUE GENERAN DATAFRAME

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.PLAN_1
def fun_cargar_df_plan_1(sqlContext):
    df_plan_1_tmp = fun_otc_t_ctl_planes_categoria_tarifa_1(sqlContext, val_cp_base_pro_transfer_consultas, val_cp_otc_t_ctl_planes_categoria_tarifa)
    df_plan_transfer_nc_1 = df_plan_1_tmp.select('PLANCD', 'PLANDESC', 'CATEGORIA', 'TARIFA_BASICA', 'COMERCIAL', 'COD_CATEGORIA').distinct()
    
    df_plan_1 = df_plan_1_tmp.cache()
    if val_cp_genera_temp_plan_1:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_plan_1), df_plan_1)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_plan_1))    
    
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_plan_1)
    return df_plan_1, "Transformacion => fun_cargar_df_plan_1 => df_plan_1" + str_datos_df

    
@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.PLAN
def fun_cargar_df_plan(sqlContext, df_plan_1):
    
    windowSpec  = Window.partitionBy('PLANCD').orderBy(asc('PLANCD'))
    df_plan_tmp = df_plan_1.withColumn('rn',row_number().over(windowSpec))
    df_plan_tmp = df_plan_tmp.filter(col('rn') == 1)
    
    df_plan =  df_plan_tmp.cache()
    # print(msg_succ('======== fun_cargar_df_plan (1) => PrintSchema ==========='))
    # print(msg_succ('%s\n => %s\n') % ( df_plan.printSchema(), df_plan.show(3)))
    
    # Eliminamos Tabla
    fun_eliminar_tabla(sqlContext, val_cp_base_desa_transfer, (val_cp_prefijo_tabla + val_cp_plan))
    # fun_eliminar_tabla(sqlContext, val_cp_base_pro_transfer_consultas, (val_cp_prefijo_tabla + val_cp_plan))
    
    # Insercion de Datos    
    # val_retorno_insercion = fun_realizar_insercion_df_tabla_final(sqlContext, val_cp_base_desa_transfer, (val_cp_prefijo_tabla + val_cp_plan), df_plan)
    val_retorno_insercion = fun_realizar_insercion_df_tabla_final(sqlContext, val_cp_base_pro_transfer_consultas, (val_cp_prefijo_tabla + val_cp_plan), df_plan)
    
    print(msg_succ('======== df_plan  =>  fun_cargar_datos_dinamico_tabla_sin_particion ==========='))
    print(msg_succ('%s => \n') % (val_retorno_insercion))
    
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_plan)
    return df_plan_1, "Transformacion => fun_cargar_df_plan => df_plan" + str_datos_df

                                            
@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.PARQUE_ACTUAL_CP_1
def fun_cargar_df_parque_actual_cp_1(sqlContext, val_ini_mes_actual_bigint_end, val_fecha_proc_bigint, val_ini_mes_actual_ini, val_fecha_dia_antes_actual):
    df_parque_actual_cp_1_tmp = fun_otc_t_nc_movi_parque_cambios_v1(sqlContext, val_cp_base_pro_transfer_consultas, val_cp_otc_t_nc_movi_parque_cambios_v1)
    df_parque_actual_cp_1_tmp = df_parque_actual_cp_1_tmp.selectExpr( "SUBSTR(TRIM(NUM_TELEFONICO),LENGTH(TRIM(NUM_TELEFONICO))-8,9) AS TELEFONO", "NUMERO_ABONADO"
                                                                        , "LOCALIZACION", "DOMAIN_LOGIN_OWN AS DOMAIN_LOGIN_OW", "NOMBRE_USUARIO_OWN AS NOMBRE_USUARIO_OW"
                                                                        , "DOMAIN_LOGIN_SUB", "NOMBRE_USUARIO_SUB", "PLAN_CODIGO", "PLAN_NOMBRE", "SEGMENTO_NC AS SEGMENTO", "SUB_SEGMENTO", "ESTADO_ABONADO"
                                                                        , "DOCUMENTO_CLIENTE", "CANAL", "OFICINA", "PROVINCIA", "cast(FECHA_ALTA as date) AS FECHA_ALTA", "cast(FECHA_CREACION as date) AS FECHA_CREACION"
                                                                        , "cast(FECHA_MODIF as date) AS FECHA_MODIFICACION", "cast(FECHA_SUBMITTED as date) AS FECHA_SUBMITTED", "ESTADO_ORDEN"
                                                                        , "TRIM(CASE WHEN DOMAIN_LOGIN_OWN = 'internal' AND DOMAIN_LOGIN_SUB <> 'internal' THEN DOMAIN_LOGIN_SUB WHEN DOMAIN_LOGIN_OWN <> 'internal' AND DOMAIN_LOGIN_SUB = 'internal' THEN DOMAIN_LOGIN_OWN WHEN DOMAIN_LOGIN_OWN <> 'internal' AND DOMAIN_LOGIN_SUB <> 'internal' THEN DOMAIN_LOGIN_OWN ELSE DOMAIN_LOGIN_OWN END) AS DOMAIN_CAMPO"
                                                                        , "case when marca is null then 'TELEFONICA' ELSE marca END MARCA"
                                                            )
                                                            
    df_parque_actual_cp_1_tmp = df_parque_actual_cp_1_tmp.filter((col('fecha_proceso') == str(val_ini_mes_actual_bigint_end)) | (col('fecha_proceso') == str(val_fecha_proc_bigint)))
    
    df_parque_actual_cp_1_tmp = df_parque_actual_cp_1_tmp.filter((col('FECHA_BAJA').cast(DateType()).isNull()) | ( (col('FECHA_BAJA').cast(DateType()) < val_ini_mes_actual_ini) & (col('FECHA_BAJA').cast(DateType()) > val_fecha_dia_antes_actual) ) )
    
    df_parque_actual_cp_1_tmp = df_parque_actual_cp_1_tmp.filter(col('FECHA_ALTA').cast(DateType()) < val_ini_mes_actual_ini)
    
    df_parque_actual_cp_1_tmp = df_parque_actual_cp_1_tmp.filter( (col('FECHA_MODIF').cast(DateType()).between(str(val_ini_mes_actual_ini), str(val_fecha_dia_antes_actual))) | (col('FECHA_SUBMITTED').cast(DateType()).between(str(val_ini_mes_actual_ini), str(val_fecha_dia_antes_actual))) )
    
    df_parque_actual_cp_1_tmp = df_parque_actual_cp_1_tmp.filter(sql_fun.upper(coalesce(col('LINEA_NEGOCIO'), lit(''))) != 'PREPAGO')
    
    df_parque_actual_cp_1_tmp = df_parque_actual_cp_1_tmp.filter(~(sql_fun.upper(coalesce(col('ESTADO_ORDEN'), lit(''))).isin(['CANCELLED','ENTERING','ERROR','ACCEPTED BY CUSTOMER'])))
    
    df_parque_actual_cp_1 =  df_parque_actual_cp_1_tmp.cache()
    
    if val_cp_genera_parque_actual_cp_1:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual_cp_1), df_parque_actual_cp_1)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual_cp_1))  
    
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_parque_actual_cp_1)
    return df_parque_actual_cp_1, "Transformacion => fun_cargar_df_parque_actual_cp_1 => df_parque_actual_cp_1" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.PARQUE_ACTUAL_CP_2
def fun_cargar_df_parque_actual_cp_2(sqlContext,df_parque_actual_cp_1):
    df_parque_actual_cp_2_tmp = df_parque_actual_cp_1
    
    df_parque_actual_cp_2_tmp = df_parque_actual_cp_2_tmp.groupBy('TELEFONO').agg(max(col('FECHA_SUBMITTED')).alias('FECHA_SUBMITTED'))
    df_parque_actual_cp_2 = df_parque_actual_cp_2_tmp.cache()
    
    if val_cp_genera_parque_actual_cp_2:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual_cp_2), df_parque_actual_cp_2)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual_cp_2))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_parque_actual_cp_2)
    return df_parque_actual_cp_2, "Transformacion => fun_cargar_df_parque_actual_cp_2 => df_parque_actual_cp_2" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.PARQUE_ACTUAL_CP
def fun_cargar_df_parque_actual_cp(sqlContext, df_parque_actual_cp_1, df_parque_actual_cp_2):
    t1 = df_parque_actual_cp_1.alias('t1')
    t2 = df_parque_actual_cp_2.alias('t2')
    
    df_parque_actual_cp_tmp =  t1.join(t2, expr("((cast(t1.FECHA_SUBMITTED as DATE) = cast(t2.FECHA_SUBMITTED as DATE)) AND (t1.TELEFONO=t2.TELEFONO))"), how='inner') \
                                .selectExpr(u"t1.TELEFONO", "t1.NUMERO_ABONADO", "t1.LOCALIZACION", "t1.DOMAIN_LOGIN_OW"
                                            , "t1.NOMBRE_USUARIO_OW", "t1.DOMAIN_LOGIN_SUB", "t1.NOMBRE_USUARIO_SUB", "t1.PLAN_CODIGO"
                                            , "t1.PLAN_NOMBRE", "t1.SEGMENTO", "t1.SUB_SEGMENTO", "t1.ESTADO_ABONADO", "t1.DOCUMENTO_CLIENTE"
                                            , "t1.CANAL", "t1.OFICINA", "t1.PROVINCIA", "t1.FECHA_ALTA", "t1.FECHA_CREACION", "t1.FECHA_MODIFICACION"
                                            , "t1.FECHA_SUBMITTED", "t1.ESTADO_ORDEN", "t1.DOMAIN_CAMPO", "t1.MARCA"
                                    )
    
    df_parque_actual_cp_tmp = df_parque_actual_cp_tmp.distinct()
    
    df_parque_actual_cp = df_parque_actual_cp_tmp.cache()
    if val_cp_genera_parque_actual_cp:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual_cp), df_parque_actual_cp)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual_cp))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_parque_actual_cp)
    return df_parque_actual_cp, "Transformacion => fun_cargar_df_parque_actual_cp => df_parque_actual_cp" + str_datos_df



@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.PARQUE_ACTUAL_CP_3
def fun_cargar_df_parque_actual_cp_3(sqlContext, df_parque_actual_cp):
    t1 = df_parque_actual_cp.alias('t1')
    df_parque_actual_cp_3_tmp = t1.selectExpr("telefono", "numero_abonado", "localizacion", "domain_login_ow", "nombre_usuario_ow", "domain_login_sub"
                                    , "nombre_usuario_sub", "plan_codigo", "plan_nombre", "segmento", "sub_segmento", "estado_abonado", "documento_cliente"
                                    , "canal", "oficina", "provincia", "fecha_alta", "fecha_creacion", "fecha_modificacion", "fecha_submitted", "estado_orden"
                                    , "CASE WHEN upper(SUBSTR(t1.DOMAIN_CAMPO,LENGTH(t1.DOMAIN_CAMPO)))='V' THEN SUBSTR(t1.DOMAIN_CAMPO,1,LENGTH(t1.DOMAIN_CAMPO)-1) ELSE t1.DOMAIN_CAMPO END AS DOMAIN_CAMPO" 
                                    , "MARCA"
                                )
                                
    df_parque_actual_cp_3 = df_parque_actual_cp_3_tmp.cache()
    if val_cp_genera_parque_actual_cp_3:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual_cp_3), df_parque_actual_cp_3)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual_cp_3))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_parque_actual_cp_3)
    return df_parque_actual_cp_3, "Transformacion => fun_cargar_df_parque_actual_cp_3 => df_parque_actual_cp_3" + str_datos_df
    

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.PARQUE_ACTUAL_1
def fun_cargar_df_parque_actual_1(sqlContext, val_fecha_proc_bigint, val_ini_mes_actual_ini):
    df_parque_actual_1_tmp = fun_parque_final_feb_tmp(sqlContext, val_cp_base_pro_transfer_consultas, val_cp_otc_t_nc_movi_parque_v1, val_fecha_proc_bigint)
    
    df_parque_actual_1_tmp = df_parque_actual_1_tmp.filter((col('fecha_proceso') == str(val_fecha_proc_bigint)))
    
    df_parque_actual_1_tmp = df_parque_actual_1_tmp.filter(sql_fun.upper(coalesce(col('LINEA_NEGOCIO'), lit(''))) != 'PREPAGO')
    
    df_parque_actual_1_tmp = df_parque_actual_1_tmp.filter((col('FECHA_BAJA_ORI').isNull()))
    
    df_parque_actual_1_tmp = df_parque_actual_1_tmp.filter(col('FECHA_ALTA').cast(DateType()) < val_ini_mes_actual_ini)
    
    df_parque_actual_1_tmp = df_parque_actual_1_tmp.selectExpr(u"SUBSTR(TRIM(NUM_TELEFONICO),LENGTH(TRIM(NUM_TELEFONICO))-8,9) AS TELEFONO", "NUMERO_ABONADO"
                                                        , "LOCALIZACION", "'' AS DOMAIN_LOGIN_OW", "'' AS NOMBRE_USUARIO_OW", "'' AS DOMAIN_LOGIN_SUB"
                                                        , "'' AS NOMBRE_USUARIO_SUB", "PLAN_CODIGO", "PLAN_NOMBRE"
                                                        , "SEGMENTO", "SUB_SEGMENTO", "ESTADO_ABONADO", "DOCUMENTO_CLIENTE", "'' AS CANAL"
                                                        , "'' AS OFICINA", "PROVINCIA", "cast(FECHA_ALTA as date) AS FECHA_ALTA", "FORMA_PAGO"
                                                        , "case when marca is null then 'TELEFONICA' ELSE marca END MARCA"
                                                        , "cast(FEC_ULT_MOD as date) AS FEC_ULT_MOD"
                                                )
    df_tmp = df_parque_actual_1_tmp.alias('df_tmp')
    df_parque_actual_1_tmp = df_tmp.select(df_tmp["TELEFONO"],df_tmp["NUMERO_ABONADO"],df_tmp["LOCALIZACION"],df_tmp["DOMAIN_LOGIN_OW"],df_tmp["NOMBRE_USUARIO_OW"]
                                        , df_tmp["DOMAIN_LOGIN_SUB"], df_tmp["NOMBRE_USUARIO_SUB"], df_tmp["PLAN_CODIGO"]
                                        , split(split(df_parque_actual_1_tmp['PLAN_NOMBRE'],"\\[")[0],"\\#").getItem(0).alias('PLAN_NOMBRE')
                                        , df_tmp["SEGMENTO"], df_tmp["SUB_SEGMENTO"], df_tmp["ESTADO_ABONADO"], df_tmp["DOCUMENTO_CLIENTE"], df_tmp["CANAL"]
                                        , df_tmp["OFICINA"], df_tmp["PROVINCIA"], df_tmp["FECHA_ALTA"], df_tmp["FORMA_PAGO"], df_tmp["MARCA"], df_tmp["FEC_ULT_MOD"]  
                                    )
    
    
    df_parque_actual_1 = df_parque_actual_1_tmp.cache()
    if val_cp_genera_parque_actual_1:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual_1), df_parque_actual_1)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual_1))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_parque_actual_1)
    return df_parque_actual_1, "Transformacion => fun_cargar_df_parque_actual_1 => df_parque_actual_1" + str_datos_df
    

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.PARQUE_ACTUAL_2
def fun_cargar_df_parque_actual_2(sqlContext, df_parque_actual_1):

    windowSpec  = Window.partitionBy('TELEFONO','MARCA').orderBy(desc('FECHA_ALTA'))
    df_parque_actual_2_tmp = df_parque_actual_1.withColumn('rn',row_number().over(windowSpec))
    df_parque_actual_2_tmp = df_parque_actual_2_tmp.filter(col('rn') == 1)
        
    df_parque_actual_2 = df_parque_actual_2_tmp.cache()
    if val_cp_genera_parque_actual_2:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual_2), df_parque_actual_2)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual_2))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_parque_actual_2)
    return df_parque_actual_2, "Transformacion => fun_cargar_df_parque_actual_2 => df_parque_actual_2" + str_datos_df
    

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.PARQUE_ACTUAL   
def fun_cargar_df_parque_actual(sqlContext, df_parque_actual_2):    
    
    df_parque_actual_tmp = df_parque_actual_2
    
    df_parque_actual = df_parque_actual_tmp.cache()
    if val_cp_genera_parque_actual:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual), df_parque_actual)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_parque_actual)
    return df_parque_actual, "Transformacion => fun_cargar_df_parque_actual => df_parque_actual" + str_datos_df



@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.otc_t_CAMBIOS_NUMERO_1  
def fun_cargar_df_cambios_numero_1(sqlContext, val_fecha_mes_ini2, val_fecha_mes_fin2):
    df_cambios_numero_1_tmp = fun_otc_t_vw_phone_number(sqlContext, val_cp_base_rdb_consultas, val_cp_otc_t_vw_phone_number)
    
    df_cambios_numero_1_tmp = df_cambios_numero_1_tmp.filter(col('CHANGE_DATE').between(val_fecha_mes_ini2, val_fecha_mes_fin2))
    df_cambios_numero_1_tmp = df_cambios_numero_1_tmp.selectExpr('customer AS nombre','old_number','new_number', 'change_date')
        
    df_cambios_numero_1 = df_cambios_numero_1_tmp.cache()
    if val_cp_genera_cambios_numero_1:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_cambios_numero_1), df_cambios_numero_1)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_cambios_numero_1))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_cambios_numero_1)
    return df_cambios_numero_1, "Transformacion => fun_cargar_df_cambios_numero_1 => df_cambios_numero_1" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.otc_t_CAMBIOS_NUMERO_2  
def fun_cargar_df_cambios_numero_2(sqlContext, df_cambios_numero_1):

    windowSpec  = Window.partitionBy('OLD_NUMBER').orderBy(desc('CHANGE_DATE'))
    df_cambios_numero_2_tmp = df_cambios_numero_1.withColumn('rn',row_number().over(windowSpec))
    df_cambios_numero_2_tmp = df_cambios_numero_2_tmp.filter(col('rn') == 1)
    
    df_cambios_numero_2 = df_cambios_numero_2_tmp.cache()
    if val_cp_genera_cambios_numero_2:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_cambios_numero_2), df_cambios_numero_2)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_cambios_numero_2))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_cambios_numero_2)
    return df_cambios_numero_2, "Transformacion => fun_cargar_df_cambios_numero_2 => df_cambios_numero_2" + str_datos_df
    

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.PARQUE_ACTUAL_FINAL  
def fun_cargar_df_parque_actual_final(sqlContext, df_parque_actual, df_cambios_numero_2):
    ta = df_parque_actual.alias('ta')
    tb = df_cambios_numero_2.alias('tb')
    
    df_parque_actual_final_tmp = ta.join(tb, expr(" nvl(trim(ta.TELEFONO),'') = nvl(trim(tb.NEW_NUMBER),'') ") , how='left') \
                                .selectExpr( 'ta.TELEFONO AS TELEFONO_ACTUAL', "CASE WHEN (tb.OLD_NUMBER is null or tb.OLD_NUMBER='') THEN ta.TELEFONO ELSE tb.OLD_NUMBER END AS TELEFONO_ANTERIOR"
                                            , 'ta.NUMERO_ABONADO', 'ta.LOCALIZACION', 'ta.DOMAIN_LOGIN_OW', 'ta.NOMBRE_USUARIO_OW', 'ta.DOMAIN_LOGIN_SUB'
                                            , 'ta.NOMBRE_USUARIO_SUB', 'ta.PLAN_CODIGO', 'ta.PLAN_NOMBRE', 'ta.SEGMENTO', 'ta.SUB_SEGMENTO', 'ta.ESTADO_ABONADO'
                                            , 'ta.DOCUMENTO_CLIENTE', 'ta.CANAL', 'ta.OFICINA', 'ta.PROVINCIA', 'ta.FECHA_ALTA', 'ta.FORMA_PAGO', 'ta.MARCA'
                                            , 'ta.FEC_ULT_MOD'
                                    )
    
    
    df_parque_actual_final = df_parque_actual_final_tmp.cache()
    if val_cp_genera_parque_actual_final:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual_final), df_parque_actual_final)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_actual_final))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_parque_actual_final)
    return df_parque_actual_final, "Transformacion => fun_cargar_df_parque_actual_final => df_parque_actual_final" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.PARQUE_ANTERIOR_1  
def fun_cargar_df_parque_anterior_1(sqlContext, df_parque_actual_final, val_ini_mes_actual_bigint_ini, val_ini_mes_actual_ini):
    df_tmp_movi_parque_v1_tmp = fun_otc_nc_movi_parque_v1(sqlContext, val_cp_base_pro_transfer_consultas, val_cp_otc_t_nc_movi_parque_v1)
    
    df_tmp_movi_parque_v1_tmp = df_tmp_movi_parque_v1_tmp.filter( (df_tmp_movi_parque_v1_tmp['FECHA_PROCESO'] == val_ini_mes_actual_bigint_ini) )
    
    df_tmp_movi_parque_v1_tmp = df_tmp_movi_parque_v1_tmp.filter( sql_fun.upper(df_tmp_movi_parque_v1_tmp['LINEA_NEGOCIO']) != 'PREPAGO' )
    
    df_tmp_movi_parque_v1_tmp = df_tmp_movi_parque_v1_tmp.filter( ( (col('FECHA_BAJA').cast(DateType()).isNull()) | (col('FECHA_BAJA')=='') ) 
                                                                    | (col('FECHA_BAJA').cast(DateType()) >= val_ini_mes_actual_ini)
                                                                )
    
    df_tmp_movi_parque_v1_tmp = df_tmp_movi_parque_v1_tmp.selectExpr('SUBSTR(TRIM(NUM_TELEFONICO),LENGTH(TRIM(NUM_TELEFONICO))-8,9) AS TELEFONO'
                                                                        , 'NUMERO_ABONADO AS SUBSCR_NO', 'PLAN_CODIGO AS COD_PLAN_ACTIVO', 'PLAN_NOMBRE'
                                                                        , 'PROVINCIA', "'' AS CIUDAD", "'' AS PARROQUIA", 'SEGMENTO'
                                                                        , 'SUB_SEGMENTO', 'LINEA_NEGOCIO', 'DOCUMENTO_CLIENTE', 'FECHA_ALTA'
                                                                        , "case when marca is null then 'TELEFONICA' ELSE marca END MARCA"
                                                                    )
    
    t1 = df_tmp_movi_parque_v1_tmp.alias('t1')
    df_parque_anterior_1_tmp = t1.select(t1['TELEFONO'], t1['SUBSCR_NO'], t1['COD_PLAN_ACTIVO'], trim(split(split(t1['PLAN_NOMBRE'],"\\[").getItem(0),"\\#").getItem(0)).alias('PLAN')
                    , t1['PROVINCIA'], t1['CIUDAD'], t1['PARROQUIA'], t1['SEGMENTO'], t1['SUB_SEGMENTO'], t1['LINEA_NEGOCIO']
                    , t1['DOCUMENTO_CLIENTE'], t1['FECHA_ALTA'], t1['MARCA']
                )
    df_parque_actual_final_tmp = df_parque_actual_final.select('TELEFONO_ANTERIOR').distinct()
    
    t1 = df_parque_anterior_1_tmp.alias('t1')
    t2 = df_parque_actual_final_tmp.alias('t2')
    
    df_parque_anterior_1_tmp = t1.join(t2, expr(' ( t1.TELEFONO = t2.TELEFONO_ANTERIOR) '), how='inner') \
                                    .selectExpr('TELEFONO', 'SUBSCR_NO', 'COD_PLAN_ACTIVO', 'PLAN', 'PROVINCIA', 'CIUDAD', 'PARROQUIA', 'SEGMENTO' 
                                                    , 'SUB_SEGMENTO', 'LINEA_NEGOCIO', 'DOCUMENTO_CLIENTE', 'FECHA_ALTA', 'MARCA'
                                            )
    
    # print(msg_succ('================================================================'))
    # print(msg_succ('======== fun_cargar_df_parque_anterior_1 ==========='))
    # print(msg_succ('================================================================'))
    # print(msg_succ('df_parque_anterior_1_tmp  %s\n => %s\n') % (df_parque_anterior_1_tmp.printSchema(),str(df_parque_anterior_1_tmp.count())))
    
    df_parque_anterior_1 = df_parque_anterior_1_tmp.cache()
    if val_cp_genera_parque_anterior_1:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_anterior_1), df_parque_anterior_1)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_anterior_1))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_parque_anterior_1)
    return df_parque_anterior_1, "Transformacion => fun_cargar_df_parque_anterior_1 => df_parque_anterior_1" + str_datos_df
    

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.PARQUE_ANTERIOR_2  
def fun_cargar_df_parque_anterior_2(sqlContext, df_parque_anterior_1):
    windowSpec  = Window.partitionBy('TELEFONO','MARCA').orderBy(desc('FECHA_ALTA'))
    df_parque_anterior_2_tmp = df_parque_anterior_1.withColumn('rn',row_number().over(windowSpec))
    df_parque_anterior_2_tmp = df_parque_anterior_2_tmp.filter(col('rn') == 1)
    
    # print(msg_succ('================================================================'))
    # print(msg_succ('======== fun_cargar_df_parque_anterior_2 ==========='))
    # print(msg_succ('================================================================'))
    # print(msg_succ('df_parque_anterior_2_tmp  %s\n => %s\n') % (df_parque_anterior_2_tmp.printSchema(),str(df_parque_anterior_2_tmp.count())))
    
    df_parque_anterior_2 = df_parque_anterior_2_tmp.cache()
    if val_cp_genera_parque_anterior_2:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_anterior_2), df_parque_anterior_2)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_anterior_2))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_parque_anterior_2)
    return df_parque_anterior_2, "Transformacion => fun_cargar_df_parque_anterior_2 => df_parque_anterior_2" + str_datos_df



@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.PARQUE_ANTERIOR  
def fun_cargar_df_parque_anterior(sqlContext, df_parque_anterior_2):
    df_parque_anterior_tmp = df_parque_anterior_2
    
    # print(msg_succ('================================================================'))
    # print(msg_succ('======== fun_cargar_df_parque_anterior ==========='))
    # print(msg_succ('================================================================'))
    # print(msg_succ('df_parque_anterior_tmp  %s\n => %s\n') % (df_parque_anterior_tmp.printSchema(),str(df_parque_anterior_tmp.count())))
    
    df_parque_anterior = df_parque_anterior_tmp.cache()
    if val_cp_genera_parque_anterior:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_anterior), df_parque_anterior)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_parque_anterior))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_parque_anterior)
    return df_parque_anterior, "Transformacion => fun_cargar_df_parque_anterior => df_parque_anterior" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.CTL_POS_USR_NC_cambio_plan  
def fun_cargar_df_ctl_pos_usr_nc_cambio_plan(sqlContext, val_ini_mes_despues_01):
    df_ctl_pos_usr_nc_cambio_plan_tmp = fun_otc_t_ctl_pos_usr_nc(sqlContext, val_cp_base_pro_transfer_consultas, val_cp_otc_t_ctl_pos_usr_nc)
    
    df_ctl_pos_usr_nc_cambio_plan_tmp = df_ctl_pos_usr_nc_cambio_plan_tmp.filter(col('FECHA').cast(DateType()) < val_ini_mes_despues_01)
    
    windowSpec  = Window.partitionBy('USUARIO').orderBy(desc('USUARIO'), desc('FECHA'), desc('CODIGO_PLAZA'), desc('CANAL'), desc('CAMPANIA'), desc('OFICINA'), desc('CODIGO_DISTRIBUIDOR'), desc('CIUDAD'), desc('PROVINCIA'), desc('REGION'), desc('SUB_CANAL'))
    df_ctl_pos_usr_nc_cambio_plan_tmp = df_ctl_pos_usr_nc_cambio_plan_tmp.withColumn('rn',row_number().over(windowSpec))
    df_ctl_pos_usr_nc_cambio_plan_tmp = df_ctl_pos_usr_nc_cambio_plan_tmp.filter(col('rn') == 1)
    
    # print(msg_succ('================================================================'))
    # print(msg_succ('======== fun_cargar_df_ctl_pos_usr_nc_cambio_plan =============='))
    # print(msg_succ('================================================================'))
    # print(msg_succ('df_ctl_pos_usr_nc_cambio_plan_tmp  %s\n => %s\n') % (df_ctl_pos_usr_nc_cambio_plan_tmp.printSchema(),str(df_ctl_pos_usr_nc_cambio_plan_tmp.count())))
    
    df_ctl_pos_usr_nc_cambio_plan = df_ctl_pos_usr_nc_cambio_plan_tmp.cache()
    if val_cp_genera_otc_t_ctl_pos_usr_nc:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_otc_t_ctl_pos_usr_nc), df_ctl_pos_usr_nc_cambio_plan)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_otc_t_ctl_pos_usr_nc))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_ctl_pos_usr_nc_cambio_plan)
    return df_ctl_pos_usr_nc_cambio_plan, "Transformacion => fun_cargar_df_ctl_pos_usr_nc_cambio_plan => df_ctl_pos_usr_nc_cambio_plan" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.CTL_POS_USR_NC_cambio_plan_union  
def fun_cargar_df_ctl_pos_usr_nc_cambio_plan_union(sqlContext, df_ctl_pos_usr_nc_cambio_plan):

    df_tmp_1 = df_ctl_pos_usr_nc_cambio_plan.selectExpr('usuario','nom_usuario', 'canal', 'campania', 'oficina', 'codigo_distribuidor', 'nom_distribuidor'
                                                        , 'codigo_plaza', 'nom_plaza', 'ciudad', 'provincia', 'region', 'sub_canal', 'fecha', 'rn'
                                                    )
    df_tmp_2 = df_ctl_pos_usr_nc_cambio_plan.selectExpr("concat(usuario,'V') as usuario",'nom_usuario', 'canal', 'campania', 'oficina', 'codigo_distribuidor', 'nom_distribuidor'
                                                        , 'codigo_plaza', 'nom_plaza', 'ciudad', 'provincia', 'region', 'sub_canal', 'fecha', 'rn'
                                                    )
                                                    
    df_ctl_pos_usr_nc_cambio_plan_union_tmp = df_tmp_1.union(df_tmp_2)
    
    # print(msg_succ('======================================================================'))
    # print(msg_succ('======== fun_cargar_df_ctl_pos_usr_nc_cambio_plan_union =============='))
    # print(msg_succ('======================================================================'))
    # print(msg_succ('df_ctl_pos_usr_nc_cambio_plan_union_tmp  %s\n => %s\n') % (df_ctl_pos_usr_nc_cambio_plan_union_tmp.printSchema(),str(df_ctl_pos_usr_nc_cambio_plan_union_tmp.count())))
    
    df_ctl_pos_usr_nc_cambio_plan_union = df_ctl_pos_usr_nc_cambio_plan_union_tmp.cache()
    if val_cp_genera_ctl_pos_usr_nc_cambio_plan_union:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_otc_t_ctl_pos_usr_nc_cambio_plan_union), df_ctl_pos_usr_nc_cambio_plan_union)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_otc_t_ctl_pos_usr_nc_cambio_plan_union))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_ctl_pos_usr_nc_cambio_plan_union)
    return df_ctl_pos_usr_nc_cambio_plan_union, "Transformacion => fun_cargar_df_ctl_pos_usr_nc_cambio_plan_union => df_ctl_pos_usr_nc_cambio_plan_union" + str_datos_df
    

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.usuario 
def fun_cargar_df_usuario(sqlContext, df_ctl_pos_usr_nc_cambio_plan_union, val_fecha_proc):
    df_usuario_tmp = df_ctl_pos_usr_nc_cambio_plan_union.filter(col('FECHA') < val_fecha_proc)
    
    # print(msg_succ('======================================================================'))
    # print(msg_succ('====================== fun_cargar_df_usuario ========================='))
    # print(msg_succ('======================================================================'))
    # print(msg_succ('df_usuario_tmp  %s\n => %s\n') % (df_usuario_tmp.printSchema(),str(df_usuario_tmp.count())))
    
    df_usuario = df_usuario_tmp.cache()
    if val_cp_genera_usuario:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_usuario), df_usuario)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_usuario))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_usuario)
    return df_usuario, "Transformacion => fun_cargar_df_usuario => df_usuario" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.tmp_cambio_plan_1_1_2 
def fun_cargar_df_tmp_cambio_plan_1_1_2(sqlContext, df_parque_actual_final, df_parque_anterior):
    ta = df_parque_actual_final.alias('ta')
    tb = df_parque_anterior.alias('tb')
    
    df_tmp_cambio_plan_1_1_2_tmp = ta.join(tb, expr(" ( nvl(trim(ta.TELEFONO_ANTERIOR),'') = nvl(trim(tb.TELEFONO),'') ) AND ( (nvl(trim(tb.COD_PLAN_ACTIVO),'') <> nvl(trim(ta.PLAN_CODIGO),'')) OR (tb.COD_PLAN_ACTIVO is null) ) "), how='inner') \
                                    .selectExpr('ta.telefono_actual', 'ta.telefono_anterior', 'ta.numero_abonado', 'ta.localizacion', 'ta.domain_login_ow', 'nombre_usuario_ow'
                                            , 'ta.domain_login_sub', 'ta.nombre_usuario_sub', 'ta.plan_codigo', 'ta.plan_nombre', 'ta.segmento'
                                            , "case when substr(ta.sub_segmento,1,5)='Peque' then 'Pequenas' else ta.sub_segmento end as sub_segmento"
                                            , 'ta.estado_abonado', 'ta.documento_cliente', 'ta.canal', 'ta.oficina', 'ta.provincia', 'ta.fecha_alta', 'ta.forma_pago'
                                            , 'tb.COD_PLAN_ACTIVO', 'tb.PLAN', 'tb.TELEFONO', 'ta.MARCA', 'ta.FEC_ULT_MOD'
                                    )
    
    df_tmp_cambio_plan_1_1_2_tmp = df_tmp_cambio_plan_1_1_2_tmp.distinct()
    
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('====================== fun_cargar_df_tmp_cambio_plan_1_1_2 ========================='))
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('df_tmp_cambio_plan_1_1_2_tmp  %s\n => %s\n') % (df_tmp_cambio_plan_1_1_2_tmp.printSchema(),str(df_tmp_cambio_plan_1_1_2_tmp.count())))
    
    df_tmp_cambio_plan_1_1_2 = df_tmp_cambio_plan_1_1_2_tmp.cache()
    if val_cp_genera_tmp_cambio_plan_1_1_2:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_tmp_cambio_plan_1_1_2), df_tmp_cambio_plan_1_1_2)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_tmp_cambio_plan_1_1_2))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_tmp_cambio_plan_1_1_2)
    return df_tmp_cambio_plan_1_1_2, "Transformacion => fun_cargar_df_tmp_cambio_plan_1_1_2 => df_tmp_cambio_plan_1_1_2" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.TMP_CAMBIO_PLAN_1_1 
def fun_cargar_df_tmp_cambio_plan_1_1(sqlContext, df_tmp_cambio_plan_1_1_2, df_parque_actual_cp_3, df_plan, df_usuario):
    
    ta = df_tmp_cambio_plan_1_1_2.alias('ta')
    tc = df_parque_actual_cp_3.alias('tc')
    td = df_plan.alias('td')
    te = df_plan.alias('te')
    df_homologacion_segmentos = fun_otc_t_homologacion_segmentos(sqlContext, val_cp_base_pro_transfer_consultas, val_cp_otc_t_homologacion_segmentos)
    df_homologacion_segmentos = df_homologacion_segmentos.select('segmentacion', 'segmento', 'segmento_fin').distinct()
    tf = df_homologacion_segmentos.alias('tf')
    th = df_usuario.alias('th')
    
    ta_tc = ta.join(tc, expr(" (nvl(trim(ta.TELEFONO_ACTUAL),'') = nvl(trim(tc.TELEFONO),'')) "), how='left') \
                .selectExpr( 'ta.TELEFONO_ACTUAL AS TELEFONO', 'ta.NUMERO_ABONADO', 'UPPER(ta.SUB_SEGMENTO) AS SUB_SEGMENTO'
                            , 'ta.ESTADO_ABONADO', 'ta.DOCUMENTO_CLIENTE', 'ta.FORMA_PAGO', 'ta.PROVINCIA', 'ta.PLAN_CODIGO AS CODIGO_PLAN_ACTUAL'
                            , 'ta.PLAN_NOMBRE', 'ta.COD_PLAN_ACTIVO AS CODIGO_PLAN_ANTERIOR', 'ta.PLAN AS DESCRIPCION_PLAN_ANTERIOR'
                            , 'ta.FEC_ULT_MOD', 'ta.MARCA'
                            , "CASE WHEN tc.FECHA_SUBMITTED is null or tc.FECHA_SUBMITTED = '' THEN ta.FEC_ULT_MOD ELSE tc.FECHA_SUBMITTED END AS FECHA_CAMBIO_PLAN"
                            , 'tc.DOMAIN_LOGIN_OW AS CODIGO_USUARIO_ORDEN', 'tc.NOMBRE_USUARIO_OW AS NOMBRE_USUARIO_ORDEN', 'tc.DOMAIN_LOGIN_SUB AS CODIGO_USUARIO_SUBMIT'
                            , 'tc.NOMBRE_USUARIO_SUB AS NOMBRE_USUARIO_SUBMIT', "CASE WHEN  ta.TELEFONO IS NOT NULL THEN 'SI' ELSE 'NO' END AS CP_CRUCE_PARQUE"
                            , "CASE WHEN tc.TELEFONO IS NULL THEN 'NO' ELSE 'SI' END AS CP_NC"
                            , 'ta.PLAN_CODIGO AS CLAVE_PLAN_CODIGO', 'ta.COD_PLAN_ACTIVO AS CLAVE_COD_PLAN_ACTIVO', 'ta.TELEFONO AS TELEFONO_TMP'
                            , 'tc.DOMAIN_LOGIN_OW AS CLAVE_DOMAIN_LOGIN_OW', 'tc.DOMAIN_LOGIN_SUB AS CLAVE_DOMAIN_LOGIN_SUB'
                        )
                            
    ta_tc = ta_tc.alias('tac')
    
    ta_tc_td = ta_tc.join(td, expr(" (nvl(trim(tac.CLAVE_PLAN_CODIGO),'') = nvl(trim(td.PLANCD),'')) "), how='left') \
                .selectExpr( 'tac.TELEFONO', 'tac.NUMERO_ABONADO', 'tac.SUB_SEGMENTO', 'tac.ESTADO_ABONADO', 'tac.DOCUMENTO_CLIENTE'
                        , 'tac.FORMA_PAGO', 'tac.PROVINCIA', 'tac.CODIGO_PLAN_ACTUAL', 'tac.PLAN_NOMBRE', 'tac.CODIGO_PLAN_ANTERIOR'
                        , 'tac.DESCRIPCION_PLAN_ANTERIOR', 'tac.FEC_ULT_MOD', 'tac.MARCA', 'tac.FECHA_CAMBIO_PLAN'
                        , 'tac.CODIGO_USUARIO_ORDEN', 'tac.NOMBRE_USUARIO_ORDEN', 'tac.CODIGO_USUARIO_SUBMIT', 'tac.NOMBRE_USUARIO_SUBMIT'
                        , 'tac.CP_CRUCE_PARQUE', 'tac.CP_NC'
                        , 'td.CATEGORIA AS CATEGORIA_PLAN_ACTUAL', 'td.TARIFA_BASICA AS TARIFA_BASICA_ACTUAL', 'td.COMERCIAL AS COMERCIAL_ANTERIOR', 'td.TARIFA_BASICA'
                        , 'tac.CLAVE_PLAN_CODIGO', 'tac.CLAVE_COD_PLAN_ACTIVO', 'td.CATEGORIA AS CATEGORIA_TMP', 'tac.TELEFONO_TMP'
                        , 'tac.CLAVE_DOMAIN_LOGIN_OW', 'tac.CLAVE_DOMAIN_LOGIN_SUB'
                    )
                    
    ta_tc_td = ta_tc_td.alias('tacd')
    
    ta_tc_td_te = ta_tc_td.join(te, expr(" (nvl(trim(tacd.CLAVE_COD_PLAN_ACTIVO),'') = nvl(trim(te.PLANCD),'')) "), how='left') \
                    .selectExpr( 'tacd.TELEFONO', 'tacd.NUMERO_ABONADO', 'tacd.SUB_SEGMENTO', 'tacd.ESTADO_ABONADO', 'tacd.DOCUMENTO_CLIENTE'
                        , 'tacd.FORMA_PAGO', 'tacd.PROVINCIA', 'tacd.CODIGO_PLAN_ACTUAL', 'tacd.PLAN_NOMBRE', 'tacd.CODIGO_PLAN_ANTERIOR'
                        , 'tacd.DESCRIPCION_PLAN_ANTERIOR', 'tacd.FEC_ULT_MOD', 'tacd.MARCA', 'tacd.FECHA_CAMBIO_PLAN'
                        , 'tacd.CODIGO_USUARIO_ORDEN', 'tacd.NOMBRE_USUARIO_ORDEN', 'tacd.CODIGO_USUARIO_SUBMIT', 'tacd.NOMBRE_USUARIO_SUBMIT'
                        , 'tacd.CP_CRUCE_PARQUE', 'tacd.CP_NC'
                        , 'tacd.CATEGORIA_PLAN_ACTUAL', 'tacd.TARIFA_BASICA_ACTUAL', 'tacd.COMERCIAL_ANTERIOR', 'tacd.TARIFA_BASICA'
                        , 'te.CATEGORIA AS CATEGORIA_PLAN_ANTERIOR', 'te.TARIFA_BASICA AS TARIFA_BASICA_ANTERIOR', 'te.COMERCIAL AS COMERCIAL_ACTUAL'
                        , '(tacd.TARIFA_BASICA-te.TARIFA_BASICA)     AS DELTA'
                        , "CASE WHEN tacd.TELEFONO_TMP IS NOT NULL AND tacd.CATEGORIA_TMP <> te.CATEGORIA THEN 'ALTA_BAJA' WHEN tacd.TELEFONO_TMP IS NOT NULL AND tacd.CATEGORIA_TMP = te.CATEGORIA AND (tacd.TARIFA_BASICA-te.TARIFA_BASICA) > 0 THEN 'UPSELL' WHEN tacd.TELEFONO_TMP IS NOT NULL  AND (tacd.CATEGORIA_TMP = te.CATEGORIA) AND (tacd.TARIFA_BASICA-te.TARIFA_BASICA) < 0 THEN 'DOWNSELL' WHEN tacd.TELEFONO_TMP IS NOT NULL AND (tacd.CATEGORIA_TMP = te.CATEGORIA) AND (tacd.TARIFA_BASICA-te.TARIFA_BASICA) = 0 THEN 'MISMA_TARIFA' ELSE 'NO_DEFINIDO' END  AS TIPO_MOVIMIENTO"
                        , 'tacd.CLAVE_PLAN_CODIGO', 'tacd.CLAVE_COD_PLAN_ACTIVO', 'tacd.CATEGORIA_TMP', 'tacd.TELEFONO_TMP'
                        , 'tacd.CLAVE_DOMAIN_LOGIN_OW', 'tacd.CLAVE_DOMAIN_LOGIN_SUB'
                    )
    
    ta_tc_td_te =  ta_tc_td_te.alias('tacde')
    
    ta_tc_td_te_tf = ta_tc_td_te.join(tf, expr(" (UPPER(nvl(trim(tacde.SUB_SEGMENTO),'')) = if(substr(UPPER(nvl(trim(tf.SEGMENTACION),'')),1,5)='PEQUE','PEQUENAS',UPPER(nvl(trim(tf.SEGMENTACION),'')))) "), how='left') \
                                .selectExpr( 'tacde.TELEFONO', 'tacde.NUMERO_ABONADO', 'tacde.SUB_SEGMENTO', 'tacde.ESTADO_ABONADO', 'tacde.DOCUMENTO_CLIENTE'
                                            , 'tacde.FORMA_PAGO', 'tacde.PROVINCIA', 'tacde.CODIGO_PLAN_ACTUAL', 'tacde.PLAN_NOMBRE', 'tacde.CODIGO_PLAN_ANTERIOR'
                                            , 'tacde.DESCRIPCION_PLAN_ANTERIOR', 'tacde.FEC_ULT_MOD', 'tacde.MARCA', 'tacde.FECHA_CAMBIO_PLAN'
                                            , 'tacde.CODIGO_USUARIO_ORDEN', 'tacde.NOMBRE_USUARIO_ORDEN', 'tacde.CODIGO_USUARIO_SUBMIT', 'tacde.NOMBRE_USUARIO_SUBMIT'
                                            , 'tacde.CP_CRUCE_PARQUE', 'tacde.CP_NC'
                                            , 'tacde.CATEGORIA_PLAN_ACTUAL', 'tacde.TARIFA_BASICA_ACTUAL', 'tacde.COMERCIAL_ANTERIOR', 'tacde.TARIFA_BASICA'
                                            , 'tacde.CATEGORIA_PLAN_ANTERIOR', 'tacde.TARIFA_BASICA_ANTERIOR', 'tacde.COMERCIAL_ACTUAL'
                                            , 'tacde.DELTA', "tacde.TIPO_MOVIMIENTO"
                                            , 'UPPER(tf.SEGMENTO_FIN) AS SEGMENTO'
                                            , 'tacde.CLAVE_PLAN_CODIGO', 'tacde.CLAVE_COD_PLAN_ACTIVO', 'tacde.CATEGORIA_TMP', 'tacde.TELEFONO_TMP'
                                            , 'tacde.CLAVE_DOMAIN_LOGIN_OW', 'tacde.CLAVE_DOMAIN_LOGIN_SUB'                                            
                                        )
                                        
    ta_tc_td_te_tf = ta_tc_td_te_tf.alias('tacdef')
    
    ta_tc_td_te_tf_th = ta_tc_td_te_tf.join(th, expr(" TRIM(CASE WHEN tacdef.CLAVE_DOMAIN_LOGIN_OW = 'internal' AND tacdef.CLAVE_DOMAIN_LOGIN_SUB <> 'internal' THEN tacdef.CLAVE_DOMAIN_LOGIN_SUB WHEN tacdef.CLAVE_DOMAIN_LOGIN_OW <> 'internal' AND tacdef.CLAVE_DOMAIN_LOGIN_SUB = 'internal' THEN tacdef.CLAVE_DOMAIN_LOGIN_OW WHEN tacdef.CLAVE_DOMAIN_LOGIN_OW <> 'internal' AND tacdef.CLAVE_DOMAIN_LOGIN_SUB <> 'internal' THEN tacdef.CLAVE_DOMAIN_LOGIN_OW ELSE tacdef.CLAVE_DOMAIN_LOGIN_OW END) = TRIM(th.USUARIO) "), how='left') \
                                    .selectExpr( 'tacdef.TELEFONO', 'tacdef.NUMERO_ABONADO', 'tacdef.SUB_SEGMENTO', 'tacdef.ESTADO_ABONADO', 'tacdef.DOCUMENTO_CLIENTE'
                                            , 'tacdef.FORMA_PAGO', 'tacdef.PROVINCIA', 'tacdef.CODIGO_PLAN_ACTUAL', 'tacdef.PLAN_NOMBRE', 'tacdef.CODIGO_PLAN_ANTERIOR'
                                            , 'tacdef.DESCRIPCION_PLAN_ANTERIOR', 'tacdef.FEC_ULT_MOD', 'tacdef.MARCA', 'tacdef.FECHA_CAMBIO_PLAN'
                                            , 'tacdef.CODIGO_USUARIO_ORDEN', 'tacdef.NOMBRE_USUARIO_ORDEN', 'tacdef.CODIGO_USUARIO_SUBMIT', 'tacdef.NOMBRE_USUARIO_SUBMIT'
                                            , 'tacdef.CP_CRUCE_PARQUE', 'tacdef.CP_NC'
                                            , 'tacdef.CATEGORIA_PLAN_ACTUAL', 'tacdef.TARIFA_BASICA_ACTUAL', 'tacdef.COMERCIAL_ANTERIOR', 'tacdef.TARIFA_BASICA'
                                            , 'tacdef.CATEGORIA_PLAN_ANTERIOR', 'tacdef.TARIFA_BASICA_ANTERIOR', 'tacdef.COMERCIAL_ACTUAL'
                                            , 'tacdef.DELTA', "tacdef.TIPO_MOVIMIENTO", 'tacdef.SEGMENTO'
                                            , 'th.CANAL', 'th.OFICINA', 'th.CAMPANIA', 'th.NOM_DISTRIBUIDOR', 'th.CIUDAD AS CIUDAD_DISTRIBUIDOR'
                                            , 'th.PROVINCIA AS PROVINCIA_DISTRIBUIDOR', 'th.REGION AS REGION_DISTRIBUIDOR'
                                            , 'th.SUB_CANAL', 'th.USUARIO AS CODIGO_USUARIO'
                                    )
    
    df_tmp_cambio_plan_1_1_tmp = ta_tc_td_te_tf_th.distinct()
    
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('====================== fun_cargar_df_tmp_cambio_plan_1_1 ==========================='))
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('df_tmp_cambio_plan_1_1_tmp  %s\n => %s\n') % (df_tmp_cambio_plan_1_1_tmp.printSchema(),str(df_tmp_cambio_plan_1_1_tmp.count())))
    
    df_tmp_cambio_plan_1_1 = df_tmp_cambio_plan_1_1_tmp.cache()
    if val_cp_genera_tmp_cambio_plan_1_1:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_tmp_cambio_plan_1_1), df_tmp_cambio_plan_1_1)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_tmp_cambio_plan_1_1))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_tmp_cambio_plan_1_1)
    return df_tmp_cambio_plan_1_1, "Transformacion => fun_cargar_df_tmp_cambio_plan_1_1 => df_tmp_cambio_plan_1_1" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.CP_VALIDAR_PA 
def fun_cargar_df_cp_validar_pa(sqlContext, df_tmp_cambio_plan_1_1):
    df_cp_validar_pa_tmp = df_tmp_cambio_plan_1_1
    
    df_cp_validar_pa_tmp = df_tmp_cambio_plan_1_1.filter( (col('CP_NC')=='NO') & (col('CP_CRUCE_PARQUE')=='SI') )
    
    df_cp_validar_pa_tmp = df_cp_validar_pa_tmp.selectExpr( 'PLAN_NOMBRE AS DESCRIPCION_PLAN_ACTUAL', 'TELEFONO', 'NUMERO_ABONADO'
                        , 'SEGMENTO', 'SUB_SEGMENTO', 'ESTADO_ABONADO', 'DOCUMENTO_CLIENTE'
                        , 'FORMA_PAGO', 'PROVINCIA', 'CODIGO_PLAN_ACTUAL', 'PLAN_NOMBRE', 'CATEGORIA_PLAN_ACTUAL'
                        , 'TARIFA_BASICA_ACTUAL', 'COMERCIAL_ANTERIOR', 'CODIGO_PLAN_ANTERIOR', 'DESCRIPCION_PLAN_ANTERIOR'
                        , 'CATEGORIA_PLAN_ANTERIOR', 'TARIFA_BASICA_ANTERIOR', 'COMERCIAL_ACTUAL', 'FECHA_CAMBIO_PLAN'
                        , 'CODIGO_USUARIO_ORDEN', 'NOMBRE_USUARIO_ORDEN', 'CODIGO_USUARIO_SUBMIT', 'NOMBRE_USUARIO_SUBMIT'
                        , 'CANAL', 'OFICINA', 'CAMPANIA', 'NOM_DISTRIBUIDOR', 'CIUDAD_DISTRIBUIDOR', 'PROVINCIA_DISTRIBUIDOR'
                        , 'REGION_DISTRIBUIDOR', 'DELTA', 'TIPO_MOVIMIENTO', 'CP_CRUCE_PARQUE', 'CP_NC', 'SUB_CANAL', 'CODIGO_USUARIO'
                        , 'MARCA'
                    )
    
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('=========================== fun_cargar_df_cp_validar_pa ==========================='))
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('df_cp_validar_pa_tmp  %s\n => %s\n') % (df_cp_validar_pa_tmp.printSchema(),str(df_cp_validar_pa_tmp.count())))
    
    df_cp_validar_pa = df_cp_validar_pa_tmp.cache()
    if val_cp_genera_cp_validar_pa:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_cp_validar_pa), df_cp_validar_pa)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_cp_validar_pa))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_cp_validar_pa)
    return df_cp_validar_pa, "Transformacion => fun_cargar_df_cp_validar_pa => df_cp_validar_pa" + str_datos_df
    

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.CP_VALIDAR_NC 
def fun_cargar_df_cp_validar_nc(sqlContext, df_tmp_cambio_plan_1_1):
    df_cp_validar_nc_tmp = df_tmp_cambio_plan_1_1
    
    df_cp_validar_nc_tmp = df_tmp_cambio_plan_1_1.filter( (coalesce(trim(col('CP_NC')), lit('')) == 'SI') & (coalesce(trim(col('CP_CRUCE_PARQUE')), lit('')) == 'NO') )
    
    df_cp_validar_nc_tmp = df_cp_validar_nc_tmp.selectExpr('PLAN_NOMBRE AS DESCRIPCION_PLAN_ACTUAL', 'TELEFONO', 'NUMERO_ABONADO', 'SEGMENTO'
                                        , 'SUB_SEGMENTO', 'ESTADO_ABONADO', 'DOCUMENTO_CLIENTE', 'FORMA_PAGO', 'PROVINCIA', 'CODIGO_PLAN_ACTUAL'
                                        , 'PLAN_NOMBRE', 'CATEGORIA_PLAN_ACTUAL', 'TARIFA_BASICA_ACTUAL', 'COMERCIAL_ANTERIOR', 'CODIGO_PLAN_ANTERIOR'
                                        , 'DESCRIPCION_PLAN_ANTERIOR', 'CATEGORIA_PLAN_ANTERIOR', 'TARIFA_BASICA_ANTERIOR', 'COMERCIAL_ACTUAL', 'FECHA_CAMBIO_PLAN'
                                        , 'CODIGO_USUARIO_ORDEN', 'NOMBRE_USUARIO_ORDEN', 'CODIGO_USUARIO_SUBMIT', 'NOMBRE_USUARIO_SUBMIT', 'CANAL', 'OFICINA'
                                        , 'CAMPANIA', 'NOM_DISTRIBUIDOR', 'CIUDAD_DISTRIBUIDOR', 'PROVINCIA_DISTRIBUIDOR', 'REGION_DISTRIBUIDOR', 'DELTA'
                                        , 'TIPO_MOVIMIENTO', 'CP_CRUCE_PARQUE', 'CP_NC', 'SUB_CANAL', 'CODIGO_USUARIO', 'MARCA'
                                    )
    
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('=========================== fun_cargar_df_cp_validar_nc ==========================='))
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('df_cp_validar_nc_tmp  %s\n => %s\n') % (df_cp_validar_nc_tmp.printSchema(),str(df_cp_validar_nc_tmp.count())))
    
    df_cp_validar_nc = df_cp_validar_nc_tmp.cache()
    if val_cp_genera_cp_validar_nc:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_cp_validar_nc), df_cp_validar_nc)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_cp_validar_nc))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_cp_validar_nc)
    return df_cp_validar_nc, "Transformacion => fun_cargar_df_cp_validar_nc => df_cp_validar_nc" + str_datos_df
    

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.CAMBIO_PLAN 
def fun_cargar_df_cambio_plan(sqlContext, df_tmp_cambio_plan_1_1):

    df_cambio_plan_tmp = df_tmp_cambio_plan_1_1
    
    df_cambio_plan_tmp = df_cambio_plan_tmp.filter( (coalesce(trim(col('CP_CRUCE_PARQUE')), lit('')) == 'SI') & (coalesce(trim(col('TIPO_MOVIMIENTO')), lit('')).isin(['UPSELL','DOWNSELL','MISMA_TARIFA'])) )
    df_cambio_plan_tmp = df_cambio_plan_tmp.selectExpr('PLAN_NOMBRE AS DESCRIPCION_PLAN_ACTUAL', 'TELEFONO', 'NUMERO_ABONADO', 'SEGMENTO', 'SUB_SEGMENTO', 'ESTADO_ABONADO', 'DOCUMENTO_CLIENTE'
                            , 'FORMA_PAGO', 'PROVINCIA', 'CODIGO_PLAN_ACTUAL', 'PLAN_NOMBRE', 'CATEGORIA_PLAN_ACTUAL', 'TARIFA_BASICA_ACTUAL', 'COMERCIAL_ANTERIOR'
                            , 'CODIGO_PLAN_ANTERIOR', 'DESCRIPCION_PLAN_ANTERIOR', 'CATEGORIA_PLAN_ANTERIOR', 'TARIFA_BASICA_ANTERIOR', 'COMERCIAL_ACTUAL'
                            , 'FECHA_CAMBIO_PLAN', 'CODIGO_USUARIO_ORDEN', 'NOMBRE_USUARIO_ORDEN', 'CODIGO_USUARIO_SUBMIT', 'NOMBRE_USUARIO_SUBMIT', 'CANAL'
                            , 'OFICINA', 'CAMPANIA', 'NOM_DISTRIBUIDOR', 'CIUDAD_DISTRIBUIDOR', 'PROVINCIA_DISTRIBUIDOR', 'REGION_DISTRIBUIDOR', 'DELTA'
                            , 'TIPO_MOVIMIENTO', 'CP_CRUCE_PARQUE', 'CP_NC', 'SUB_CANAL', 'CODIGO_USUARIO', 'MARCA'
                        )
    
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('=========================== fun_cargar_df_cambio_plan =============================='))
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('df_cambio_plan_tmp  %s\n => %s\n') % (df_cambio_plan_tmp.printSchema(),str(df_cambio_plan_tmp.count())))
    
    df_cambio_plan = df_cambio_plan_tmp.cache()
    if val_cp_genera_cambio_plan:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_cambio_plan), df_cambio_plan)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_cambio_plan))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_cambio_plan)
    return df_cambio_plan, "Transformacion => fun_cargar_df_cambio_plan => df_cambio_plan" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.TMP_CAMBIO_PLAN_FNL 
def fun_cargar_df_tmp_cambio_plan_fnl(sqlContext, df_cambio_plan, val_fecha_proc_mes_anio_bigint, val_fecha_proc, val_ini_mes_actual_ini, val_fecha_dia_antes_actual):

    df_tmp_cambio_plan_fnl_tmp = df_cambio_plan
    
    df_tmp_cambio_plan_fnl_tmp = df_tmp_cambio_plan_fnl_tmp.withColumn('ANIO_MES',lit(val_fecha_proc_mes_anio_bigint))
    df_tmp_cambio_plan_fnl_tmp = df_tmp_cambio_plan_fnl_tmp.withColumn('FECHA_CARGA',lit(val_fecha_proc))
    
    df_tmp_cambio_plan_fnl_tmp = df_tmp_cambio_plan_fnl_tmp.filter( col('FECHA_CAMBIO_PLAN').cast(DateType()).between(str(val_ini_mes_actual_ini), str(val_fecha_dia_antes_actual)) )
    df_tmp_cambio_plan_fnl_tmp = df_tmp_cambio_plan_fnl_tmp.selectExpr( 'TELEFONO', 'NUMERO_ABONADO', 'SEGMENTO', 'SUB_SEGMENTO', 'ESTADO_ABONADO', 'DOCUMENTO_CLIENTE', 'PROVINCIA', 'FORMA_PAGO'
                                            , 'CODIGO_PLAN_ACTUAL', 'DESCRIPCION_PLAN_ACTUAL', 'CATEGORIA_PLAN_ACTUAL', 'TARIFA_BASICA_ACTUAL', 'COMERCIAL_ACTUAL', 'CODIGO_PLAN_ANTERIOR'
                                            , 'DESCRIPCION_PLAN_ANTERIOR', 'CATEGORIA_PLAN_ANTERIOR', 'TARIFA_BASICA_ANTERIOR', 'COMERCIAL_ANTERIOR', 'FECHA_CAMBIO_PLAN'
                                            , 'CODIGO_USUARIO_ORDEN', 'NOMBRE_USUARIO_ORDEN', 'CODIGO_USUARIO_SUBMIT', 'NOMBRE_USUARIO_SUBMIT', 'CANAL', 'OFICINA', 'CAMPANIA'
                                            , 'NOM_DISTRIBUIDOR', 'CIUDAD_DISTRIBUIDOR', 'PROVINCIA_DISTRIBUIDOR', 'REGION_DISTRIBUIDOR', 'DELTA', 'TIPO_MOVIMIENTO', 'CP_CRUCE_PARQUE'
                                            , 'CP_NC', 'SUB_CANAL', 'CODIGO_USUARIO', 'MARCA', 'ANIO_MES', 'FECHA_CARGA'
                                    )
        
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('=================== fun_cargar_df_tmp_cambio_plan_fnl =============================='))
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('df_tmp_cambio_plan_fnl_tmp  %s\n => %s\n') % (df_tmp_cambio_plan_fnl_tmp.printSchema(),str(df_tmp_cambio_plan_fnl_tmp.count())))
    
    df_tmp_cambio_plan_fnl = df_tmp_cambio_plan_fnl_tmp.cache()
    if val_cp_genera_tmp_cambio_plan_fnl:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_tmp_cambio_plan_fnl), df_tmp_cambio_plan_fnl)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_tmp_cambio_plan_fnl))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_tmp_cambio_plan_fnl)
    return df_tmp_cambio_plan_fnl, "Transformacion => fun_cargar_df_tmp_cambio_plan_fnl => df_tmp_cambio_plan_fnl" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.OVERRIDE_ACTUAL 
def fun_cargar_df_override_actual(sqlContext, val_fecha_dia_bigint, val_ini_mes_actual_ini, val_fecha_proc):
    df_override_actual_tmp = fun_otc_t_overwrite_planes(sqlContext, val_cp_base_pro_transfer_consultas, val_cp_otc_t_overwrite_planes)
    
    df_override_actual_tmp = df_override_actual_tmp.filter( (col('p_fecha_proceso') == val_fecha_dia_bigint) & (col('CREATED_WHEN_ORDEN').cast(DateType()).between(str(val_ini_mes_actual_ini),str(val_fecha_proc))) )
    
    windowSpec  = Window.partitionBy('PHONE_NUMBER').orderBy(desc('CREATED_WHEN_ORDEN'))
    df_override_actual_tmp = df_override_actual_tmp.withColumn('ROWNUM',row_number().over(windowSpec))
    df_override_actual_tmp = df_override_actual_tmp.filter(col('ROWNUM') == 1)
    
    df_override_actual_tmp = df_override_actual_tmp.selectExpr('ORDEN_NAME','NUMERO_ORDEN','ESTADO_ORDEN','PHONE_NUMBER AS TELEFONO','TIPO_PROCESO_ESP'
                                        , 'MRC_BASE_PRICE', 'MRC_OV_CREATED_BY AS USU_APLICA_OV_PLAN_ACT', 'MRC_OV_PRICE AS TARIFA_OV_PLAN_ACT'
                                        , 'NRC_OV_CREATED_WHEN', 'CAST(CREATED_WHEN_ORDEN AS DATE) AS FECHA_APLICA_OV_PLAN_ACT'
                                        , 'SUBMITTED_WHEN_ORDEN'#, 'PHONE_NUMBER', 'CREATED_WHEN_ORDEN'
                                    ) 
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('======================= fun_cargar_df_override_actual =============================='))
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('df_override_actual_tmp  %s\n => %s\n') % (df_override_actual_tmp.printSchema(),str(df_override_actual_tmp.count())))
    
    df_override_actual = df_override_actual_tmp.cache()
    if val_cp_genera_override_actual:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_override_actual), df_override_actual)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_override_actual))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_override_actual)
    return df_override_actual, "Transformacion => fun_cargar_df_override_actual => df_override_actual" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.OTC_T_OVERWRITE_PLANES_ANT 
def fun_cargar_df_override_planes_ant(sqlContext, val_fecha_dia_bigint, val_ini_mes_anterior_date, val_fin_mes_anterior_date):
    df_override_planes_ant_tmp = fun_otc_t_overwrite_planes(sqlContext, val_cp_base_pro_transfer_consultas, val_cp_otc_t_overwrite_planes)
        
    df_override_planes_ant_tmp = df_override_planes_ant_tmp.filter( (col('p_fecha_proceso') == val_fecha_dia_bigint) & (col('CREATED_WHEN_ORDEN').cast(DateType()).between(str(val_ini_mes_anterior_date),str(val_fin_mes_anterior_date))) )

    windowSpec  = Window.partitionBy('PHONE_NUMBER').orderBy(desc('CREATED_WHEN_ORDEN'))
    df_override_planes_ant_tmp = df_override_planes_ant_tmp.withColumn('ROWNUM',row_number().over(windowSpec))
    df_override_planes_ant_tmp = df_override_planes_ant_tmp.filter(col('ROWNUM') == 1)
    
    df_override_planes_ant_tmp = df_override_planes_ant_tmp.selectExpr('ORDEN_NAME','NUMERO_ORDEN','ESTADO_ORDEN','PHONE_NUMBER AS TELEFONO','TIPO_PROCESO_ESP'
                                        , 'MRC_BASE_PRICE', 'MRC_OV_CREATED_BY AS USU_APLICA_OV_PLAN_ANT', 'MRC_OV_PRICE AS TARIFA_OV_PLAN_ANT'
                                        , 'NRC_OV_CREATED_WHEN', 'CAST(CREATED_WHEN_ORDEN AS DATE) AS FECHA_APLICA_OV_PLAN_ANT'
                                        , 'SUBMITTED_WHEN_ORDEN', 'CREATED_WHEN_ORDEN' #,'PHONE_NUMBER'
                                    )
    df_override_planes_ant_tmp = df_override_planes_ant_tmp.cache()
    
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('======================= fun_cargar_df_override_planes_ant =========================='))
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('df_override_planes_ant_tmp  %s\n => %s\n') % (df_override_planes_ant_tmp.printSchema(),str(df_override_planes_ant_tmp.count())))
    
    df_otc_t_override_planes_ant = df_override_planes_ant_tmp.cache()
    if val_cp_genera_override_planes_ant:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_override_planes_ant), df_otc_t_override_planes_ant)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_override_planes_ant))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_otc_t_override_planes_ant)
    return df_otc_t_override_planes_ant, "Transformacion => fun_cargar_df_override_planes_ant => df_otc_t_override_planes_ant" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.OVERRIDE_ANTERIOR 
def fun_cargar_df_override_anterior(sqlContext, df_tmp_cambio_plan_fnl, df_parque_anterior, df_otc_t_override_planes_ant):
    
    ta = df_tmp_cambio_plan_fnl.alias('ta')
    tb = df_parque_anterior.alias('tb')
    tc = df_otc_t_override_planes_ant.alias('tc')
    
    tab = ta.join(tb, expr(" ta.TELEFONO=tb.TELEFONO "), how='left') \
            .selectExpr('ta.TELEFONO', 'ta.CODIGO_PLAN_ANTERIOR', 'ta.FECHA_CAMBIO_PLAN', 'tb.FECHA_ALTA AS FECHA_INICIO_PLAN_ANTERIOR', 'tb.FECHA_ALTA AS FECHA_ALTA_TMP')
    
    tab = tab.alias('tab')
    tabc = tab.join(tc, expr(" tab.TELEFONO = tc.TELEFONO "), how='left') \
            .selectExpr('tab.TELEFONO', 'tab.CODIGO_PLAN_ANTERIOR', 'tab.FECHA_CAMBIO_PLAN', 'tab.FECHA_INICIO_PLAN_ANTERIOR', 'tab.FECHA_ALTA_TMP'
                        , 'tc.ORDEN_NAME', 'tc.NUMERO_ORDEN', 'tc.ESTADO_ORDEN', 'tc.TIPO_PROCESO_ESP', 'tc.MRC_BASE_PRICE', 'tc.USU_APLICA_OV_PLAN_ANT'
                        , 'tc.TARIFA_OV_PLAN_ANT', 'tc.NRC_OV_CREATED_WHEN', 'tc.CREATED_WHEN_ORDEN', 'tc.FECHA_APLICA_OV_PLAN_ANT', 'tc.SUBMITTED_WHEN_ORDEN'
            )
    
    df_override_anterior_tmp = tabc.filter( ( col('CREATED_WHEN_ORDEN').cast(DateType()) >= col('FECHA_ALTA_TMP').cast(DateType()) ) & ( col('CREATED_WHEN_ORDEN').cast(DateType()) < col('FECHA_CAMBIO_PLAN').cast(DateType()) ) & (col('TARIFA_OV_PLAN_ANT').isNotNull()) )       
    
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('========================= fun_cargar_df_override_anterior =========================='))
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('df_override_anterior_tmp  %s\n => %s\n') % (df_override_anterior_tmp.printSchema(),str(df_override_anterior_tmp.count())))
    
    df_override_anterior = df_override_anterior_tmp.cache()
    if val_cp_genera_override_anterior:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_override_anterior), df_override_anterior)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_override_anterior))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_override_anterior)
    return df_override_anterior, "Transformacion => fun_cargar_df_override_anterior => df_override_anterior" + str_datos_df
    

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.OTC_T_DESCUENTOS_PLANES_ACT 
def fun_cargar_df_otc_t_descuentos_planes_act(sqlContext, val_fecha_dia_bigint, val_ini_mes_actual_ini, val_fecha_proc):
    
    df_otc_t_descuentos_planes_act_tmp = fun_otc_t_descuentos_planes(sqlContext, val_cp_base_pro_transfer_consultas, val_cp_otc_t_descuentos_planes)

    df_otc_t_descuentos_planes_act_tmp = df_otc_t_descuentos_planes_act_tmp.filter( (col('p_fecha_proceso') == val_fecha_dia_bigint) & (col('CREATED_WHEN_ORDEN').cast(DateType()).between(str(val_ini_mes_actual_ini),str(val_fecha_proc))) )
    
    windowSpec  = Window.partitionBy('PHONE_NUMBER').orderBy(desc('CREATED_WHEN_ORDEN'))
    df_otc_t_descuentos_planes_act_tmp = df_otc_t_descuentos_planes_act_tmp.withColumn('ROWNUM',row_number().over(windowSpec))
    df_otc_t_descuentos_planes_act_tmp = df_otc_t_descuentos_planes_act_tmp.filter(col('ROWNUM') == 1)
    
    df_otc_t_descuentos_planes_act_tmp = df_otc_t_descuentos_planes_act_tmp.selectExpr('PHONE_NUMBER as TELEFONO','DISCOUNT_VALUE as DESCUENTO_TARIFA_PLAN_ACT','DISCOUNT_VALUE_WITH_TAX as DESCUENTO_TOTAL'
                                                                , 'DESCRIPCION_DESCUENTO as  DETALLE_DESCUENTO', 'DESC_ACT_DESDE as FECHA_INICIO', 'DESC_ACT_HASTA as FECHA_FIN'
                                                                , 'USUARIO_CREACION_ODV as USUARIO_DESCUENTO', 'cast(CREATED_WHEN_ORDEN as date) CREATED_WHEN_ORDEN', 'MRC_DISCOUNT'
                                                                #, 'PHONE_NUMBER'
                                                )
    
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('=================== fun_cargar_df_otc_t_descuentos_planes_act ======================'))
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('df_otc_t_descuentos_planes_act_tmp  %s\n => %s\n') % (df_otc_t_descuentos_planes_act_tmp.printSchema(),str(df_otc_t_descuentos_planes_act_tmp.count())))
    
    df_otc_t_descuentos_planes_act = df_otc_t_descuentos_planes_act_tmp.cache()
    if val_cp_genera_otc_t_descuentos_planes_act:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_otc_t_descuentos_planes_act), df_otc_t_descuentos_planes_act)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_otc_t_descuentos_planes_act))
    
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_otc_t_descuentos_planes_act)
    return df_otc_t_descuentos_planes_act, "Transformacion => fun_cargar_df_otc_t_descuentos_planes_act => df_otc_t_descuentos_planes_act" + str_datos_df
    

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.DESCUENTO_ACTUAL 
def fun_cargar_df_descuento_actual(sqlContext, df_tmp_cambio_plan_fnl, df_otc_t_descuentos_planes_act):
    ta_1 = df_tmp_cambio_plan_fnl.alias('ta')
    tb_1 = df_otc_t_descuentos_planes_act.alias('tb')
    tab_1 = ta_1.join(tb_1, expr(" (ta.TELEFONO = tb.TELEFONO) AND (tb.USUARIO_DESCUENTO is not null) "), how='left') \
            .selectExpr( 'ta.TELEFONO', 'tb.DESCUENTO_TARIFA_PLAN_ACT', 'tb.DESCUENTO_TOTAL', 'tb.DETALLE_DESCUENTO', 'tb.FECHA_INICIO'
                    , 'tb.FECHA_FIN', 'tb.USUARIO_DESCUENTO', 'tb.CREATED_WHEN_ORDEN', 'tb.MRC_DISCOUNT'
                ) 
    tab_1 = tab_1.filter( (col('MRC_DISCOUNT') == col('DESCUENTO_TARIFA_PLAN_ACT')) & (col('MRC_DISCOUNT').isNotNull()) )
    
    ta_2 = df_tmp_cambio_plan_fnl.alias('taa')
    tb_2 = df_otc_t_descuentos_planes_act.alias('tbb')
    tab_2 = ta_2.join(tb_2, expr(" (taa.TELEFONO = tbb.TELEFONO) AND (tbb.USUARIO_DESCUENTO is not null) "), how='left') \
            .selectExpr( 'taa.TELEFONO', 'tbb.DESCUENTO_TARIFA_PLAN_ACT', 'tbb.DESCUENTO_TOTAL', 'tbb.DETALLE_DESCUENTO', 'tbb.FECHA_INICIO'
                        , 'tbb.FECHA_FIN', 'tbb.USUARIO_DESCUENTO', 'tbb.CREATED_WHEN_ORDEN'
                )
    tab_2 = tab_2.filter( (col('DETALLE_DESCUENTO').like("%CONADIS%")) )
    
    tab_1 = tab_1.drop('MRC_DISCOUNT')
    df_descuento_actual_tmp = tab_1.union(tab_2)
    
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('=========================== fun_cargar_df_descuento_actual ========================='))
    # print(msg_succ('===================================================================================='))
    # print(msg_succ('df_descuento_actual_tmp  %s\n => %s\n') % (df_descuento_actual_tmp.printSchema(),str(df_descuento_actual_tmp.count())))
    
    df_descuento_actual = df_descuento_actual_tmp.cache()
    if val_cp_genera_descuento_actual:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_descuento_actual), df_descuento_actual)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_descuento_actual))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_descuento_actual)
    return df_descuento_actual, "Transformacion => fun_cargar_df_descuento_actual => df_descuento_actual" + str_datos_df
    

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.TMP_OV_DESC_PLAN_NEW 
def fun_cargar_df_tmp_ov_desc_plan_new(sqlContext, df_tmp_cambio_plan_fnl, val_ini_mes_anterior_date, val_fin_mes_anterior_date):
    df_tmp_ov_desc_plan_new_tmp = fun_otc_t_descuentos_planes(sqlContext, val_cp_base_pro_transfer_consultas, val_cp_otc_t_descuentos_planes)
    
    t1 = df_tmp_ov_desc_plan_new_tmp.alias('t1')
    t2 = df_tmp_cambio_plan_fnl.alias('t2')
    
    df_tmp_ov_desc_plan_new_tmp = t1.join(t2, expr(" t1.PHONE_NUMBER = t2.TELEFONO "), how='inner') \
                                .selectExpr( 'OBJECT_ID', 'ORDEN_NAME', 'NUMERO_ORDEN', 'ESTADO_ORDEN', 'USUARIO_CREACION_ODV', 'NOMBRE_USUARIO_CREACION_ODV'
                                            , 'CANAL_USUARIO_CREACION_ODV', 'OFICINA_USUARIO_CREACION_ODV', 'OBJECT_ID_OI', 'ACCION_OI', 'PHONE_NUMBER', 'BILLING_ACCOUNT'
                                            , 'BILLING_ACCT_NUMBER', 'TARIFF_PLAN_ID', 'TARIFF_PLAN_NAME', 'TARIFF_MRC', 'TOTAL_MRC', 'ORDER_ITEM', 'OFERTA'
                                            , 'CODIGO_SERVICIO', 'TIPO_PROCESO_ESP', 'DISCOUNT_VALUE', 'DISCOUNT_VALUE_WITH_TAX', 'TAX_MRC', 'MRC_DISCOUNT'
                                            , 'DESCRIPCION_DESCUENTO', 'TAX_NRC', 'NRC_DISCOUNT', 'DESC_ACT_DESDE', 'DESC_ACT_HASTA', 'CREATED_WHEN_ORDEN'
                                            , 'SUBMITTED_WHEN_ORDEN'
                                    )
    df_tmp_ov_desc_plan_new_tmp = df_tmp_ov_desc_plan_new_tmp.filter( (col('USUARIO_CREACION_ODV').isNotNull()) & (col('CREATED_WHEN_ORDEN').cast(DateType()).between(str(val_ini_mes_anterior_date),str(val_fin_mes_anterior_date))) )
    
    # print(msg_succ('========================================================================================'))
    # print(msg_succ('=========================== fun_cargar_df_tmp_ov_desc_plan_new ========================='))
    # print(msg_succ('========================================================================================'))
    # print(msg_succ('df_tmp_ov_desc_plan_new_tmp  %s\n => %s\n') % (df_tmp_ov_desc_plan_new_tmp.printSchema(),str(df_tmp_ov_desc_plan_new_tmp.count())))
    
    df_tmp_ov_desc_plan_new = df_tmp_ov_desc_plan_new_tmp.cache()
    if val_cp_genera_tmp_ov_desc_plan_new:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_tmp_ov_desc_plan_new), df_tmp_ov_desc_plan_new)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_tmp_ov_desc_plan_new))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_tmp_ov_desc_plan_new)
    return df_tmp_ov_desc_plan_new, "Transformacion => fun_cargar_df_tmp_ov_desc_plan_new => df_tmp_ov_desc_plan_new" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.DESCUENTO_ANTERIOR 
def fun_cargar_df_descuento_anterior(sqlContext, df_tmp_cambio_plan_fnl, df_parque_anterior, df_tmp_ov_desc_plan_new):
    # print(msg_succ('=========================== df_tmp_cambio_plan_fnl ==========================='))
    ta = df_tmp_cambio_plan_fnl.alias('ta')
    # print(msg_succ('df_tmp_cambio_plan_fnl  %s\n') % (ta.printSchema()))
    
    # print(msg_succ('=========================== df_parque_anterior ================================'))
    taa = df_parque_anterior.alias('taa')
    # print(msg_succ('df_parque_anterior  %s\n') % (df_parque_anterior.printSchema()))
    
    # print(msg_succ('=========================== df_tmp_ov_desc_plan_new ==========================='))
    tb = df_tmp_ov_desc_plan_new.alias('tb')
    # print(msg_succ('df_tmp_ov_desc_plan_new  %s\n') % (df_tmp_ov_desc_plan_new.printSchema()))
    
    ta_aa = ta.join(taa, expr(" ta.TELEFONO = taa.TELEFONO "), how='left').selectExpr('ta.TELEFONO AS TELEFONO', 'ta.FECHA_CAMBIO_PLAN', 'CAST(taa.FECHA_ALTA AS DATE) AS FECHA_INICIO_PLAN_ANTERIOR')
    ta_aa = ta_aa.alias('ta_aa')
    ta_aa_b = ta_aa.join(tb, expr(" ta_aa.TELEFONO = tb.PHONE_NUMBER ") ,how='left') \
            .selectExpr('ta_aa.TELEFONO', 'tb.MRC_DISCOUNT AS VALOR_DESCUENTO', 'tb.USUARIO_CREACION_ODV AS CREADOR_DESCUENTO', 'tb.DESCRIPCION_DESCUENTO as DETALLE_DESCUENTO'
                        , 'CAST(tb.SUBMITTED_WHEN_ORDEN AS DATE) AS FECHA_LINEA_ORDEN', 'tb.NUMERO_ORDEN', 'CAST(tb.DESC_ACT_DESDE AS DATE) as FECHA_INICIO'
                        , 'CAST(tb.DESC_ACT_HASTA AS DATE) as FECHA_FIN', 'ta_aa.FECHA_CAMBIO_PLAN', 'ta_aa.FECHA_INICIO_PLAN_ANTERIOR'
                        , 'CAST(tb.CREATED_WHEN_ORDEN AS DATE) AS FECHA', 'tb.DISCOUNT_VALUE AS F_DISCOUNT_VALUE', 'tb.MRC_DISCOUNT AS F_MRC_DISCOUNT'
                        , 'tb.DESCRIPCION_DESCUENTO AS F_DESCRIPCION_DESCUENTO'
                    )        
    # print(msg_succ('df_descuento_anterior_tmp(0)  %s\n') % (str(ta_aa_b.count())))
    
    ta_aa_b_1 = ta_aa_b.filter((col('F_MRC_DISCOUNT') == col('F_DISCOUNT_VALUE')))
    # print(msg_succ('df_descuento_anterior_tmp_1(1)  %s\n') % (str(ta_aa_b_1.count())))
    
    ta_aa_b_1 = ta_aa_b_1.distinct()
    # print(msg_succ('df_descuento_anterior_tmp_1(2)  %s\n') % (str(ta_aa_b_1.count())))

    ta_aa_b_2 = ta_aa_b.filter((col('F_DESCRIPCION_DESCUENTO').like("%CONADIS%")))
    # print(msg_succ('df_descuento_anterior_tmp_2(1)  %s\n') % (str(ta_aa_b_2.count())))
    
    ta_aa_b_2 = ta_aa_b_2.distinct()
    # print(msg_succ('df_descuento_anterior_tmp_2(2)  %s\n') % (str(ta_aa_b_2.count())))
    
    df_descuento_anterior_tmp = ta_aa_b_1.union(ta_aa_b_2)
    df_descuento_anterior_tmp = df_descuento_anterior_tmp.filter( ( (col('FECHA').cast(DateType())) >= (col('FECHA_INICIO_PLAN_ANTERIOR').cast(DateType())) ) 
                                        & ( (col('FECHA').cast(DateType())) < (col('FECHA_CAMBIO_PLAN').cast(DateType())) )
                                        & ( (col('FECHA_FIN').cast(DateType())) >= (col('FECHA_CAMBIO_PLAN').cast(DateType())) )
                                        & ( (col('FECHA_INICIO').cast(DateType())) < (col('FECHA_CAMBIO_PLAN').cast(DateType())) )
                                        & ( (col('VALOR_DESCUENTO').isNotNull()) )
                                    )
    df_descuento_anterior_tmp = df_descuento_anterior_tmp.selectExpr('TELEFONO', 'VALOR_DESCUENTO', 'CREADOR_DESCUENTO', 'DETALLE_DESCUENTO'
                                                            , 'FECHA_CAMBIO_PLAN', 'FECHA_LINEA_ORDEN', 'NUMERO_ORDEN', 'FECHA_INICIO', 'FECHA_FIN'
                                                            , 'FECHA_INICIO_PLAN_ANTERIOR', 'FECHA'
                                                        )
    df_descuento_anterior_tmp = df_descuento_anterior_tmp.distinct()
    
    # print(msg_succ('========================================================================================'))
    # print(msg_succ('=========================== fun_cargar_df_descuento_anterior ==========================='))
    # print(msg_succ('========================================================================================'))
    # print(msg_succ('df_descuento_anterior_tmp  %s\n => %s\n') % (df_descuento_anterior_tmp.printSchema(),str(df_descuento_anterior_tmp.count())))
    
    df_descuento_anterior = df_descuento_anterior_tmp.cache()
    if val_cp_genera_descuento_anterior:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_descuento_anterior), df_descuento_anterior)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_descuento_anterior))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_descuento_anterior)
    return df_descuento_anterior, "Transformacion => fun_cargar_df_descuento_anterior => df_descuento_anterior" + str_datos_df


@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_cs_altas.CAMBIO_PLAN_FNL_INT 
def fun_cargar_df_cambio_plan_fnl_int(sqlContext, df_tmp_cambio_plan_fnl, df_parque_anterior, df_override_actual, df_override_anterior, df_descuento_actual):
    ta = df_tmp_cambio_plan_fnl.alias('ta')
    # print(msg_succ('df_tmp_cambio_plan_fnl  %s\n') % (df_tmp_cambio_plan_fnl.printSchema()))
    
    tb = df_parque_anterior.alias('tb')
    # print(msg_succ('df_parque_anterior  %s\n') % (df_parque_anterior.printSchema()))
    
    toact = df_override_actual.alias('toact')
    # print(msg_succ('df_override_actual  %s\n') % (df_override_actual.printSchema()))
    
    toant = df_override_anterior.alias('toant')
    # print(msg_succ('df_override_anterior  %s\n') % (df_override_anterior.printSchema()))
    
    tdact = df_descuento_actual.alias('tdact')
    tdant = df_descuento_actual.alias('tdant')
    # print(msg_succ('df_descuento_actual  %s\n') % (df_descuento_actual.printSchema()))
    
    ta_b = ta.join(tb, expr(" ta.TELEFONO = tb.TELEFONO ") , how='left') \
            .selectExpr('ta.ANIO_MES', 'ta.FECHA_CARGA', 'ta.TELEFONO', 'ta.NUMERO_ABONADO', 'ta.SEGMENTO', 'ta.SUB_SEGMENTO', 'ta.ESTADO_ABONADO', 'ta.DOCUMENTO_CLIENTE'
                    , 'ta.PROVINCIA', 'ta.FORMA_PAGO', 'ta.CODIGO_PLAN_ACTUAL', 'ta.DESCRIPCION_PLAN_ACTUAL', 'ta.CATEGORIA_PLAN_ACTUAL', 'ta.TARIFA_BASICA_ACTUAL'
                    , 'ta.COMERCIAL_ACTUAL', 'ta.CODIGO_PLAN_ANTERIOR', 'ta.DESCRIPCION_PLAN_ANTERIOR', 'ta.CATEGORIA_PLAN_ANTERIOR', 'ta.TARIFA_BASICA_ANTERIOR'
                    , 'ta.COMERCIAL_ANTERIOR', 'ta.FECHA_CAMBIO_PLAN', 'ta.CODIGO_USUARIO_ORDEN', 'ta.NOMBRE_USUARIO_ORDEN', 'ta.CODIGO_USUARIO_SUBMIT', 'ta.NOMBRE_USUARIO_SUBMIT'
                    , 'ta.CANAL', 'ta.OFICINA', 'ta.CAMPANIA', 'ta.NOM_DISTRIBUIDOR', 'ta.CIUDAD_DISTRIBUIDOR', 'ta.PROVINCIA_DISTRIBUIDOR', 'ta.REGION_DISTRIBUIDOR'
                    , 'ta.DELTA', 'ta.TIPO_MOVIMIENTO', 'ta.CP_CRUCE_PARQUE', 'ta.CP_NC', 'ta.SUB_CANAL', 'ta.CODIGO_USUARIO', 'ta.MARCA'
                    , 'CAST(tb.FECHA_ALTA AS DATE) AS FECHA_INICIO_PLAN_ANTERIOR'
            )
    tab = ta_b.alias('tab')
    
    tab_oact = tab.join(toact, expr(" tab.TELEFONO = toact.TELEFONO "), how='left') \
                .selectExpr('tab.ANIO_MES', 'tab.FECHA_CARGA', 'tab.TELEFONO', 'tab.NUMERO_ABONADO', 'tab.SEGMENTO', 'tab.SUB_SEGMENTO', 'tab.ESTADO_ABONADO', 'tab.DOCUMENTO_CLIENTE'
                    , 'tab.PROVINCIA', 'tab.FORMA_PAGO', 'tab.CODIGO_PLAN_ACTUAL', 'tab.DESCRIPCION_PLAN_ACTUAL', 'tab.CATEGORIA_PLAN_ACTUAL', 'tab.TARIFA_BASICA_ACTUAL'
                    , 'tab.COMERCIAL_ACTUAL', 'tab.CODIGO_PLAN_ANTERIOR', 'tab.DESCRIPCION_PLAN_ANTERIOR', 'tab.CATEGORIA_PLAN_ANTERIOR', 'tab.TARIFA_BASICA_ANTERIOR'
                    , 'tab.COMERCIAL_ANTERIOR', 'tab.FECHA_CAMBIO_PLAN', 'tab.CODIGO_USUARIO_ORDEN', 'tab.NOMBRE_USUARIO_ORDEN', 'tab.CODIGO_USUARIO_SUBMIT', 'tab.NOMBRE_USUARIO_SUBMIT'
                    , 'tab.CANAL', 'tab.OFICINA', 'tab.CAMPANIA', 'tab.NOM_DISTRIBUIDOR', 'tab.CIUDAD_DISTRIBUIDOR', 'tab.PROVINCIA_DISTRIBUIDOR', 'tab.REGION_DISTRIBUIDOR'
                    , 'tab.DELTA', 'tab.TIPO_MOVIMIENTO', 'tab.CP_CRUCE_PARQUE', 'tab.CP_NC', 'tab.SUB_CANAL', 'tab.CODIGO_USUARIO', 'tab.MARCA'
                    , 'tab.FECHA_INICIO_PLAN_ANTERIOR'
                    , 'toact.TARIFA_OV_PLAN_ACT AS TARIFA_PLAN_ACTUAL_OV', 'toact.USU_APLICA_OV_PLAN_ACT AS USU_APLICA_OV_PLAN_ACT'
                    , 'toact.FECHA_APLICA_OV_PLAN_ACT AS FECHA_APLICA_OV_PLAN_ACT'
                )
    taboact = tab_oact.alias('taboact')
    
    taboact_oant = taboact.join(toant, expr(" taboact.TELEFONO = toant.TELEFONO "), how='left') \
                    .selectExpr('taboact.ANIO_MES', 'taboact.FECHA_CARGA', 'taboact.TELEFONO', 'taboact.NUMERO_ABONADO', 'taboact.SEGMENTO', 'taboact.SUB_SEGMENTO', 'taboact.ESTADO_ABONADO', 'taboact.DOCUMENTO_CLIENTE'
                        , 'taboact.PROVINCIA', 'taboact.FORMA_PAGO', 'taboact.CODIGO_PLAN_ACTUAL', 'taboact.DESCRIPCION_PLAN_ACTUAL', 'taboact.CATEGORIA_PLAN_ACTUAL', 'taboact.TARIFA_BASICA_ACTUAL'
                        , 'taboact.COMERCIAL_ACTUAL', 'taboact.CODIGO_PLAN_ANTERIOR', 'taboact.DESCRIPCION_PLAN_ANTERIOR', 'taboact.CATEGORIA_PLAN_ANTERIOR', 'taboact.TARIFA_BASICA_ANTERIOR'
                        , 'taboact.COMERCIAL_ANTERIOR', 'taboact.FECHA_CAMBIO_PLAN', 'taboact.CODIGO_USUARIO_ORDEN', 'taboact.NOMBRE_USUARIO_ORDEN', 'taboact.CODIGO_USUARIO_SUBMIT', 'taboact.NOMBRE_USUARIO_SUBMIT'
                        , 'taboact.CANAL', 'taboact.OFICINA', 'taboact.CAMPANIA', 'taboact.NOM_DISTRIBUIDOR', 'taboact.CIUDAD_DISTRIBUIDOR', 'taboact.PROVINCIA_DISTRIBUIDOR', 'taboact.REGION_DISTRIBUIDOR'
                        , 'taboact.DELTA', 'taboact.TIPO_MOVIMIENTO', 'taboact.CP_CRUCE_PARQUE', 'taboact.CP_NC', 'taboact.SUB_CANAL', 'taboact.CODIGO_USUARIO', 'taboact.MARCA'
                        , 'taboact.FECHA_INICIO_PLAN_ANTERIOR'
                        , 'taboact.TARIFA_PLAN_ACTUAL_OV', 'taboact.USU_APLICA_OV_PLAN_ACT'
                        , 'taboact.FECHA_APLICA_OV_PLAN_ACT'
                        , 'toant.TARIFA_OV_PLAN_ANT AS TARIFA_OV_PLAN_ANT', 'toant.USU_APLICA_OV_PLAN_ANT AS USU_APLICA_OV_PLAN_ANT'
                        , 'toant.FECHA_APLICA_OV_PLAN_ANT AS FECHA_APLICA_OV_PLAN_ANT'
                    )            
    tabo_act_oant = taboact_oant.alias('tabo_act_oant')

    tabo_act_oant_dact = tabo_act_oant.join(tdact, expr(" tabo_act_oant.TELEFONO = tdact.TELEFONO "), how='left') \
                    .selectExpr('tabo_act_oant.ANIO_MES', 'tabo_act_oant.FECHA_CARGA', 'tabo_act_oant.TELEFONO', 'tabo_act_oant.NUMERO_ABONADO', 'tabo_act_oant.SEGMENTO', 'tabo_act_oant.SUB_SEGMENTO', 'tabo_act_oant.ESTADO_ABONADO', 'tabo_act_oant.DOCUMENTO_CLIENTE'
                        , 'tabo_act_oant.PROVINCIA', 'tabo_act_oant.FORMA_PAGO', 'tabo_act_oant.CODIGO_PLAN_ACTUAL', 'tabo_act_oant.DESCRIPCION_PLAN_ACTUAL', 'tabo_act_oant.CATEGORIA_PLAN_ACTUAL', 'tabo_act_oant.TARIFA_BASICA_ACTUAL'
                        , 'tabo_act_oant.COMERCIAL_ACTUAL', 'tabo_act_oant.CODIGO_PLAN_ANTERIOR', 'tabo_act_oant.DESCRIPCION_PLAN_ANTERIOR', 'tabo_act_oant.CATEGORIA_PLAN_ANTERIOR', 'tabo_act_oant.TARIFA_BASICA_ANTERIOR'
                        , 'tabo_act_oant.COMERCIAL_ANTERIOR', 'tabo_act_oant.FECHA_CAMBIO_PLAN', 'tabo_act_oant.CODIGO_USUARIO_ORDEN', 'tabo_act_oant.NOMBRE_USUARIO_ORDEN', 'tabo_act_oant.CODIGO_USUARIO_SUBMIT', 'tabo_act_oant.NOMBRE_USUARIO_SUBMIT'
                        , 'tabo_act_oant.CANAL', 'tabo_act_oant.OFICINA', 'tabo_act_oant.CAMPANIA', 'tabo_act_oant.NOM_DISTRIBUIDOR', 'tabo_act_oant.CIUDAD_DISTRIBUIDOR', 'tabo_act_oant.PROVINCIA_DISTRIBUIDOR', 'tabo_act_oant.REGION_DISTRIBUIDOR'
                        , 'tabo_act_oant.DELTA', 'tabo_act_oant.TIPO_MOVIMIENTO', 'tabo_act_oant.CP_CRUCE_PARQUE', 'tabo_act_oant.CP_NC', 'tabo_act_oant.SUB_CANAL', 'tabo_act_oant.CODIGO_USUARIO', 'tabo_act_oant.MARCA'
                        , 'tabo_act_oant.FECHA_INICIO_PLAN_ANTERIOR'
                        , 'tabo_act_oant.TARIFA_PLAN_ACTUAL_OV', 'tabo_act_oant.USU_APLICA_OV_PLAN_ACT'
                        , 'tabo_act_oant.FECHA_APLICA_OV_PLAN_ACT'
                        , 'tabo_act_oant.TARIFA_OV_PLAN_ANT', 'tabo_act_oant.USU_APLICA_OV_PLAN_ANT'
                        , 'tabo_act_oant.FECHA_APLICA_OV_PLAN_ANT'
                        , 'CASE WHEN tdact.DESCUENTO_TOTAL IS NULL THEN 0 ELSE tdact.DESCUENTO_TOTAL END AS DESCUENTO_TARIFA_PLAN_ACT'
                        , 'tdact.DETALLE_DESCUENTO AS DESCRIPCION_DESCUENTO_PLAN_ACT', 'tdact.FECHA_INICIO  AS FECHA_INICIO_DESCUENTO_PLAN_ACT'
                        , 'tdact.FECHA_FIN AS FECHA_FIN_DESCUENTO_PLAN_ACT', 'tdact.USUARIO_DESCUENTO  AS USU_APLICA_DESCUENTO_PLAN_ACT'
                    )
    tabo_act_oant_dact = tabo_act_oant_dact.alias('tabo_act_oant_dact')
    
    tabo_act_oant_dact = tabo_act_oant_dact.join(tdant, expr(" tabo_act_oant_dact.TELEFONO = tdant.TELEFONO "), how='left') \
                    .selectExpr('tabo_act_oant_dact.ANIO_MES', 'tabo_act_oant_dact.FECHA_CARGA', 'tabo_act_oant_dact.TELEFONO', 'tabo_act_oant_dact.NUMERO_ABONADO', 'tabo_act_oant_dact.SEGMENTO'
                                , 'tabo_act_oant_dact.SUB_SEGMENTO', 'tabo_act_oant_dact.ESTADO_ABONADO', 'tabo_act_oant_dact.DOCUMENTO_CLIENTE', 'tabo_act_oant_dact.PROVINCIA', 'tabo_act_oant_dact.FORMA_PAGO'
                                , 'tabo_act_oant_dact.CODIGO_PLAN_ACTUAL', 'tabo_act_oant_dact.DESCRIPCION_PLAN_ACTUAL', 'tabo_act_oant_dact.CATEGORIA_PLAN_ACTUAL', 'tabo_act_oant_dact.TARIFA_BASICA_ACTUAL', 'tabo_act_oant_dact.COMERCIAL_ACTUAL'
                                , 'tabo_act_oant_dact.CODIGO_PLAN_ANTERIOR', 'tabo_act_oant_dact.DESCRIPCION_PLAN_ANTERIOR', 'tabo_act_oant_dact.CATEGORIA_PLAN_ANTERIOR', 'tabo_act_oant_dact.TARIFA_BASICA_ANTERIOR', 'tabo_act_oant_dact.COMERCIAL_ANTERIOR'
                                , 'tabo_act_oant_dact.FECHA_CAMBIO_PLAN', 'tabo_act_oant_dact.CODIGO_USUARIO_ORDEN', 'tabo_act_oant_dact.NOMBRE_USUARIO_ORDEN', 'tabo_act_oant_dact.CODIGO_USUARIO_SUBMIT', 'tabo_act_oant_dact.NOMBRE_USUARIO_SUBMIT'
                                , 'tabo_act_oant_dact.CANAL', 'tabo_act_oant_dact.OFICINA', 'tabo_act_oant_dact.CAMPANIA', 'tabo_act_oant_dact.NOM_DISTRIBUIDOR', 'tabo_act_oant_dact.CIUDAD_DISTRIBUIDOR'
                                , 'tabo_act_oant_dact.PROVINCIA_DISTRIBUIDOR', 'tabo_act_oant_dact.REGION_DISTRIBUIDOR', 'tabo_act_oant_dact.DELTA', 'tabo_act_oant_dact.TIPO_MOVIMIENTO', 'tabo_act_oant_dact.CP_CRUCE_PARQUE'
                                , 'tabo_act_oant_dact.CP_NC', 'tabo_act_oant_dact.SUB_CANAL', 'tabo_act_oant_dact.CODIGO_USUARIO', 'tabo_act_oant_dact.MARCA', 'tabo_act_oant_dact.FECHA_INICIO_PLAN_ANTERIOR'
                                , 'tabo_act_oant_dact.TARIFA_PLAN_ACTUAL_OV', 'tabo_act_oant_dact.USU_APLICA_OV_PLAN_ACT', 'tabo_act_oant_dact.FECHA_APLICA_OV_PLAN_ACT', 'tabo_act_oant_dact.TARIFA_OV_PLAN_ANT', 'tabo_act_oant_dact.USU_APLICA_OV_PLAN_ANT'
                                , 'tabo_act_oant_dact.FECHA_APLICA_OV_PLAN_ANT', 'tabo_act_oant_dact.DESCUENTO_TARIFA_PLAN_ACT', 'tabo_act_oant_dact.DESCRIPCION_DESCUENTO_PLAN_ACT', 'tabo_act_oant_dact.FECHA_INICIO_DESCUENTO_PLAN_ACT', 'tabo_act_oant_dact.FECHA_FIN_DESCUENTO_PLAN_ACT'
                                , 'tabo_act_oant_dact.USU_APLICA_DESCUENTO_PLAN_ACT'
                                , 'CASE WHEN tdant.DESCUENTO_TOTAL IS NULL THEN 0 ELSE tdant.DESCUENTO_TOTAL END AS DESCUENTO_TARIFA_PLAN_ANT'
                                , 'tdant.DETALLE_DESCUENTO', 'CAST(tdant.FECHA_INICIO AS DATE) AS FECHA_INICIO_DESCUENTO_PLAN_ANT'
                                , 'CAST(tdant.FECHA_FIN AS DATE) AS FECHA_FIN_DESCUENTO_PLAN_ANT', 'tdant.USUARIO_DESCUENTO'
                            )

    df_cambio_plan_fnl_int_tmp = tabo_act_oant_dact.distinct()
    # print(msg_succ('========================================================================================'))
    # print(msg_succ('=========================== fun_cargar_df_cambio_plan_fnl_int =========================='))
    # print(msg_succ('========================================================================================'))
    # print(msg_succ('df_cambio_plan_fnl_int_tmp  %s\n => %s\n') % (df_cambio_plan_fnl_int_tmp.printSchema(),str(df_cambio_plan_fnl_int_tmp.count())))
    
    df_cambio_plan_fnl_int = df_cambio_plan_fnl_int_tmp.cache()
    if val_cp_genera_cambio_plan_fnl_int:
        fun_realizar_insercion_df_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_cambio_plan_fnl_int), df_cambio_plan_fnl_int)
    else:
        fun_eliminar_tabla(sqlContext, val_cp_base_temporales, (val_cp_prefijo_tabla + val_cp_cambio_plan_fnl_int))
        
    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_cambio_plan_fnl_int)
    return df_cambio_plan_fnl_int, "Transformacion => fun_cargar_df_cambio_plan_fnl_int => df_cambio_plan_fnl_int" + str_datos_df
    

@seguimiento_transformacion
#  INSERTAMOS :: Cargamos los Datos en la tabla de HIVE => db_cs_altas.OTC_T_CAMBIO_PLAN_BI
def fun_cargar_df_otc_t_cambio_plan_bi(sqlContext, df_cambio_plan_fnl_int, val_fecha_dia_bigint):

    df_otc_t_cambio_plan_bi_tmp = df_cambio_plan_fnl_int
    # print(msg_succ('======== Fechas ::: df_otc_t_cambio_plan_bi_tmp ==========='))
    # print(msg_succ('\n %s\n') % (str(val_fecha_dia_bigint)))
    # print(msg_succ('df_cambio_plan_fnl_int  %s\n') % (df_cambio_plan_fnl_int.printSchema()))
    
    # print(msg_succ('======== df_otc_t_cambio_plan_bi_tmp (0) ==========='))
    # print( msg_succ('\n%s\n => %s\n => %s\n') % ( str(df_otc_t_cambio_plan_bi_tmp.count()), df_otc_t_cambio_plan_bi_tmp.printSchema(), df_otc_t_cambio_plan_bi_tmp.show(3) ) )
    
    df_tt = df_otc_t_cambio_plan_bi_tmp.alias('tt')
    df_otc_t_cambio_plan_bi_tmp = df_tt.selectExpr( 'ANIO_MES', 'FECHA_CARGA', 'TELEFONO', 'NUMERO_ABONADO', 'SEGMENTO', 'SUB_SEGMENTO', 'ESTADO_ABONADO'
                                                , 'DOCUMENTO_CLIENTE', 'PROVINCIA', 'FORMA_PAGO', 'CODIGO_PLAN_ACTUAL', 'DESCRIPCION_PLAN_ACTUAL', 'CATEGORIA_PLAN_ACTUAL'
                                                , 'TARIFA_BASICA_ACTUAL', 'COMERCIAL_ACTUAL', 'CODIGO_PLAN_ANTERIOR', 'DESCRIPCION_PLAN_ANTERIOR', 'CATEGORIA_PLAN_ANTERIOR'
                                                , 'TARIFA_BASICA_ANTERIOR', 'COMERCIAL_ANTERIOR', 'FECHA_CAMBIO_PLAN', 'CODIGO_USUARIO_ORDEN', 'NOMBRE_USUARIO_ORDEN'
                                                , 'CODIGO_USUARIO_SUBMIT', 'NOMBRE_USUARIO_SUBMIT', 'CANAL', 'OFICINA', 'CAMPANIA', 'NOM_DISTRIBUIDOR', 'CIUDAD_DISTRIBUIDOR'
                                                , 'PROVINCIA_DISTRIBUIDOR', 'REGION_DISTRIBUIDOR', 'DELTA', 'TIPO_MOVIMIENTO', 'CP_CRUCE_PARQUE', 'CP_NC', 'SUB_CANAL'
                                                , 'CODIGO_USUARIO', 'MARCA', 'TARIFA_PLAN_ACTUAL_OV', 'USU_APLICA_OV_PLAN_ACT', 'FECHA_APLICA_OV_PLAN_ACT', 'DESCUENTO_TARIFA_PLAN_ACT'
                                                , 'DESCRIPCION_DESCUENTO_PLAN_ACT', 'FECHA_INICIO_DESCUENTO_PLAN_ACT', 'FECHA_FIN_DESCUENTO_PLAN_ACT', 'USU_APLICA_DESCUENTO_PLAN_ACT'
                                                , 'CASE WHEN USU_APLICA_OV_PLAN_ACT IS NULL THEN (TARIFA_BASICA_ACTUAL-DESCUENTO_TARIFA_PLAN_ACT) ELSE (TARIFA_PLAN_ACTUAL_OV-DESCUENTO_TARIFA_PLAN_ACT) END AS TARIFA_FINAL_PLAN_ACT'
                                                , 'CASE WHEN FECHA_INICIO_PLAN_ANTERIOR IS NULL THEN FECHA_APLICA_OV_PLAN_ANT ELSE FECHA_INICIO_PLAN_ANTERIOR END AS FECHA_INICIO_PLAN_ANTERIOR'
                                                , 'TARIFA_OV_PLAN_ANT', 'USU_APLICA_OV_PLAN_ANT', 'FECHA_APLICA_OV_PLAN_ANT', 'DESCUENTO_TARIFA_PLAN_ANT', 'DETALLE_DESCUENTO'
                                                , 'FECHA_INICIO_DESCUENTO_PLAN_ANT', 'FECHA_FIN_DESCUENTO_PLAN_ANT', 'USUARIO_DESCUENTO'
                                                , 'CASE WHEN TARIFA_OV_PLAN_ANT IS NULL THEN (TARIFA_BASICA_ANTERIOR-DESCUENTO_TARIFA_PLAN_ANT) ELSE (TARIFA_OV_PLAN_ANT-DESCUENTO_TARIFA_PLAN_ANT) END AS TARIFA_FINAL_PLAN_ANT'
                                            )
    
    df_otc_t_cambio_plan_bi_tmp = df_otc_t_cambio_plan_bi_tmp.withColumn('P_FECHA_PROCESO', lit(val_fecha_dia_bigint))
    df_otc_t_cambio_plan_bi_tmp = df_otc_t_cambio_plan_bi_tmp.withColumn('P_FECHA_PROCESO', df_otc_t_cambio_plan_bi_tmp['P_FECHA_PROCESO'].cast(IntegerType()))
    
    # print(msg_succ('======== df_transfer_in_para_insertar_tmp (1) ==========='))
    # print( msg_succ('\n%s\n => %s\n => %s\n') % ( str(df_transfer_in_para_insertar_tmp.count()), df_transfer_in_para_insertar_tmp.printSchema(), df_transfer_in_para_insertar_tmp.show(3) ) )
    
    
    df_otc_t_cambio_plan_bi = df_otc_t_cambio_plan_bi_tmp.cache()
    
    # Insercion de Datos
    # val_retorno_insercion = fun_cargar_datos_dinamico(sqlContext, df_otc_t_cambio_plan_bi, val_cp_base_desa_transfer, val_cp_otc_t_cambio_plan_bi, 'P_FECHA_PROCESO', val_fecha_dia_bigint, val_fecha_dia_bigint)
    val_retorno_insercion = fun_cargar_datos_dinamico(sqlContext, df_otc_t_cambio_plan_bi, val_cp_base_pro_transfer_consultas, val_cp_otc_t_cambio_plan_bi, 'P_FECHA_PROCESO', val_fecha_dia_bigint, val_fecha_dia_bigint)
    
    print(msg_succ('======== df_otc_t_cambio_plan_bi  =>  fun_cargar_datos_dinamico ==========='))
    print( msg_succ('%s => \n') % ( val_retorno_insercion ) )

    str_datos_df = fun_obtener_datos_df(val_cp_se_calcula,df_otc_t_cambio_plan_bi)
    return df_otc_t_cambio_plan_bi, "Transformacion => fun_cargar_df_otc_t_cambio_plan_bi => df_otc_t_cambio_plan_bi" + str_datos_df


