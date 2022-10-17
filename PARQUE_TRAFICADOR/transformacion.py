# -*- coding: utf-8 -*-
from Configuraciones.configuracion import *
from Querys.sql import *
from Funciones.funcion import *
import pyspark.sql.functions as sql_fun
from pyspark.sql.functions import col, substring, max, min, when, count, sum, lit, unix_timestamp, desc, asc, length, expr, datediff, row_number, coalesce, split, trim, concat, concat_ws, to_date, last_day, to_timestamp
from pyspark.sql.types import StringType, DateType, IntegerType, StructType, TimestampType
from pyspark.sql.window import Window
import pandas as pd

@seguimiento_transformacion
# Funcion Principal 
def func_proceso_principal(sqlContext, val_fecha_ejecucion, val_base_reportes, val_otc_t_360_ubicacion, val_franja_horaria):
    
    # controlpoint1    
    val_str_controlpoint1, df_mksharevozdatos_90, df_mksharevozdatos_90_max, df_mksharevozdatos_90_final = fun_controlpoint1(sqlContext, val_fecha_ejecucion, val_franja_horaria)

    # controlpoint2
    val_str_controlpoint2 = fun_controlpoint2(sqlContext, df_mksharevozdatos_90_final, val_base_reportes, val_otc_t_360_ubicacion)
    
    # Mensajes
    val_str_resultado = val_str_controlpoint1
    
    val_str_resultado = val_str_resultado + "\n" + val_str_controlpoint2
        
    return val_str_resultado

# FUNCIONES PRINCIPALES DE CONTROL

@seguimiento_transformacion
def fun_controlpoint1(sqlContext, val_fecha_ejecucion,  val_franja_horaria):
    
    print(msg_succ('================================================================'))
    print(msg_succ('======== FUNCION DE CONTROL => fun_controlpoint1 ==============='))
    print(msg_succ('================================================================'))
    
    df_mksharevozdatos_90, val_str_df_mksharevozdatos_90= fun_cargar_df_mksharevozdatos_90(sqlContext, val_franja_horaria)    
    df_mksharevozdatos_90_max, val_str_df_mksharevozdatos_90_max= fun_cargar_df_mksharevozdatos_90_max(sqlContext, val_fecha_ejecucion)
    df_mksharevozdatos_90_final, val_str_df_mksharevozdatos_90_final= fun_cargar_df_mksharevozdatos_90_final(sqlContext, val_fecha_ejecucion)
        
    val_str_resultado_cp1 = val_str_df_mksharevozdatos_90 + "\n" + val_str_df_mksharevozdatos_90_max 
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_df_mksharevozdatos_90_final
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n"
    
    return val_str_resultado_cp1, df_mksharevozdatos_90, df_mksharevozdatos_90_max, df_mksharevozdatos_90_final
    
    
@seguimiento_transformacion
def fun_controlpoint2(sqlContext, df_mksharevozdatos_90_final, val_fecha_ejecucion, val_base_reportes, val_otc_t_360_ubicacion):
    print(msg_succ('================================================================'))
    print(msg_succ('======== FUNCION DE CONTROL => fun_controlpoint2 ==============='))
    print(msg_succ('================================================================'))
    
    val_str_elim_otc_t_360_ubicacion = fun_eliminar_df_otc_t_360_ubicacion(sqlContext, val_fecha_ejecucion)
    val_str_cargar_otc_t_360_ubicacion = fun_cargar_df_otc_t_360_ubicacion(sqlContext, df_mksharevozdatos_90_final, val_base_reportes, val_otc_t_360_ubicacion, val_fecha_ejecucion)
    
    val_str_resultado_cp2 = val_str_elim_otc_t_360_ubicacion + "\n" + val_str_cargar_otc_t_360_ubicacion 
    val_str_resultado_cp2 = val_str_resultado_cp2 + "\n"
    
    return val_str_resultado_cp2

# FUNCIONES QUE GENERAN DATAFRAME

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_altamira.otc_t_ppcs_llamadas
def fun_cargar_df_otc_t_ppcs_llamadas(sqlContext):
    df_otc_t_ppcs_llamadas_tmp = fun_otc_t_ppcs_llamadas(sqlContext, val_base_altamira_consultas, val_otc_t_ppcs_llamadas, val_fecha_ejecucion, val_fecha_ini)
    
    df_otc_t_ppcs_llamadas = df_otc_t_ppcs_llamadas_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_ppcs_llamadas)
    return df_otc_t_ppcs_llamadas, "Transformacion => fun_cargar_df_otc_t_ppcs_llamadas => df_otc_t_ppcs_llamadas" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_reportes.otc_t_dev_cat_plan
def fun_cargar_lista_otc_t_dev_cat_plan(sqlContext, ):
    df_otc_t_dev_cat_plan_tmp = fun_otc_t_dev_cat_plan(sqlContext, val_base_altamira_consultas, val_otc_t_dev_cat_plan, val_fecha_ejecucion, val_fecha_ini)
    
    df_otc_t_dev_cat_plan = df_otc_t_dev_cat_plan_tmp.cache()
    list_dev_cat_plan_codigo = list(df_otc_t_dev_cat_plan.select('codigo').toPandas()['codigo'])
    
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_dev_cat_plan)
    return list_dev_cat_plan_codigo, "Transformacion => fun_cargar_lista_otc_t_dev_cat_plan => list_dev_cat_plan_codigo" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_altamira.otc_t_ppcs_diameter
def fun_cargar_df_otc_t_ppcs_diameter(sqlContext, ):
    df_otc_t_ppcs_diameter_tmp = fun_otc_t_ppcs_diameter(sqlContext, val_base_altamira_consultas, val_otc_t_ppcs_diameter, val_fecha_ejecucion, val_fecha_ini)
    
    df_otc_t_ppcs_diameter = df_otc_t_ppcs_diameter_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_ppcs_diameter)
    return df_otc_t_ppcs_diameter, "Transformacion => fun_cargar_df_otc_t_ppcs_diameter => df_otc_t_ppcs_diameter" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_altamira.otc_t_ppcs_mecoorig
def fun_cargar_df_otc_t_ppcs_mecoorig(sqlContext, ):
    df_otc_t_ppcs_mecoorig_tmp = fun_otc_t_ppcs_mecoorig(sqlContext, val_base_altamira_consultas, val_otc_t_ppcs_mecoorig, val_fecha_ejecucion, val_fecha_ini)
    
    df_otc_t_ppcs_mecoorig = df_otc_t_ppcs_mecoorig_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_ppcs_mecoorig)
    return df_otc_t_ppcs_mecoorig, "Transformacion => fun_cargar_df_otc_t_ppcs_mecoorig => df_otc_t_ppcs_mecoorig" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base a los datos de la tabla db_altamira.otc_t_ppcs_content
def fun_cargar_df_otc_t_ppcs_content(sqlContext, ):
    df_otc_t_ppcs_content_tmp = fun_otc_t_ppcs_content(sqlContext, val_base_altamira_consultas, val_otc_t_ppcs_content, val_fecha_ejecucion, val_fecha_ini)
    
    df_otc_t_ppcs_content = df_otc_t_ppcs_content_tmp.cache()
        
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_ppcs_content)
    return df_otc_t_ppcs_content, "Transformacion => fun_cargar_df_otc_t_ppcs_content => df_otc_t_ppcs_content" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base al dataframe y lista antes generados: df_otc_t_ppcs_llamadas, list_dev_cat_plan_codigo
def fun_cargar_df_otc_t_voz_dias_tmp(sqlContext, df_otc_t_ppcs_llamadas, list_dev_cat_plan_codigo):
    
    df_otc_t_voz_dias_tmp = df_otc_t_ppcs_llamadas.filter(col('tip_prepago').isin(list_dev_cat_plan_codigo))
        
    df_otc_t_voz_dias_tmp = df_otc_t_voz_dias_tmp.selectExpr('msisdn','fecha','T_VOZ').distinct()
    
    df_otc_t_voz_dias_tmp = df_otc_t_voz_dias_tmp.cache()
        
    # Insercion de datos (sobre-escritura) para tabla DB_TEMPORALES.OTC_T_voz_dias_tmp
    val_retorno_insercion = fun_realizar_insercion_df_tabla(sqlContext, val_base_temporales, val_otc_t_voz_dias_tmp, df_otc_t_voz_dias_tmp)
    
    print(msg_succ('======== df_otc_t_voz_dias_tmp  =>  fun_realizar_insercion_df_tabla ==========='))
    print( msg_succ('%s => \n') % ( val_retorno_insercion ) )

    return df_otc_t_voz_dias_tmp, "Transformacion => fun_cargar_df_otc_t_voz_dias_tmp => df_otc_t_voz_dias_tmp" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base al dataframe y lista antes generados: df_otc_t_ppcs_diameter, list_dev_cat_plan_codigo
def fun_cargar_df_otc_t_datos_dias_tmp(sqlContext, df_otc_t_ppcs_diameter, list_dev_cat_plan_codigo):
    
    df_otc_t_datos_dias_tmp = df_otc_t_ppcs_diameter.filter(col('tip_prepago').isin(list_dev_cat_plan_codigo))
        
    df_otc_t_datos_dias_tmp = df_otc_t_datos_dias_tmp.selectExpr('msisdn','fecha','T_DATOS').distinct()
    
    df_otc_t_datos_dias_tmp = df_otc_t_datos_dias_tmp.cache()
        
    # Insercion de datos (sobre-escritura) para tabla DB_TEMPORALES.OTC_T_datos_dias_tmp
    val_retorno_insercion = fun_realizar_insercion_df_tabla(sqlContext, val_base_temporales, val_otc_t_datos_dias_tmp, df_otc_t_datos_dias_tmp)
    
    print(msg_succ('======== df_otc_t_datos_dias_tmp  =>  fun_realizar_insercion_df_tabla ==========='))
    print( msg_succ('%s => \n') % ( val_retorno_insercion ) )

    return df_otc_t_datos_dias_tmp, "Transformacion => fun_cargar_df_otc_t_datos_dias_tmp => df_otc_t_datos_dias_tmp" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base al dataframe y lista antes generados: df_otc_t_ppcs_mecoorig, list_dev_cat_plan_codigo
def fun_cargar_df_otc_t_sms_dias_tmp(sqlContext, df_otc_t_ppcs_mecoorig, list_dev_cat_plan_codigo):
    
    df_otc_t_sms_dias_tmp = df_otc_t_ppcs_mecoorig.filter(col('tip_prepago').isin(list_dev_cat_plan_codigo))
        
    df_otc_t_sms_dias_tmp = df_otc_t_sms_dias_tmp.selectExpr('msisdn','fecha','T_SMS').distinct()
    
    df_otc_t_sms_dias_tmp = df_otc_t_sms_dias_tmp.cache()
        
    # Insercion de datos (sobre-escritura) para tabla DB_TEMPORALES.OTC_T_datos_dias_tmp
    val_retorno_insercion = fun_realizar_insercion_df_tabla(sqlContext, val_base_temporales, val_otc_t_sms_dias_tmp, df_otc_t_sms_dias_tmp)
    
    print(msg_succ('======== df_otc_t_sms_dias_tmp  =>  fun_realizar_insercion_df_tabla ==========='))
    print( msg_succ('%s => \n') % ( val_retorno_insercion ) )

    return df_otc_t_sms_dias_tmp, "Transformacion => fun_cargar_df_otc_t_sms_dias_tmp => df_otc_t_sms_dias_tmp" + str_datos_df

@seguimiento_transformacion
# Generamos un dataframe en base al dataframe y lista antes generados: df_otc_t_ppcs_content, list_dev_cat_plan_codigo
def fun_cargar_df_otc_t_cont_dias_tmp(sqlContext, df_otc_t_ppcs_content, list_dev_cat_plan_codigo):
    
    df_otc_t_cont_dias_tmp = df_otc_t_ppcs_content.filter(col('tip_prepago').isin(list_dev_cat_plan_codigo))
        
    df_otc_t_cont_dias_tmp = df_otc_t_cont_dias_tmp.selectExpr('msisdn','fecha','T_CONTENIDO').distinct()
    
    df_otc_t_cont_dias_tmp = df_otc_t_cont_dias_tmp.cache()
        
    # Insercion de datos (sobre-escritura) para tabla DB_TEMPORALES.OTC_T_datos_dias_tmp
    val_retorno_insercion = fun_realizar_insercion_df_tabla(sqlContext, val_base_temporales, val_otc_t_cont_dias_tmp, df_otc_t_cont_dias_tmp)
    
    print(msg_succ('======== df_otc_t_cont_dias_tmp  =>  fun_realizar_insercion_df_tabla ==========='))
    print( msg_succ('%s => \n') % ( val_retorno_insercion ) )

    return df_otc_t_cont_dias_tmp, "Transformacion => fun_cargar_df_otc_t_cont_dias_tmp => df_otc_t_cont_dias_tmp" + str_datos_df

@seguimiento_transformacion
#  Cargamos los Datos resultantes de la union de tablas en HIVE => db_temporales.otc_t_parque_traficador_dias_tmp
def fun_cargar_df_otc_t_360_ubicacion(sqlContext, df_mksharevozdatos_90_final, val_base_reportes, val_otc_t_360_ubicacion, val_fecha_ejecucion): 
    
    # Insercion de Datos
    val_retorno_insercion = fun_cargar_datos_dinamico(sqlContext, df_mksharevozdatos_90_final, val_base_reportes, val_otc_t_360_ubicacion, val_fecha_ejecucion, val_fecha_ejecucion, val_fecha_ejecucion)
    print(msg_succ('======== df_otc_t_360_ubicacion  =>  fun_cargar_datos_dinamico ==========='))
    print( msg_succ('%s => \n') % ( val_retorno_insercion ) )

    str_datos_df = fun_obtener_datos_df(val_se_calcula,df_otc_t_360_ubicacion)
    return "Transformacion => fun_cargar_df_otc_t_360_ubicacion => df_otc_t_360_ubicacion" + str_datos_df






### control point 2
@seguimiento_transformacion
#  Eliminados los Datos en la tabla de HIVE => db_reportes.otc_t_360_ubicacion
def fun_eliminar_df_otc_t_360_ubicacion(sqlContext, val_fecha_ejecucion):
    print(msg_succ('======== Fechas ::: fun_eliminar_df_otc_t_360_ubicacion ==========='))
    print( msg_succ('\n %s\n') % ( str(val_fecha_ejecucion) ) )
    
    val_retorno_insercion = fun_eliminar_particiones(sqlContext, val_base_reportes, val_otc_t_360_ubicacion, val_fecha_ejecucion, val_fecha_ejecucion, val_fecha_ejecucion)
    print(msg_succ('======== df_otc_t_360_ubicacion =>  fun_eliminar_particiones ==========='))
    print( msg_succ('%s => \n') % ( val_retorno_insercion ) )

    str_datos_df = str(val_retorno_insercion)
    return "Transformacion => fun_eliminar_df_otc_t_360_ubicacion => df_otc_t_360_ubicacion " + str_datos_df

@seguimiento_transformacion
#  Cargamos los Datos en la tabla de HIVE => db_reportes.otc_t_360_ubicacion
def fun_cargar_df_otc_t_360_ubicacion(sqlContext, df_mksharevozdatos_90_final, val_base_reportes, val_otc_t_360_ubicacion, val_fecha_ejecucion): 
    
    # Insercion de Datos
    val_retorno_insercion = fun_cargar_datos_dinamico(sqlContext, df_mksharevozdatos_90_final, val_base_reportes, val_otc_t_360_ubicacion, val_fecha_ejecucion, val_fecha_ejecucion, val_fecha_ejecucion)
    print(msg_succ('======== df_otc_t_360_ubicacion  =>  fun_cargar_datos_dinamico ==========='))
    print( msg_succ('%s => \n') % ( val_retorno_insercion ) )

    str_datos_df = fun_obtener_datos_df(val_se_calcula,df_otc_t_360_ubicacion)
    return "Transformacion => fun_cargar_df_otc_t_360_ubicacion => df_otc_t_360_ubicacion" + str_datos_df
    