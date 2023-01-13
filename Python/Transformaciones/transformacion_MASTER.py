# -*- coding: utf-8 -*-
from Configuraciones.configuracion import *
from Querys.sql import *
from Funciones.funcion import *
import pyspark.sql.functions as sql_fun
from pyspark.sql.functions import col, substring, max, min, when, count, sum, lit, unix_timestamp, desc, asc, length, expr, datediff, row_number, coalesce, split, trim, concat, concat_ws, to_date, last_day, to_timestamp
from pyspark.sql.types import StringType, DateType, IntegerType, StructType, TimestampType
from pyspark.sql.window import Window
import pandas as pd

#*******************************************************************#
#* 1. OTC_T_360_UBICACION                                           #
#*******************************************************************#

@seguimiento_transformacion
# Funcion Principal 
def func_proceso_ubicacion(sqlContext, val_fecha_ejecucion, val_base_reportes, val_otc_t_360_ubicacion, val_franja_horaria):
    
    # controlpoint1    
    val_str_controlpoint1, df_mksharevozdatos_90, df_mksharevozdatos_90_max, df_otc_t_360_ubicacion = fun_controlpoint1(sqlContext, val_fecha_ejecucion, val_franja_horaria, val_base_reportes, val_otc_t_360_ubicacion)

    # Mensajes
    val_str_resultado = val_str_controlpoint1
    
    return val_str_resultado

# FUNCIONES PRINCIPALES DE CONTROL

@seguimiento_transformacion
def fun_controlpoint1(sqlContext, val_fecha_ejecucion,  val_franja_horaria, val_base_reportes, val_otc_t_360_ubicacion):
    
    print(msg_succ('================================================================'))
    print(msg_succ('======== FUNCION DE CONTROL => fun_controlpoint1 ==============='))
    print(msg_succ('================================================================'))
    
    df_mksharevozdatos_90, val_str_df_mksharevozdatos_90= fun_cargar_df_mksharevozdatos_90(sqlContext, val_franja_horaria)    
    df_mksharevozdatos_90_max, val_str_df_mksharevozdatos_90_max= fun_cargar_df_mksharevozdatos_90_max(sqlContext, val_fecha_ejecucion)
    df_otc_t_360_ubicacion, val_str_cargar_otc_t_360_ubicacion = fun_cargar_otc_t_360_ubicacion(sqlContext, df_mksharevozdatos_90, df_mksharevozdatos_90_max, val_base_reportes, val_otc_t_360_ubicacion, val_fecha_ejecucion)
        
    val_str_resultado_cp1 = val_str_df_mksharevozdatos_90 + "\n" + val_str_df_mksharevozdatos_90_max 
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n" + val_str_cargar_otc_t_360_ubicacion
    val_str_resultado_cp1 = val_str_resultado_cp1 + "\n"
    
    return val_str_resultado_cp1, df_mksharevozdatos_90, df_mksharevozdatos_90_max, df_otc_t_360_ubicacion
    
# FUNCIONES QUE GENERAN DATAFRAME

@seguimiento_transformacion
# Se genera un dataframe en base a los datos de la tabla db_ipaccess.mksharevozdatos_90
def fun_cargar_df_mksharevozdatos_90(sqlContext, val_franja_horaria):
    df_mksharevozdatos_90_tmp = fun_mksharevozdatos_90 (sqlContext, val_base_ipaccess_consultas, val_mksharevozdatos_90, val_franja_horaria)
    df_mksharevozdatos_90 = df_mksharevozdatos_90_tmp.cache()
    df_mksharevozdatos_90.show()
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_mksharevozdatos_90)
    return df_mksharevozdatos_90, "Transformacion => fun_cargar_df_mksharevozdatos_90 => df_mksharevozdatos_90" + str_datos_df

@seguimiento_transformacion
# Se genera un dataframe en base a los datos de la tabla db_ipaccess.mksharevozdatos_90
def fun_cargar_df_mksharevozdatos_90_max(sqlContext, val_fecha_ejecucion):
    df_mksharevozdatos_90_max_tmp = fun_mksharevozdatos_90_max(sqlContext, val_base_ipaccess_consultas, val_mksharevozdatos_90, val_fecha_ejecucion)
    df_mksharevozdatos_90_max = df_mksharevozdatos_90_max_tmp.cache()
    df_mksharevozdatos_90_max.show()
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_mksharevozdatos_90_max)
    return df_mksharevozdatos_90_max, "Transformacion => fun_cargar_df_mksharevozdatos_90_max => df_mksharevozdatos_90_max" + str_datos_df

@seguimiento_transformacion
#  Se cargan los Datos en la tabla de HIVE => db_reportes.otc_t_360_ubicacion
def fun_cargar_otc_t_360_ubicacion(sqlContext, df_mksharevozdatos_90, df_mksharevozdatos_90_max, val_base_reportes, val_otc_t_360_ubicacion, val_fecha_ejecucion): 
    
    t1 = df_mksharevozdatos_90.alias('t1')
    t2 = df_mksharevozdatos_90_max.alias('t2')
    
    df_mksharevozdatos_90_final_tmp =  t1.join(t2, expr("t2.max_fecha = t1.fecha_proceso"), how='inner') \
                                .selectExpr(u"t1.*")
    df_mksharevozdatos_90_final_tmp = df_mksharevozdatos_90_final_tmp.withColumn('fecha_proceso', lit(val_fecha_ejecucion))
    df_otc_t_360_ubicacion = df_mksharevozdatos_90_final_tmp.cache()
    
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_otc_t_360_ubicacion)
    
    # Insercion de Datos
    val_retorno_insercion = fun_cargar_datos_dinamico(sqlContext, df_otc_t_360_ubicacion, val_base_reportes, val_otc_t_360_ubicacion, val_fecha_ejecucion, val_fecha_ejecucion, val_fecha_ejecucion)
    print(msg_succ('======== otc_t_360_ubicacion  =>  fun_cargar_datos_dinamico ==========='))
    print( msg_succ('%s => \n') % ( val_retorno_insercion ) )

    return df_otc_t_360_ubicacion, "Transformacion => fun_cargar_otc_t_360_ubicacion => otc_t_360_ubicacion" + str_datos_df

#*******************************************************************#
#* 2. OTC_T_360_PARQUE_TRAFICADOR                                   #
#*******************************************************************#

@seguimiento_transformacion
# Funcion Principal 
def func_proceso_parque_traficador(sqlContext, val_fecha_ejecucion, val_fecha_inicio, val_base_temporales):
    
    # controlpoint2    
    val_str_controlpoint2, df_mksharevozdatos_90, df_mksharevozdatos_90_max, df_otc_t_360_ubicacion = fun_controlpoint2(sqlContext, val_fecha_ejecucion, val_franja_horaria, val_base_reportes, val_otc_t_360_ubicacion)

    # Mensajes
    val_str_resultado = val_str_controlpoint1
    
    return val_str_resultado

@seguimiento_transformacion
def fun_controlpoint2(sqlContext,val_fecha_ejecucion, val_fecha_inicio, val_base_temporales):
    print(msg_succ('================================================================'))
    print(msg_succ('======== FUNCION DE CONTROL => fun_controlpoint2 ==============='))
    print(msg_succ('================================================================'))
    
    val_str_resultado_cp2 = val_str_cargar_otc_t_360_ubicacion_tmp + "\n" + val_str_cargar_df_otc_t_360_ubicacion_tmp 
    val_str_resultado_cp2 = val_str_resultado_cp2 + "\n" + "\n" + val_str_cargar_otc_t_360_ubicacion 
    val_str_resultado_cp2 = val_str_resultado_cp2 + "\n"
    
    return val_str_resultado_cp2

# FUNCIONES QUE GENERAN DATAFRAME

@seguimiento_transformacion
# Se genera un dataframe en base a los datos de la tabla db_reportes.otc_t_dev_cat_plan
def fun_cargar_df_dev_cat_plan(sqlContext, val_fecha_ejecucion):
    df_dev_cat_plan_tmp = fun_otc_t_dev_cat_plan(sqlContext, val_base_reportes_consultas, val_otc_t_dev_cat_plan, val_marca)
    df_dev_cat_plan = df_dev_cat_plan_tmp.cache()
    # df_dev_cat_plan.show()
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_dev_cat_plan)
    return df_dev_cat_plan, "Transformacion => fun_cargar_df_dev_cat_plan => df_dev_cat_plan" + str_datos_df

@seguimiento_transformacion
# Se genera un dataframe en base a los datos de la tabla db_ipaccess.mksharevozdatos_90
def fun_cargar_df_voz_dias_tmp(sqlContext, val_fecha_ejecucion, val_fecha_inicio, df_dev_cat_plan):
    df_ppcs_llamadas = fun_otc_t_ppcs_llamadas (sqlContext, val_base_altamira_consultas, val_otc_t_ppcs_llamadas, val_fecha_ejecucion, val_fecha_inicio)
    
    list_dev_cat_plan = list(df_dev_cat_plan.select('codigo') )
    df_voz_dias_tmp = df_parque_actual_cp_1_tmp.selectExpr( "SUBSTR(TRIM(NUM_TELEFONICO),LENGTH(TRIM(NUM_TELEFONICO))-8,9) AS TELEFONO",
    df_voz_dias_tmp = df_mksharevozdatos_90_tmp.cache()
    df_mksharevozdatos_90.show()
    str_datos_df = fun_obtener_datos_df(val_se_calcula, df_mksharevozdatos_90)
    return df_mksharevozdatos_90, "Transformacion => fun_cargar_df_mksharevozdatos_90 => df_mksharevozdatos_90" + str_datos_df

