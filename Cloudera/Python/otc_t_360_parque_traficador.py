from datetime import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType, DoubleType, DateType, StringType, FloatType, TimestampType, StructField, IntegerType, BooleanType
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import concat
from pyspark.sql.functions import to_date
from pyspark import SQLContext
import argparse
import time
import sys
import os
# General cliente 360
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Configuraciones')
from otc_t_360_parque_traficador_config import *
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Querys')
from otc_t_360_parque_traficador_query import *
# Genericos otc_t_360_parque_traficador
sys.path.insert(1,'/var/opt/tel_spark')
from messages import *
from functions import *

timestart = datetime.now()
print(etq_info("Inicio del proceso en PySpark..."))
#Parametros definidos
VStp='Paso [1]: Cargando parametros desde la Shell..'
print(lne_dvs())
try:
    ts_step = datetime.now() 
    print(etq_info(VStp))
    print(lne_dvs())
    parser = argparse.ArgumentParser()
    parser.add_argument('--vSEntidad', required=True, type=str)
    parser.add_argument('--vSSchHiveTmp', required=True, type=str)
    parser.add_argument('--vTPPCSLlamadas', required=True, type=str)
    parser.add_argument('--vTDevCatPlan', required=True, type=str)
    parser.add_argument('--vTPPCSDiameter', required=True, type=str)
    parser.add_argument('--vTPPCSMecooring', required=True, type=str)
    parser.add_argument('--vTPPCSContent', required=True, type=str)
    parser.add_argument('--f_inicio', required=True, type=str)
    parser.add_argument('--FECHAEJE', required=True, type=str)
    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vSSchHiveTmp=parametros.vSSchHiveTmp
    vTPPCSLlamadas=parametros.vTPPCSLlamadas
    vTDevCatPlan=parametros.vTDevCatPlan
    vTPPCSDiameter=parametros.vTPPCSDiameter
    vTPPCSMecooring=parametros.vTPPCSMecooring
    vTPPCSContent=parametros.vTPPCSContent
    f_inicio=parametros.f_inicio
    FECHAEJE=parametros.FECHAEJE
    print(etq_info(log_p_parametros("vSEntidad",vSEntidad)))
    print(etq_info(log_p_parametros("vSSchHiveTmp",vSSchHiveTmp)))
    print(etq_info(log_p_parametros("vTPPCSLlamadas",vTPPCSLlamadas)))
    print(etq_info(log_p_parametros("vTDevCatPlan",vTDevCatPlan)))
    print(etq_info(log_p_parametros("vTPPCSDiameter",vTPPCSDiameter)))
    print(etq_info(log_p_parametros("vTPPCSMecooring",vTPPCSMecooring)))
    print(etq_info(log_p_parametros("vTPPCSContent",vTPPCSContent)))
    print(etq_info(log_p_parametros("f_inicio",f_inicio)))
    print(etq_info(log_p_parametros("FECHAEJE",FECHAEJE)))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [2]: Cargando session en PySpark..'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    spark = SparkSession\
        .builder\
        .enableHiveSupport() \
        .config("spark.sql.broadcastTimeout", "36000") \
        .config("spark.yarn.queue", "capa_semantica") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("hive.enforce.bucketing", "false")\
	    .config("hive.enforce.sorting", "false")\
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    app_id = spark._sc.applicationId
    print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [3]: Cargando configuracion [NOMBRE TABLAS MAIN/TEMP]/Eliminando tablas temporales'

try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())  
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_parque_traficador_01(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_parque_traficador_02(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_parque_traficador_03(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_parque_traficador_04(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_parque_traficador_05(vSSchHiveTmp)))
    print(etq_info(str(nme_tbl_otc_t_360_parque_traficador_01(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_parque_traficador_02(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_parque_traficador_03(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_parque_traficador_04(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_parque_traficador_05(vSSchHiveTmp))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4]: Generando logica de negocio '
print(etq_info(VStp))
print(lne_dvs())
VStp='Paso [4.1]: Se obtiene la informacion de los dias voz '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_parque_traficador_01(vSSchHiveTmp)))))
    print(etq_sql(qyr_otc_t_voz_dias_tmp(vTPPCSLlamadas, f_inicio, FECHAEJE, vTDevCatPlan)))
    df01=spark.sql(qyr_otc_t_voz_dias_tmp(vTPPCSLlamadas, f_inicio, FECHAEJE, vTDevCatPlan))
    if df01.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df01'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_parque_traficador_01(vSSchHiveTmp)))))
            df01.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_parque_traficador_01(vSSchHiveTmp)))
            df01.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_parque_traficador_01(vSSchHiveTmp)),str(df01.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_parque_traficador_01(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_parque_traficador_01(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.2]: Se obtiene la informacion de los datos moviles dias '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_parque_traficador_02(vSSchHiveTmp)))))
    print(etq_sql(qyr_otc_t_datos_dias_tmp(vTPPCSDiameter, f_inicio, FECHAEJE, vTDevCatPlan)))
    df02=spark.sql(qyr_otc_t_datos_dias_tmp(vTPPCSDiameter, f_inicio, FECHAEJE, vTDevCatPlan))
    if df02.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df02'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_parque_traficador_02(vSSchHiveTmp)))))
            df02.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_parque_traficador_02(vSSchHiveTmp)))
            df02.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_parque_traficador_02(vSSchHiveTmp)),str(df02.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_parque_traficador_02(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_parque_traficador_02(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.3]: Se obtiene la informacion de los sms dias'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_parque_traficador_03(vSSchHiveTmp)))))
    print(etq_sql(qyr_otc_t_sms_dias_tmp(vTPPCSMecooring, f_inicio, FECHAEJE, vTDevCatPlan)))
    df03=spark.sql(qyr_otc_t_sms_dias_tmp(vTPPCSMecooring, f_inicio, FECHAEJE, vTDevCatPlan))
    if df03.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df03'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_parque_traficador_03(vSSchHiveTmp)))))
            df03.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_parque_traficador_03(vSSchHiveTmp)))
            df03.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_parque_traficador_03(vSSchHiveTmp)),str(df03.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_parque_traficador_03(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_parque_traficador_03(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.4]: Se obtiene el universo de recargas unicos '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_parque_traficador_04(vSSchHiveTmp)))))
    print(etq_sql(qyr_otc_t_cont_dias_tmp(vTPPCSContent, f_inicio, FECHAEJE, vTDevCatPlan)))
    df04=spark.sql(qyr_otc_t_cont_dias_tmp(vTPPCSContent, f_inicio, FECHAEJE, vTDevCatPlan))
    if df04.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df04'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_parque_traficador_04(vSSchHiveTmp)))))
            df04.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_parque_traficador_04(vSSchHiveTmp)))
            df04.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_parque_traficador_04(vSSchHiveTmp)),str(df04.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_parque_traficador_04(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_parque_traficador_04(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.5]: Se obtiene la tabla final de parque traficador'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_parque_traficador_05(vSSchHiveTmp)))))
    print(etq_sql(qyr_otc_t_parque_traficador_dias_tmp(str(nme_tbl_otc_t_360_parque_traficador_01(vSSchHiveTmp)), str(nme_tbl_otc_t_360_parque_traficador_02(vSSchHiveTmp)), str(nme_tbl_otc_t_360_parque_traficador_03(vSSchHiveTmp)), str(nme_tbl_otc_t_360_parque_traficador_04(vSSchHiveTmp)), FECHAEJE)))
    df05=spark.sql(qyr_otc_t_parque_traficador_dias_tmp(str(nme_tbl_otc_t_360_parque_traficador_01(vSSchHiveTmp)), str(nme_tbl_otc_t_360_parque_traficador_02(vSSchHiveTmp)), str(nme_tbl_otc_t_360_parque_traficador_03(vSSchHiveTmp)), str(nme_tbl_otc_t_360_parque_traficador_04(vSSchHiveTmp)), FECHAEJE))
    if df05.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df05'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_parque_traficador_05(vSSchHiveTmp)))))
            df05.write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_parque_traficador_05(vSSchHiveTmp)))
            df05.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_parque_traficador_05(vSSchHiveTmp)),str(df05.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_parque_traficador_05(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_parque_traficador_05(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
vStpFin='Paso [Final]: Eliminando dataframes ..'
print(lne_dvs())

try:
    ts_step = datetime.now()    
    del df01
    del df02
    del df03
    del df04
    del df05
    
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStpFin,str(e))))

print(lne_dvs())

spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())