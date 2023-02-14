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
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/config')
from otc_t_360_recargas_config import *
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/query')
from otc_t_360_recargas_query import *
# Genericos otc_t_360_recargas
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
    parser.add_argument('--vTDetRecarg', required=True, type=str)
    parser.add_argument('--vTCatBonosPdv', required=True, type=str)
    parser.add_argument('--vTParOriRecarg', required=True, type=str)
    parser.add_argument('--fechaIni_menos_3meses', required=True, type=str)
    parser.add_argument('--fecha_eje2', required=True, type=str)
    parser.add_argument('--fechaIni_menos_2meses', required=True, type=str)
    parser.add_argument('--fecha_inico_mes_1_2', required=True, type=str)
    parser.add_argument('--fecha_menos30', required=True, type=str)
    parser.add_argument('--vSSchHiveTmp', required=True, type=str)
    parser.add_argument('--vSTblHiveMain', required=True, type=str)
    parser.add_argument('--fechaIni_menos_4meses', required=True, type=str)
    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vTDetRecarg=parametros.vTDetRecarg
    vTCatBonosPdv=parametros.vTCatBonosPdv
    vTParOriRecarg=parametros.vTParOriRecarg
    fechaIni_menos_3meses=parametros.fechaIni_menos_3meses
    fecha_eje2=parametros.fecha_eje2
    fechaIni_menos_2meses=parametros.fechaIni_menos_2meses
    fecha_inico_mes_1_2=parametros.fecha_inico_mes_1_2
    fecha_menos30=parametros.fecha_menos30
    vSSchHiveTmp=parametros.vSSchHiveTmp
    vSTblHiveMain=parametros.vSTblHiveMain
    fechaIni_menos_4meses=parametros.fechaIni_menos_4meses
    print(etq_info(log_p_parametros("vSEntidad",vSEntidad)))
    print(etq_info(log_p_parametros("vTDetRecarg",vTDetRecarg)))
    print(etq_info(log_p_parametros("vTCatBonosPdv",vTCatBonosPdv)))
    print(etq_info(log_p_parametros("vTParOriRecarg",vTParOriRecarg)))
    print(etq_info(log_p_parametros("fechaIni_menos_3meses",fechaIni_menos_3meses)))
    print(etq_info(log_p_parametros("fecha_eje2",fecha_eje2)))
    print(etq_info(log_p_parametros("fechaIni_menos_2meses",fechaIni_menos_2meses)))
    print(etq_info(log_p_parametros("fecha_inico_mes_1_2",fecha_inico_mes_1_2)))
    print(etq_info(log_p_parametros("fecha_menos30",fecha_menos30)))
    print(etq_info(log_p_parametros("vSSchHiveTmp",vSSchHiveTmp)))
    print(etq_info(log_p_parametros("fechaIni_menos_4meses",fechaIni_menos_4meses)))
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
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.yarn.queue", "default") \
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
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_03(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_04(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_05(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_06(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_07(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_08(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_09(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_10(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_11(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_12(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_13(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_14(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_15(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_16(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_recargas_17(vSSchHiveTmp)))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_03(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_04(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_05(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_06(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_07(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_08(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_09(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_10(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_11(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_12(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_13(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_14(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_15(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_16(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_recargas_17(vSSchHiveTmp))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4]: Generando logica de negocio '
print(etq_info(VStp))
print(lne_dvs())
VStp='Paso [4.1]: Se obtienen las recargas dia periodo de la tabla '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_recargas_dia_periodo(vTDetRecarg, vTParOriRecarg, fechaIni_menos_3meses, fecha_eje2)))
    df01=spark.sql(qry_tmp_360_otc_t_recargas_dia_periodo(vTDetRecarg, vTParOriRecarg, fechaIni_menos_3meses, fecha_eje2))
    if df01.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df01'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)))))
            df01.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)))
            df01.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)),str(df01.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.2]: Se modifica el rango de fecha para obtener bonos y combos de 2 meses atras '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_paquetes_payment_acum(vTDetRecarg, vTCatBonosPdv, fechaIni_menos_2meses, fecha_eje2)))
    df02=spark.sql(qry_tmp_360_otc_t_paquetes_payment_acum(vTDetRecarg, vTCatBonosPdv, fechaIni_menos_2meses, fecha_eje2))
    if df02.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df02'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)))))
            df02.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)))
            df02.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)),str(df02.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.3]: Se obtiene el universo de recargas '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_03(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_universo_recargas(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp))
                                                     ,str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)))))
    df03=spark.sql(qry_tmp_360_otc_t_universo_recargas(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp))
                                                     ,str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp))))
    if df03.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df03'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_03(vSSchHiveTmp)))))
            df03.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_03(vSSchHiveTmp)))
            df03.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_03(vSSchHiveTmp)),str(df03.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_03(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_03(vSSchHiveTmp)),str(e))))
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
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_04(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_universo_recargas_unicos(str(nme_tbl_otc_t_360_recargas_03(vSSchHiveTmp)))))
    df04=spark.sql(qry_tmp_360_otc_t_universo_recargas_unicos(str(nme_tbl_otc_t_360_recargas_03(vSSchHiveTmp))))
    if df04.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df04'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_04(vSSchHiveTmp)))))
            df04.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_04(vSSchHiveTmp)))
            df04.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_04(vSSchHiveTmp)),str(df04.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_04(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_04(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.5]: Se obtienen las recargas acumuladas mes 0'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_05(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_recargas_acum_0(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)), fecha_inico_mes_1_2, fecha_eje2)))
    df05=spark.sql(qry_tmp_360_otc_t_recargas_acum_0(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)), fecha_inico_mes_1_2, fecha_eje2))
    if df05.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df05'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_05(vSSchHiveTmp)))))
            df05.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_05(vSSchHiveTmp)))
            df05.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_05(vSSchHiveTmp)),str(df05.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_05(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_05(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.6]: Se obtienen las recargas de un rango de 30 dias menos a la fecha de ejecucion '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_06(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_recargas_acum_menos30(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)), fecha_menos30, fecha_eje2)))
    df06=spark.sql(qry_tmp_360_otc_t_recargas_acum_menos30(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)), fecha_menos30, fecha_eje2))
    if df06.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df06'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_06(vSSchHiveTmp)))))
            df06.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_06(vSSchHiveTmp)))
            df06.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_06(vSSchHiveTmp)),str(df06.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_06(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_06(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.7]: Se obtienen las recargas mes menos 1 '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_07(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_recargas_acum_1(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)), fechaIni_menos_2meses, fecha_inico_mes_1_2)))
    df07=spark.sql(qry_tmp_360_otc_t_recargas_acum_1(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)), fechaIni_menos_2meses, fecha_inico_mes_1_2))
    if df07.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df07'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_07(vSSchHiveTmp)))))
            df07.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_07(vSSchHiveTmp)))
            df07.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_07(vSSchHiveTmp)),str(df07.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_07(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_07(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.8]: Se obtienen las recargas mes menos 2'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_08(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_recargas_acum_2(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)), fechaIni_menos_3meses, fechaIni_menos_2meses)))
    df08=spark.sql(qry_tmp_360_otc_t_recargas_acum_2(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)), fechaIni_menos_3meses, fechaIni_menos_2meses))
    ##if df08.rdd.isEmpty():
        ##exit(etq_nodata(msg_e_df_nodata(str('df08'))))
    ##else:
    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_08(vSSchHiveTmp)))))
        df08.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_08(vSSchHiveTmp)))
        df08.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_08(vSSchHiveTmp)),str(df08.count())))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_08(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_08(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.9]: Se obtienen las recargas mes menos 3'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_09(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_recargas_acum_3(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)), fechaIni_menos_4meses, fechaIni_menos_3meses)))
    df09=spark.sql(qry_tmp_360_otc_t_recargas_acum_3(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)), fechaIni_menos_4meses, fechaIni_menos_3meses))
    ##if df09.rdd.isEmpty():
        ##exit(etq_nodata(msg_e_df_nodata(str('df09'))))
    ##else:
    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_09(vSSchHiveTmp)))))
        df09.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_09(vSSchHiveTmp)))
        df09.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_09(vSSchHiveTmp)),str(df09.count())))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_09(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_09(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.10]: dia ejecucion '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_10(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_recargas_dia_periodo_1(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)), fecha_eje2)))
    df10=spark.sql(qry_tmp_360_otc_t_recargas_dia_periodo_1(str(nme_tbl_otc_t_360_recargas_01(vSSchHiveTmp)), fecha_eje2))
    ##if df10.rdd.isEmpty():
      ##  exit(etq_nodata(msg_e_df_nodata(str('df10'))))
    ##else:
    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_10(vSSchHiveTmp)))))
        df10.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_10(vSSchHiveTmp)))
        df10.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_10(vSSchHiveTmp)),str(df10.count())))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_10(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_10(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.11]: Se obtienen los bonos acumulados del mes, se modifica el rango de fecha para obtener bonos y combos del mes '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_11(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_paquetes_payment_acum_bono(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)), fecha_inico_mes_1_2, fecha_eje2)))
    df11=spark.sql(qry_tmp_360_otc_t_paquetes_payment_acum_bono(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)), fecha_inico_mes_1_2, fecha_eje2))
    ##if df11.rdd.isEmpty():
        ##exit(etq_nodata(msg_e_df_nodata(str('df11'))))
    ##else:
    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_11(vSSchHiveTmp)))))
        df11.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_11(vSSchHiveTmp)))
        df11.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_11(vSSchHiveTmp)),str(df10.count())))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_11(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_11(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.12]: Se obtienen los bonos del dia'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_12(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_paquetes_payment_dia_bono(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)), fecha_eje2)))
    df12=spark.sql(qry_tmp_360_otc_t_paquetes_payment_dia_bono(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)), fecha_eje2))
    ##if df12.rdd.isEmpty():
      ##  exit(etq_nodata(msg_e_df_nodata(str('df12'))))
    ##else:
    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_12(vSSchHiveTmp)))))
        df12.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_12(vSSchHiveTmp)))
        df12.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_12(vSSchHiveTmp)),str(df12.count())))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_12(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_12(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.13]: Se obtienen los BONOS menos 30 dias'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_13(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_paquetes_bono_menos30(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)), fecha_menos30, fecha_eje2)))
    df13=spark.sql(qry_tmp_360_otc_t_paquetes_bono_menos30(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)), fecha_menos30, fecha_eje2))
    ##if df13.rdd.isEmpty():
        ##exit(etq_nodata(msg_e_df_nodata(str('df13'))))
    ##else:
    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_13(vSSchHiveTmp)))))
        df13.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_13(vSSchHiveTmp)))
        df13.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_13(vSSchHiveTmp)),str(df13.count())))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_13(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_13(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.14]: Se modifica el rango de fecha para obtener bonos y combos del mes'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_14(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_paquetes_payment_acum_combo(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)), fecha_inico_mes_1_2, fecha_eje2)))
    df14=spark.sql(qry_tmp_360_otc_t_paquetes_payment_acum_combo(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)), fecha_inico_mes_1_2, fecha_eje2))
    ##if df14.rdd.isEmpty():
       ## exit(etq_nodata(msg_e_df_nodata(str('df14'))))
    ##else:
    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_14(vSSchHiveTmp)))))
        df14.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_14(vSSchHiveTmp)))
        df14.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_14(vSSchHiveTmp)),str(df14.count())))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_14(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_14(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.15]: Se obtienen los combos del dia'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_15(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_paquetes_payment_dia_combo(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)), fecha_eje2)))
    df15=spark.sql(qry_tmp_360_otc_t_paquetes_payment_dia_combo(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)), fecha_eje2))
    ##if df15.rdd.isEmpty():
      ##  exit(etq_nodata(msg_e_df_nodata(str('df15'))))
    ##else:
    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_15(vSSchHiveTmp)))))
        df15.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_15(vSSchHiveTmp)))
        df15.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_15(vSSchHiveTmp)),str(df15.count())))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_15(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_15(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.16]: Se obtienen los combos de un rango de 30 dias menos a la fecha de ejecucion'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_16(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_paquetes_payment_combo_menos30(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)), fecha_menos30, fecha_eje2)))
    df16=spark.sql(qry_tmp_360_otc_t_paquetes_payment_combo_menos30(str(nme_tbl_otc_t_360_recargas_02(vSSchHiveTmp)), fecha_menos30, fecha_eje2))
    ##if df16.rdd.isEmpty():
      ##  exit(etq_nodata(msg_e_df_nodata(str('df16'))))
    ##else:
    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_16(vSSchHiveTmp)))))
        df16.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_16(vSSchHiveTmp)))
        df16.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_16(vSSchHiveTmp)),str(df16.count())))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_16(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_16(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.17]: CONSOLIDACION DE TODOS LOS VALORES OBTENIDOS, modificacion se agregan los campos ingreso_recargas_30,cantidad_recargas_30, ingreso_bonos_30,cantidad_bonos_30, ingreso_combos_30,cantidad_combos_30 para obtener PARQUE_RECARGADOR_30_DIAS'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_recargas_17(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_otc_t_360_recargas(str(nme_tbl_otc_t_360_recargas_04(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_10(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_05(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_07(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_08(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_09(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_11(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_14(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_12(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_15(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_06(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_13(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_16(vSSchHiveTmp)))))
    df17=spark.sql(qry_tmp_otc_t_360_recargas(str(nme_tbl_otc_t_360_recargas_04(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_10(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_05(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_07(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_08(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_09(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_11(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_14(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_12(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_15(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_06(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_13(vSSchHiveTmp)), str(nme_tbl_otc_t_360_recargas_16(vSSchHiveTmp))))
    ##if df17.rdd.isEmpty():
        ##exit(etq_nodata(msg_e_df_nodata(str('df17'))))
    ##else:
    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_recargas_17(vSSchHiveTmp)))))
        df17.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_recargas_17(vSSchHiveTmp)))
        df17.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_recargas_17(vSSchHiveTmp)),str(df17.count())))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_recargas_17(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_recargas_17(vSSchHiveTmp)),str(e))))
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
    del df06
    del df07
    del df08
    del df09
    del df10
    del df11
    del df12
    del df13
    del df14
    del df15
    del df16
    del df17
    
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStpFin,str(e))))

print(lne_dvs())

spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())