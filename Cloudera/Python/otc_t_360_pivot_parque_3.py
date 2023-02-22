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
from otc_t_360_pivot_parque_config import *
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Querys')
from otc_t_360_pivot_parque_query import *
# Genericos
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
    parser.add_argument('--vTRiMobPN', required=True, type=str)
    parser.add_argument('--fec_alt_ini', required=True, type=str)
    parser.add_argument('--vAbrev', required=True, type=str)
    parser.add_argument('--vIFechaProceso', required=True, type=str)
    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vSSchHiveTmp=parametros.vSSchHiveTmp
    vTRiMobPN=parametros.vTRiMobPN
    fec_alt_ini=parametros.fec_alt_ini
    vAbrev=parametros.vAbrev 
    vIFechaProceso=parametros.vIFechaProceso 
    print(etq_info(log_p_parametros("vSEntidad",vSEntidad)))
    print(etq_info(log_p_parametros("vSSchHiveTmp",vSSchHiveTmp)))
    print(etq_info(log_p_parametros("vTRiMobPN",vTRiMobPN)))
    print(etq_info(log_p_parametros("fec_alt_ini",fec_alt_ini)))
    print(etq_info(log_p_parametros("vAbrev",vAbrev)))
    print(etq_info(log_p_parametros("vIFechaProceso",vIFechaProceso)))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
## .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")\
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
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_pivot_parque_20(vSSchHiveTmp)))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_pivot_parque_20(vSSchHiveTmp))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
## N15
VStp='Paso [4]: Generando logica de negocio '
print(etq_info(VStp))
print(lne_dvs())
VStp='Paso [4.1]: Se obtiene el parque activo  '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev)))))
    print(etq_sql(qyr_tmp_360_otc_t_parque_act(str(nme_tbl_tmp_otc_t_360_pivot_parque_14(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_01(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_04(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_05(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_06(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_07(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_02(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_03(vSSchHiveTmp, vAbrev)))))
    df15=spark.sql(qyr_tmp_360_otc_t_parque_act(str(nme_tbl_tmp_otc_t_360_pivot_parque_14(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_01(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_04(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_05(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_06(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_07(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_02(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_03(vSSchHiveTmp, vAbrev))))
    #if df15.rdd.isEmpty():
    #    exit(etq_nodata(msg_e_df_nodata(str('df15'))))
    #else:
    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev)))))
        df15.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev)))
        df15.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev)),str(df15.count())))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

## N16
VStp='Paso [4.2]: Se obtiene el parque inactivo '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev)))))
    print(etq_sql(qyr_tmp_360_otc_t_parque_inact(str(nme_tbl_tmp_otc_t_360_pivot_parque_13(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_10(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_02(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_03(vSSchHiveTmp, vAbrev)))))
    df16=spark.sql(qyr_tmp_360_otc_t_parque_inact(str(nme_tbl_tmp_otc_t_360_pivot_parque_13(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_10(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_02(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_03(vSSchHiveTmp, vAbrev))))
    #if df16.rdd.isEmpty():
    #    exit(etq_nodata(msg_e_df_nodata(str('df16'))))
    #else:
    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev)))))
        df16.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev)))
        df16.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev)),str(df16.count())))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
## N17
VStp='Paso [4.3]: Se obtienen las lineas preactivas	 '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev)))))
    print(etq_sql(qyr_tmp_360_base_preactivos(vTRiMobPN, fec_alt_ini)))
    df17=spark.sql(qyr_tmp_360_base_preactivos(vTRiMobPN, fec_alt_ini))
    #if df17.rdd.isEmpty():
    #    exit(etq_nodata(msg_e_df_nodata(str('df17'))))
    #else:
    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev)))))
        df17.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev)))
        df17.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev)),str(df17.count())))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
## N18
VStp='Paso [4.4]: Se obtiene la primera tabla temporal con todo el parque '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev)))))
    print(etq_sql(qyr_otc_t_360_parque_1_tmp_all(str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev)))))
    df18=spark.sql(qyr_otc_t_360_parque_1_tmp_all(str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev))))
    df18.printSchema()
    
    print(lne_dvs())
    print(etq_sql(qyr_otc_t_360_parque_1_tmp_all_1(str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev)), vIFechaProceso)))
    df18a=spark.sql(qyr_otc_t_360_parque_1_tmp_all_1(str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev)), vIFechaProceso))
    df18a.printSchema()
    
    print(lne_dvs())
    print(etq_sql(qyr_otc_t_360_parque_1_tmp_all_2(str(nme_tbl_tmp_otc_t_360_pivot_parque_19(vSSchHiveTmp, vAbrev)), vIFechaProceso)))
    df18b=spark.sql(qyr_otc_t_360_parque_1_tmp_all_2(str(nme_tbl_tmp_otc_t_360_pivot_parque_19(vSSchHiveTmp, vAbrev)), vIFechaProceso))
    df18b.printSchema()
    
    print(lne_dvs())
    print(etq_sql(qyr_not_in_list_1(str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev)))))
    df18l1=spark.sql(qyr_not_in_list_1(str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev))))
    df18l1.printSchema()
    
    print(lne_dvs())
    print(etq_sql(qyr_not_in_list_2(str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev)))))
    df18l2=spark.sql(qyr_not_in_list_2(str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev))))
    df18l2.printSchema()
    #if (df18.rdd.isEmpty()) | (df18a.rdd.isEmpty()) | (df18b.rdd.isEmpty()) | (df18l1.rdd.isEmpty()) | (df18l2.rdd.isEmpty()):
    #    exit(etq_nodata(msg_e_df_nodata(str('df18'))))
    #else:
    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev)))))
        not_in_list_1 = list(df18l1.select('num_telefonico').toPandas()['num_telefonico'])
        not_in_list_2 = list(df18l2.select('num_telefonico').toPandas()['num_telefonico'])
        df18a=df18a.filter(df18a.num_telefonico.isin(not_in_list_1))
        df18b=df18b.filter(df18b.num_telefonico.isin(not_in_list_2))
        df18final=df18.union(df18a.union(df18b))
        df18final.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev)))
        df18final.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev)),str(df18final.count())))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

# N 20
VStp='Paso [4.5]: Se obtiene la TABLA FINAL de este proceso '
try: 
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_pivot_parque_20(vSSchHiveTmp)))))
    print(etq_sql(qyr_otc_t_360_parque_1_tmp(str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_09(vSSchHiveTmp, vAbrev)))))
    df20=spark.sql(qyr_otc_t_360_parque_1_tmp(str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev)), str(nme_tbl_tmp_otc_t_360_pivot_parque_09(vSSchHiveTmp, vAbrev))))
    if df20.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df20'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_20(vSSchHiveTmp)))))
            df20.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_pivot_parque_20(vSSchHiveTmp)))
            df20.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_20(vSSchHiveTmp)),str(df20.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_20(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_20(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
vStpFin='Paso [Final]: Eliminando dataframes ..'
print(lne_dvs())

try:
    ts_step = datetime.now()    
    del df15
    del df16
    del df17
    del df18
    del df20
    
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStpFin,str(e))))

print(lne_dvs())

spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())

### sh -x /RGenerator/reportes/Cliente360/Bin/OTC_T_360_PIVOT_PARQUE.sh 20230126
