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
from functools import reduce
from pyspark.sql import DataFrame

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
    print(etq_info("Inicio del proceso en PySpark...")) 
    print(lne_dvs())
    print(etq_info("Importando librerias personalizadas..."))
    sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Configuraciones')
    #sys.path.insert(1,'/STAGE/versionamiento/CapaSemantica/CLIENTE_360/Python/Configuraciones')
    from otc_t_360_pivot_parque_config import *
    sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Querys')
    #sys.path.insert(1,'/STAGE/versionamiento/CapaSemantica/CLIENTE_360/Python/Querys')
    from otc_t_360_pivot_parque_query import *
    print(lne_dvs())
    print(etq_info("Tablas termporales del proceso..."))
    print(lne_dvs())
    vTPP01=str(nme_tbl_tmp_otc_t_360_pivot_parque_01(vSSchHiveTmp, vAbrev))
    vTPP02=str(nme_tbl_tmp_otc_t_360_pivot_parque_02(vSSchHiveTmp, vAbrev))
    vTPP03=str(nme_tbl_tmp_otc_t_360_pivot_parque_03(vSSchHiveTmp, vAbrev))
    vTPP04=str(nme_tbl_tmp_otc_t_360_pivot_parque_04(vSSchHiveTmp, vAbrev))
    vTPP05=str(nme_tbl_tmp_otc_t_360_pivot_parque_05(vSSchHiveTmp, vAbrev))
    vTPP06=str(nme_tbl_tmp_otc_t_360_pivot_parque_06(vSSchHiveTmp, vAbrev))
    vTPP07=str(nme_tbl_tmp_otc_t_360_pivot_parque_07(vSSchHiveTmp, vAbrev))
    vTPP09=str(nme_tbl_tmp_otc_t_360_pivot_parque_09(vSSchHiveTmp, vAbrev))
    vTPP10=str(nme_tbl_tmp_otc_t_360_pivot_parque_10(vSSchHiveTmp, vAbrev))
    vTPP13=str(nme_tbl_tmp_otc_t_360_pivot_parque_13(vSSchHiveTmp, vAbrev))
    vTPP14=str(nme_tbl_tmp_otc_t_360_pivot_parque_14(vSSchHiveTmp, vAbrev))
    vTPP15=str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev))
    vTPP16=str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev))
    vTPP17=str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev))
    vTPP18=str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev))
    vTPP19=str(nme_tbl_tmp_otc_t_360_pivot_parque_19(vSSchHiveTmp, vAbrev))
    vTPP20=str(nme_tbl_tmp_otc_t_360_pivot_parque_20(vSSchHiveTmp))
    print(etq_info(log_p_parametros('vTPP02', vTPP02)))
    print(etq_info(log_p_parametros('vTPP03', vTPP03)))
    print(etq_info(log_p_parametros('vTPP04', vTPP04)))
    print(etq_info(log_p_parametros('vTPP05', vTPP05)))
    print(etq_info(log_p_parametros('vTPP06', vTPP06)))
    print(etq_info(log_p_parametros('vTPP07', vTPP07)))
    print(etq_info(log_p_parametros('vTPP01', vTPP01)))
    print(etq_info(log_p_parametros('vTPP09', vTPP09)))
    print(etq_info(log_p_parametros('vTPP10', vTPP10)))
    print(etq_info(log_p_parametros('vTPP13', vTPP13)))
    print(etq_info(log_p_parametros('vTPP14', vTPP14)))
    print(etq_info(log_p_parametros('vTPP15', vTPP15)))
    print(etq_info(log_p_parametros('vTPP16', vTPP16)))
    print(etq_info(log_p_parametros('vTPP17', vTPP17)))
    print(etq_info(log_p_parametros('vTPP18', vTPP18)))
    print(etq_info(log_p_parametros('vTPP19', vTPP19)))
    print(etq_info(log_p_parametros('vTPP20', vTPP20)))
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
    print(etq_info(msg_i_create_hive_tmp(vTPP15)))
    print(etq_sql(qyr_tmp_360_otc_t_parque_act(vTPP14, vTPP01, vTPP04, vTPP05, vTPP06, vTPP07, vTPP02, vTPP03)))
    df15=spark.sql(qyr_tmp_360_otc_t_parque_act(vTPP14, vTPP01, vTPP04, vTPP05, vTPP06, vTPP07, vTPP02, vTPP03))
    if df15.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df15'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP15)))
            df15.write.mode('overwrite').saveAsTable(vTPP15)
            df15.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP15,str(df15.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP15,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP15,str(e))))
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
    print(etq_info(msg_i_create_hive_tmp(vTPP16)))
    print(etq_sql(qyr_tmp_360_otc_t_parque_inact(vTPP13, vTPP10, vTPP02, vTPP03)))
    df16=spark.sql(qyr_tmp_360_otc_t_parque_inact(vTPP13, vTPP10, vTPP02, vTPP03))
    if df16.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df16'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP16)))
            df16.write.mode('overwrite').saveAsTable(vTPP16)
            df16.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP16,str(df16.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP16,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP16,str(e))))
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
    print(etq_info(msg_i_create_hive_tmp(vTPP17)))
    print(etq_sql(qyr_tmp_360_base_preactivos(vTRiMobPN, fec_alt_ini)))
    df17=spark.sql(qyr_tmp_360_base_preactivos(vTRiMobPN, fec_alt_ini))
    if df17.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df17'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP17)))
            df17.write.mode('overwrite').saveAsTable(vTPP17)
            df17.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP17,str(df17.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP17,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP17,str(e))))
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
    print(etq_info(msg_i_create_hive_tmp(vTPP18)))
    print(etq_sql(qyr_otc_t_360_parque_1_tmp_all(vTPP15, vTPP16)))
    df18=spark.sql(qyr_otc_t_360_parque_1_tmp_all(vTPP15, vTPP16))
    df18.printSchema()
    
    print(lne_dvs())
    print(etq_sql(qyr_not_in_list_1(vTPP15, vTPP16)))
    df18l1=spark.sql(qyr_not_in_list_1(vTPP15, vTPP16))
    df18l1.printSchema()
    df18l1.write.mode('overwrite').saveAsTable(vSSchHiveTmp+".otc_t_pivot_1811")
    
    print(lne_dvs())
    print(etq_sql(qyr_otc_t_360_parque_1_tmp_all_1(vTPP17, vIFechaProceso,vSSchHiveTmp+".otc_t_pivot_1811")))
    df18a=spark.sql(qyr_otc_t_360_parque_1_tmp_all_1(vTPP17, vIFechaProceso,vSSchHiveTmp+".otc_t_pivot_1811"))
    df18a.printSchema()
    print(etq_info("Guardando tabla tmp: (otc_t_pivot_18a)"))
    df18a.write.mode('overwrite').saveAsTable(vSSchHiveTmp+".otc_t_pivot_18a")      
        
    print(lne_dvs())
    print(etq_sql(qyr_not_in_list_2(vTPP15, vTPP16, vTPP17)))
    df18l2=spark.sql(qyr_not_in_list_2(vTPP15, vTPP16, vTPP17))
    df18l2.printSchema()
    df18l2.write.mode('overwrite').saveAsTable(vSSchHiveTmp+".otc_t_pivot_1812")
    
    print(lne_dvs())
    print(etq_sql(qyr_otc_t_360_parque_1_tmp_all_2(vTPP19, vIFechaProceso,vSSchHiveTmp+".otc_t_pivot_1812")))
    df18b=spark.sql(qyr_otc_t_360_parque_1_tmp_all_2(vTPP19, vIFechaProceso,vSSchHiveTmp+".otc_t_pivot_1812"))
    df18b.printSchema()
    print(etq_info("Guardando tabla tmp: (otc_t_pivot_18b)"))
    df18b.write.mode('overwrite').saveAsTable(vSSchHiveTmp+".otc_t_pivot_18b") 
    
    if (df18.rdd.isEmpty()) | (df18a.rdd.isEmpty()) | (df18b.rdd.isEmpty()) | (df18l1.rdd.isEmpty()) | (df18l2.rdd.isEmpty()):
        exit(etq_nodata(msg_e_df_nodata(str('df18'))))
    else:
        try:
            ts_step_tbl = datetime.now()            
            #not_in_list_1 = list(df18l1.select('num_telefonico').toPandas()['num_telefonico'])
            #print(len(not_in_list_1))
            #not_in_list_2 = list(df18l2.select('num_telefonico').toPandas()['num_telefonico'])
            #print(len(not_in_list_2))
            #df18a=df18a.filter(df18a.num_telefonico.isin(not_in_list_1))
                  
            
            #df18b=df18b.filter(df18b.num_telefonico.isin(not_in_list_2))
                       
            print(etq_info("Guardando tabla tmp: (otc_t_pivot_18)"))
            df18.write.mode('overwrite').saveAsTable(vSSchHiveTmp+".otc_t_pivot_18")            
            print(etq_info(msg_i_insert_hive(vTPP18)))
            df18final=spark.sql(qyr_union_pivot(vSSchHiveTmp+".otc_t_pivot_18",vSSchHiveTmp+".otc_t_pivot_18a",vSSchHiveTmp+".otc_t_pivot_18b"))
            
            print('Inicia el overwrite de la tabla [{}] ').format(vTPP18)
            df18final.write.mode('overwrite').saveAsTable(vTPP18)
            df18final.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP18,str(df18final.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP18,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP18,str(e))))
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
    print(etq_info(msg_i_create_hive_tmp(vTPP20)))
    print(etq_sql(qyr_otc_t_360_parque_1_tmp(vTPP18, vTPP09)))
    df20=spark.sql(qyr_otc_t_360_parque_1_tmp(vTPP18, vTPP09))
    if df20.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df20'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP20)))
            df20.write.mode('overwrite').saveAsTable(vTPP20)
            df20.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP20,str(df20.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP20,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP20,str(e))))
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
