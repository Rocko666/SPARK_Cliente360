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
from otc_t_360_pivot_parque_config import *
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/query')
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
    parser.add_argument('--vTAltasBi', required=True, type=str)
    parser.add_argument('--vTTransferOutBi', required=True, type=str)
    parser.add_argument('--vTTransferInBi', required=True, type=str)
    parser.add_argument('--vTCPBi', required=True, type=str)
    parser.add_argument('--vTBajasInv', required=True, type=str)
    parser.add_argument('--vTChurnSP2', required=True, type=str)
    parser.add_argument('--vTCFact', required=True, type=str)
    parser.add_argument('--vTPRMANDATE', required=True, type=str)
    parser.add_argument('--vSSchHiveMain', required=True, type=str)
    parser.add_argument('--vSSchHiveTmp', required=True, type=str)
    parser.add_argument('--vSTblHiveMain', required=True, type=str)
    parser.add_argument('--fec_alt_ini', required=True, type=str)
    parser.add_argument('--fec_alt_fin', required=True, type=str)
    parser.add_argument('--fec_eje_pv', required=True, type=int)
    parser.add_argument('--fec_proc', required=True, type=int)
    parser.add_argument('--fec_menos_5', required=True, type=int)
    parser.add_argument('--fec_mas_1', required=True, type=int)
    parser.add_argument('--fec_alt_dos_meses_ant_fin', required=True, type=str)
    parser.add_argument('--fec_alt_dos_meses_ant_ini', required=True, type=str)
    parser.add_argument('--fec_ini_mes', required=True, type=int)
    parser.add_argument('--fec_inac_1', required=True, type=int)
    parser.add_argument('--fechaeje1', required=True, type=str)
    parser.add_argument('--vAbrev', required=True, type=str)
    parser.add_argument('--vIFechaProceso', required=True, type=str)
    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vTAltasBi=parametros.vTAltasBi
    vTTransferOutBi=parametros.vTTransferOutBi
    vTTransferInBi=parametros.vTTransferInBi
    vTCPBi=parametros.vTCPBi
    vTBajasInv=parametros.vTBajasInv
    vTChurnSP2=parametros.vTChurnSP2
    vTCFact=parametros.vTCFact
    vTPRMANDATE=parametros.vTPRMANDATE
    vSSchHiveMain=parametros.vSSchHiveMain
    vSSchHiveTmp=parametros.vSSchHiveTmp
    vSTblHiveMain=parametros.vSTblHiveMain
    fec_alt_ini=parametros.fec_alt_ini
    fec_alt_fin=parametros.fec_alt_fin
    fec_eje_pv=parametros.fec_eje_pv
    fec_proc=parametros.fec_proc
    fec_menos_5=parametros.fec_menos_5
    fec_mas_1=parametros.fec_mas_1
    fec_alt_dos_meses_ant_fin=parametros.fec_alt_dos_meses_ant_fin
    fec_alt_dos_meses_ant_ini=parametros.fec_alt_dos_meses_ant_ini
    fec_ini_mes=parametros.fec_ini_mes
    fec_inac_1=parametros.fec_inac_1
    fechaeje1=parametros.fechaeje1
    vAbrev=parametros.vAbrev 
    vIFechaProceso=parametros.vIFechaProceso 
    print(etq_info(log_p_parametros("vSEntidad",vSEntidad)))
    print(etq_info(log_p_parametros("vTAltasBi",vTAltasBi)))
    print(etq_info(log_p_parametros("vTTransferOutBi",vTTransferOutBi)))
    print(etq_info(log_p_parametros("vTTransferInBi",vTTransferInBi)))
    print(etq_info(log_p_parametros("vTCPBi",vTCPBi)))
    print(etq_info(log_p_parametros("vTBajasInv",vTBajasInv)))
    print(etq_info(log_p_parametros("vTChurnSP2",vTChurnSP2)))
    print(etq_info(log_p_parametros("vTCFact",vTCFact)))
    print(etq_info(log_p_parametros("vTPRMANDATE",vTPRMANDATE)))
    print(etq_info(log_p_parametros("vSSchHiveMain",vSSchHiveMain)))
    print(etq_info(log_p_parametros("vSSchHiveTmp",vSSchHiveTmp)))
    print(etq_info(log_p_parametros("vSTblHiveMain",vSTblHiveMain)))
    print(etq_info(log_p_parametros("fec_alt_ini",fec_alt_ini)))
    print(etq_info(log_p_parametros("fec_alt_fin",fec_alt_fin)))
    print(etq_info(log_p_parametros("fec_eje_pv",fec_eje_pv)))
    print(etq_info(log_p_parametros("fec_proc",fec_proc)))
    print(etq_info(log_p_parametros("fec_menos_5",fec_menos_5)))
    print(etq_info(log_p_parametros("fec_mas_1",fec_mas_1)))
    print(etq_info(log_p_parametros("fec_alt_dos_meses_ant_fin",fec_alt_dos_meses_ant_fin)))
    print(etq_info(log_p_parametros("fec_alt_dos_meses_ant_ini",fec_alt_dos_meses_ant_ini)))
    print(etq_info(log_p_parametros("fec_ini_mes",fec_ini_mes)))
    print(etq_info(log_p_parametros("fec_inac_1",fec_inac_1)))
    print(etq_info(log_p_parametros("fechaeje1",fechaeje1)))
    print(etq_info(log_p_parametros("vAbrev",vAbrev)))
    print(etq_info(log_p_parametros("vIFechaProceso",vIFechaProceso)))
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
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_pivot_parque_20(vSSchHiveTmp, vAbrev)))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_pivot_parque_20(vSSchHiveTmp, vAbrev))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
## N15
VStp='Paso [4]: Generando logica de negocio '
print(etq_info(VStp))
print(lne_dvs())
VStp='Paso [4.1]: Se obtiene el parque activo y se lo guarda en la tabla {} '.format()
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev)))))
    print(etq_sql(qyr_tmp_360_otc_t_parque_act(vTParq2, vTPP1, vTPP4, vTPP5, vTPP6, vTPP7, vTPP2, vTPP3)))
    df15=spark.sql(qyr_tmp_360_otc_t_parque_act(vTParq2, vTPP1, vTPP4, vTPP5, vTPP6, vTPP7, vTPP2, vTPP3))
    if df15.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df15'))))
    else:
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
    print(etq_sql(qyr_tmp_360_otc_t_parque_inact(vTPP13, vTPP10, vTPP02, vTPP03)))
    df16=spark.sql(qyr_tmp_360_otc_t_parque_inact(vTPP13, vTPP10, vTPP02, vTPP03))
    if df16.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df16'))))
    else:
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
    if df17.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df17'))))
    else:
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
VStp='Paso [4.4]: Se obtiene la primera tabla temporal con todo el parque {}'.format(vTCPBi)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev)))))
    print(etq_sql(qyr_otc_t_360_parque_1_tmp_all(vTPP15, vTPP16, vTPP17, vTPP19, vIFechaProceso)))
    df18=spark.sql(qyr_otc_t_360_parque_1_tmp_all(vTPP15, vTPP16, vTPP17, vTPP19, vIFechaProceso))
    if df18.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df18'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev)))))
            df18.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev)))
            df18.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev)),str(df18.count())))) 
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
VStp='Paso [4.5]: Se obtiene la TABLA FINAL de este proceso '.format(vTCPBi)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_pivot_parque_20(vSSchHiveTmp)))))
    print(etq_sql(qyr_otc_t_360_parque_1_tmp(vTPP18, vTPP09)))
    df20=spark.sql(qyr_otc_t_360_parque_1_tmp(vTPP18, vTPP09))
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