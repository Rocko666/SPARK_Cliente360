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
from otc_t_360_movimientos_parque_config import *
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/query')
from otc_t_360_movimientos_parque_query import *
# Genericos otc_t_360_movimientos_parque
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
    parser.add_argument('--vSchTmp', required=True, type=str)
    parser.add_argument('--vSchRep', required=True, type=str)
    parser.add_argument('--vTAltBajHist', required=True, type=str)
    parser.add_argument('--vTAltBI', required=True, type=str)
    parser.add_argument('--vTBajBI', required=True, type=str)
    parser.add_argument('--vTTransfHist', required=True, type=str)
    parser.add_argument('--vTTrInBI', required=True, type=str)
    parser.add_argument('--vTTrOutBI', required=True, type=str)
    parser.add_argument('--vTCPHist', required=True, type=str)
    parser.add_argument('--vTCPBI', required=True, type=str)
    parser.add_argument('--vTNRHist', required=True, type=str)
    parser.add_argument('--vTNRCSA', required=True, type=str)
    parser.add_argument('--vTABRHist', required=True, type=str)
    parser.add_argument('--vTCatPosUsr', required=True, type=str)
    parser.add_argument('--vTPivotParq', required=True, type=str)
    parser.add_argument('--f_inicio', required=True, type=str)
    parser.add_argument('--fecha_proceso', required=True, type=str)
    parser.add_argument('--fecha_movimientos', required=True, type=str)
    parser.add_argument('--fecha_movimientos_cp', required=True, type=str)
    parser.add_argument('--fecha_mes_ant_cp', required=True, type=str)
    parser.add_argument('--fecha_mes_ant', required=True, type=str)
    parser.add_argument('--f_inicio_abr', required=True, type=str)
    parser.add_argument('--f_fin_abr', required=True, type=str)
    parser.add_argument('--f_efectiva', required=True, type=str)
    
    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vSchTmp=parametros.vSchTmp
    vSchRep=parametros.vSchRep
    vTAltBajHist=parametros.vTAltBajHist
    vTAltBI=parametros.vTAltBI
    vTBajBI=parametros.vTBajBI
    vTTransfHist=parametros.vTTransfHist
    vTTrInBI=parametros.vTTrInBI
    vTTrOutBI=parametros.vTTrOutBI
    vTCPHist=parametros.vTCPHist
    vTCPBI=parametros.vTCPBI
    vTNRHist=parametros.vTNRHist
    vTNRCSA=parametros.vTNRCSA
    vTABRHist=parametros.vTABRHist
    vTCatPosUsr=parametros.vTCatPosUsr
    vTPivotParq=parametros.vTPivotParq
    f_inicio=parametros.f_inicio
    fecha_proceso=parametros.fecha_proceso
    fecha_movimientos=parametros.fecha_movimientos
    fecha_movimientos_cp=parametros.fecha_movimientos_cp
    fecha_mes_ant_cp=parametros.fecha_mes_ant_cp
    fecha_mes_ant=parametros.fecha_mes_ant
    f_inicio_abr=parametros.f_inicio_abr
    f_fin_abr=parametros.f_fin_abr
    f_efectiva=parametros.f_efectiva
    
    print(etq_info(log_p_parametros("vSEntidad",vSEntidad)))
    print(etq_info(log_p_parametros("vSchTmp",vSchTmp)))
    print(etq_info(log_p_parametros("vSchTmp",vSchRep)))
    print(etq_info(log_p_parametros("vTAltBajHist",vTAltBajHist)))
    print(etq_info(log_p_parametros("vTAltBI",vTAltBI)))
    print(etq_info(log_p_parametros("vTBajBI",vTBajBI)))
    print(etq_info(log_p_parametros("vTTransfHist",vTTransfHist)))
    print(etq_info(log_p_parametros("vTTrInBI",vTTrInBI)))
    print(etq_info(log_p_parametros("vTTrOutBI",vTTrOutBI)))
    print(etq_info(log_p_parametros("vTCPHist",vTCPHist)))
    print(etq_info(log_p_parametros("vTCPBI",vTCPBI)))
    print(etq_info(log_p_parametros("vTNRHist",vTNRHist)))
    print(etq_info(log_p_parametros("vTNRCSA",vTNRCSA)))
    print(etq_info(log_p_parametros("vTABRHist",vTABRHist)))
    print(etq_info(log_p_parametros("vTCatPosUsr",vTCatPosUsr)))
    print(etq_info(log_p_parametros("vTPivotParq",vTPivotParq)))
    print(etq_info(log_p_parametros("f_inicio",f_inicio)))
    print(etq_info(log_p_parametros("fecha_proceso",fecha_proceso)))
    print(etq_info(log_p_parametros("fecha_movimientos",fecha_movimientos)))
    print(etq_info(log_p_parametros("fecha_movimientos_cp",fecha_movimientos_cp)))
    print(etq_info(log_p_parametros("fecha_mes_ant_cp",fecha_mes_ant_cp)))
    print(etq_info(log_p_parametros("fecha_mes_ant",fecha_mes_ant)))
    print(etq_info(log_p_parametros("f_inicio_abr",f_inicio_abr)))
    print(etq_info(log_p_parametros("f_fin_abr",f_fin_abr)))
    print(etq_info(log_p_parametros("f_efectiva",f_efectiva)))
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
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_14(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_17(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_18(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_19(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_20(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_21(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_22(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_23(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_24(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_25(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_26(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_27(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_28(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_29(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_30(vSchTmp)))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_14(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_17(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_18(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_19(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_20(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_21(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_22(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_23(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_24(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_25(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_26(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_27(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_28(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_29(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_30(vSchTmp))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4]: Generando logica de negocio '
print(etq_info(VStp))
print(lne_dvs())
VStp='Paso [4.1]: Se obtienen las altas del mes en la tabla {} '.format(vTAltBajHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_delete_table(vTAltBajHist)))
    print(etq_sql(qry_dlt_otc_t_alta_baja_hist_alta(vTAltBajHist, f_inicio, fecha_proceso)))
    spark.sql(qry_dlt_otc_t_alta_baja_hist_alta(vTAltBajHist, f_inicio, fecha_proceso))
    print(etq_info(msg_i_insert_hive(vTAltBajHist)))
    print(etq_sql(qry_insrt_otc_t_alta_baja_hist_alta(vTAltBI, fecha_movimientos_cp)))
    df01=spark.sql(qry_insrt_otc_t_alta_baja_hist_alta(vTAltBI, fecha_movimientos_cp))
    if df01.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df01'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTAltBajHist)))
            df01.repartition(1).write.mode('append').saveAsTable(vTAltBajHist)
            df01.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTAltBajHist,str(df01.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTAltBajHist,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTAltBajHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.2]: Se inserta las bajas del mes en la tabla {} '.format(vTAltBajHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_delete_table(vTAltBajHist)))
    print(etq_sql(qry_dlt_otc_t_alta_baja_hist_baja(vTAltBajHist, f_inicio, fecha_proceso)))
    spark.sql(qry_dlt_otc_t_alta_baja_hist_baja(vTAltBajHist, f_inicio, fecha_proceso))
    print(etq_info(msg_i_insert_hive(vTAltBajHist)))
    print(etq_sql(qry_insrt_otc_t_alta_baja_hist_baja(vTBajBI, fecha_movimientos_cp)))
    df02=spark.sql(qry_insrt_otc_t_alta_baja_hist_baja(vTBajBI, fecha_movimientos_cp))
    if df02.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df02'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTAltBajHist)))
            df02.repartition(1).write.mode('append').saveAsTable(vTAltBajHist)
            df02.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTAltBajHist,str(df02.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTAltBajHist,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTAltBajHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [4.5]: Se eliminan los transfer_in pre existentes del mes que se procesa de la tabla {} '.format(vTTransfHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_select_query(vTTransfHist)))
    print(etq_sql(qry_dlt_otc_t_transfer_hist_pre_pos(vTTransfHist, f_inicio, fecha_proceso)))
    try:
        ts_step_tbl = datetime.now()
        spark.sql(qry_dlt_otc_t_transfer_hist_pre_pos(vTTransfHist, f_inicio, fecha_proceso))
        print(etq_info(msg_i_delete_table(vTTransfHist))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(vTTransfHist,vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_ejecucion(vTTransfHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.6]: Se insertan las los transfer_in del mes en la tabla {} '.format(vTTransfHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTTransfHist)))
    print(etq_sql(qry_insrt_otc_t_transfer_hist_pre_pos(vTTransfHist, vTTrInBI, fecha_movimientos_cp)))
    try:
        ts_step_tbl = datetime.now()
        spark.sql(qry_insrt_otc_t_transfer_hist_pre_pos(vTTransfHist, vTTrInBI, fecha_movimientos_cp))
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(vTTransfHist,vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(vTTransfHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [4.7]: Se eliminan los transfer_out pre existentes del mes que se procesa de la tabla {} '.format(vTTransfHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_select_query(vTTransfHist)))
    print(etq_sql(qry_dlt_otc_t_transfer_hist_pos_pre(vTTransfHist, f_inicio, fecha_proceso)))
    try:
        ts_step_tbl = datetime.now()
        spark.sql(qry_dlt_otc_t_transfer_hist_pos_pre(vTTransfHist, f_inicio, fecha_proceso))
        print(etq_info(msg_i_delete_table(vTTransfHist))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(vTTransfHist,vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_ejecucion(vTTransfHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.8]: Se insertan las los transfer_out del mes en la tabla {} '.format(vTTransfHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTTransfHist)))
    print(etq_sql(qry_insrt_otc_t_transfer_hist_pos_pre(vTTransfHist, vTTrOutBI, fecha_movimientos_cp)))
    try:
        ts_step_tbl = datetime.now()
        spark.sql(qry_insrt_otc_t_transfer_hist_pos_pre(vTTransfHist, vTTrOutBI, fecha_movimientos_cp))
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(vTTransfHist,vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(vTTransfHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [4.9]: Se eliminan los cambios de plan pre existentes del mes que se procesa de la tabla {} '.format(vTCPHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_select_query(vTCPHist)))
    print(etq_sql(qry_dlt_otc_t_cambio_plan_hist(vTCPHist, f_inicio, fecha_proceso)))
    try:
        ts_step_tbl = datetime.now()
        spark.sql(qry_dlt_otc_t_cambio_plan_hist(vTCPHist, f_inicio, fecha_proceso))
        print(etq_info(msg_i_delete_table(vTCPHist))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(vTCPHist,vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_ejecucion(vTCPHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.10]: Se insertan las los cambios de plan del mes en la tabla {} '.format(vTCPHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTCPHist)))
    print(etq_sql(qry_insrt_otc_t_cambio_plan_hist(vTCPHist, vTCPBI, fecha_movimientos_cp)))
    try:
        ts_step_tbl = datetime.now()
        spark.sql(qry_insrt_otc_t_cambio_plan_hist(vTCPHist, vTCPBI, fecha_movimientos_cp))
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(vTCPHist,vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(vTCPHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [4.11]: Se eliminan los no_reciclables pre existentes del mes que se procesa de la tabla {} '.format(vTNRHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_select_query(vTNRHist)))
    print(etq_sql(qry_dlt_otc_t_no_reciclable_hist(vTNRHist, fecha_movimientos)))
    try:
        ts_step_tbl = datetime.now()
        spark.sql(qry_dlt_otc_t_no_reciclable_hist(vTNRHist, fecha_movimientos))
        print(etq_info(msg_i_delete_table(vTNRHist))) 
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(vTNRHist,vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_ejecucion(vTNRHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.12]: Se insertan las los no_reciclables del mes en la tabla {} '.format(vTNRHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTNRHist)))
    print(etq_sql(qry_insrt_otc_t_no_reciclable_hist(vTNRHist, vTNRCSA, fecha_movimientos)))
    try:
        ts_step_tbl = datetime.now()
        spark.sql(qry_insrt_otc_t_no_reciclable_hist(vTNRHist, vTNRCSA, fecha_movimientos))
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(vTNRHist,vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:       
        exit(etq_error(msg_e_insert_hive(vTNRHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.13]: Se almacena las altas de todos los dias (particiones) para el mes de analisis '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)))))
    print(etq_sql(qry_tmp_altas_ttls_mes(vTAltBI, f_inicio_abr, f_fin_abr)))
    df13=spark.sql(qry_tmp_altas_ttls_mes(vTAltBI, f_inicio_abr, f_fin_abr))
    if df13.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df13'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)))))
            df13.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)))
            df13.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)),str(df13.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.14]: Se almacenan los atributos de los movimientos:altas, transfer in, transfer out, cambios de plan,\n'+
'se tendra una informacion real a mes caido ej: yyyymm01, en la tabla {}'.format(str(nme_tbl_otc_t_360_movimientos_parque_14(vSchTmp)))
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_movimientos_parque_14(vSchTmp)))))
    print(etq_sql(qry_tmp_movs_efctvs(vTAltBI, vTTrInBI, vTTrOutBI, vTCPBI, fecha_movimientos_cp)))
    df14=spark.sql(qry_tmp_movs_efctvs(vTAltBI, vTTrInBI, vTTrOutBI, vTCPBI, fecha_movimientos_cp))
    if df14.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df14'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_14(vSchTmp)))))
            df14.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_movimientos_parque_14(vSchTmp)))
            df14.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_movimientos_parque_14(vSchTmp)),str(df14.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_movimientos_parque_14(vSchTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_14(vSchTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.15]: Se obtienen las ALTAS BAJAS REPROCESO '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_delete_table(vTABRHist)))
    print(etq_sql(qry_dlt_otc_t_alta_baja_reproceso_hist(vTABRHist, f_inicio, fecha_proceso)))
    spark.sql(qry_dlt_otc_t_alta_baja_reproceso_hist(vTABRHist, f_inicio, fecha_proceso))
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_movimientos_parque_16(vSchTmp)))))
    print(etq_sql(qry_insrt_otc_t_alta_baja_reproceso_hist(vTAltBI, f_inicio_abr, f_fin_abr)))
    df16=spark.sql(qry_insrt_otc_t_alta_baja_reproceso_hist(vTAltBI, f_inicio_abr, f_fin_abr))
    if df16.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df16'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_16(vSchTmp)))))
            df16.repartition(1).write.mode('append').saveAsTable(str(nme_tbl_otc_t_360_movimientos_parque_16(vSchTmp)))
            df16.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_movimientos_parque_16(vSchTmp)),str(df16.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_movimientos_parque_16(vSchTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_16(vSchTmp)),str(e))))
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
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_movimientos_parque_16(vSchTmp)))))
    print(etq_sql(qry_tmp_360_otc_t_paquetes_payment_combo_menos30(vTAltBI, f_inicio_abr, f_fin_abr)))
    df16=spark.sql(qry_tmp_360_otc_t_paquetes_payment_combo_menos30(vTAltBI, f_inicio_abr, f_fin_abr))
    if df16.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df16'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_16(vSchTmp)))))
            df16.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_movimientos_parque_16(vSchTmp)))
            df16.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_movimientos_parque_16(vSchTmp)),str(df16.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_movimientos_parque_16(vSchTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_16(vSchTmp)),str(e))))
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
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_movimientos_parque_17(vSchTmp)))))
    print(etq_sql(qry_tmp_otc_t_360_movimientos_parque(str(nme_tbl_otc_t_360_movimientos_parque_04(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_10(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_05(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_07(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_08(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_09(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_11(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_14(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_12(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_15(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_06(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_16(vSchTmp)))))
    df17=spark.sql(qry_tmp_otc_t_360_movimientos_parque(str(nme_tbl_otc_t_360_movimientos_parque_04(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_10(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_05(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_07(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_08(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_09(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_11(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_14(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_12(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_15(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_06(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)), str(nme_tbl_otc_t_360_movimientos_parque_16(vSchTmp))))
    if df17.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df17'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_17(vSchTmp)))))
            df17.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_movimientos_parque_17(vSchTmp)))
            df17.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_movimientos_parque_17(vSchTmp)),str(df17.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_movimientos_parque_17(vSchTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_17(vSchTmp)),str(e))))
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