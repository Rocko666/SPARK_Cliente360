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
from pyspark_llap import HiveWarehouseSession
import argparse
import time
import sys
import os

# Genericos otc_t_360_movimientos_parque
sys.path.insert(1,'/var/opt/tel_spark')
from messages import *
from functions import *

timestart = datetime.now()
#Parametros definidos
VStp='[Paso inicial]: Cargando parametros desde la Shell:'
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
    print(lne_dvs())
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

VStp='Paso [1]: Configuracion del Spark Session:'
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
        .config("hive.enforce.bucketing", "false")\
	    .config("hive.enforce.sorting", "false")\
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    hive_hwc = HiveWarehouseSession.session(spark).build()
    app_id = spark._sc.applicationId
    print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [2]: Cargando configuraciones y nombre de tablas:'

try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs()) 
    print(etq_info("Inicio del proceso en PySpark...")) 
    print(lne_dvs())
    print(etq_info("Importando librerias personalizadas..."))
    sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Configuraciones')
    from otc_t_360_movimientos_parque_config import *
    sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Querys')
    from otc_t_360_movimientos_parque_query import *
    print(lne_dvs())
    print(etq_info("Tablas termporales del proceso..."))
    print(lne_dvs())
    vTMP06=nme_tbl_otc_t_360_movimientos_parque_06(vSchTmp)
    vTMP07=nme_tbl_otc_t_360_movimientos_parque_07(vSchTmp)
    vTMP08=nme_tbl_otc_t_360_movimientos_parque_08(vSchTmp)
    vTMP09=nme_tbl_otc_t_360_movimientos_parque_09(vSchTmp)
    vTMP10=nme_tbl_otc_t_360_movimientos_parque_10(vSchTmp)
    vTMP11=nme_tbl_otc_t_360_movimientos_parque_11(vSchTmp)
    vTMP12=nme_tbl_otc_t_360_movimientos_parque_12(vSchTmp)
    vTMP13=nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)
    vTMP14=nme_tbl_otc_t_360_movimientos_parque_14(vSchTmp)
    print(etq_info(log_p_parametros('vTMP06',vTMP06)))
    print(etq_info(log_p_parametros('vTMP07',vTMP07)))
    print(etq_info(log_p_parametros('vTMP08',vTMP08)))
    print(etq_info(log_p_parametros('vTMP09',vTMP09)))
    print(etq_info(log_p_parametros('vTMP10',vTMP10)))
    print(etq_info(log_p_parametros('vTMP11',vTMP11)))
    print(etq_info(log_p_parametros('vTMP12',vTMP12)))
    print(etq_info(log_p_parametros('vTMP13',vTMP13)))
    print(etq_info(log_p_parametros('vTMP14',vTMP14)))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [3]: Generando logica de negocio '
print(etq_info(VStp))

print(lne_dvs())
vStp='Paso [3.01]: Se inserta las altas del mes de la tabla [{}] en la tabla [{}] '.format(vTAltBI, vTAltBajHist)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("Se eliminan las altas  preexistentes de la tabla[{}]".format(vTAltBajHist)))
    VSQLdelete=qry_dlt_otc_t_abh_alta(vTAltBajHist, f_inicio, fecha_proceso)
    print(etq_sql(VSQLdelete))
    hive_hwc.executeUpdate(VSQLdelete)
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTAltBajHist)))
    VSQLinsrt=qry_insrt_otc_t_abh_alta(vTAltBajHist, vTAltBI, fecha_movimientos_cp)
    hive_hwc.executeUpdate(VSQLinsrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))


print(lne_dvs())
vStp='Paso [3.02]: Se inserta las bajas del mes de la tabla [{}] en la tabla [{}] '.format(vTBajBI, vTAltBajHist)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("Se eliminan las bajas  preexistentes de la tabla[{}]".format(vTAltBajHist)))
    VSQLdelete=qry_dlt_otc_t_abh_baja(vTAltBajHist, f_inicio, fecha_proceso)
    print(etq_sql(VSQLdelete))
    hive_hwc.executeUpdate(VSQLdelete)
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTAltBajHist)))
    VSQLinsrt=qry_insrt_otc_t_abh_baja(vTAltBajHist, vTBajBI, fecha_movimientos_cp)
    hive_hwc.executeUpdate(VSQLinsrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))


print(lne_dvs())
vStp='Paso [3.03]: Se inserta los transfer_in del mes de la tabla [{}] en la tabla [{}] '.format(vTTrInBI, vTTransfHist)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("Se eliminan los transfer_in  preexistentes de la tabla[{}]".format(vTTransfHist)))
    VSQLdelete=qry_dlt_otc_t_th_pre_pos(vTTransfHist, f_inicio, fecha_proceso)
    print(etq_sql(VSQLdelete))
    hive_hwc.executeUpdate(VSQLdelete)
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTTransfHist)))
    VSQLinsrt=qry_insrt_otc_t_th_pre_pos(vTTransfHist, vTTrInBI, fecha_movimientos_cp)
    hive_hwc.executeUpdate(VSQLinsrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))
    

print(lne_dvs())
vStp='Paso [3.04]: Se inserta los transfer_out del mes de la tabla [{}] en la tabla [{}] '.format(vTTrOutBI, vTTransfHist)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("Se eliminan los transfer_out preexistentes de la tabla[{}]".format(vTTransfHist)))
    VSQLdelete=qry_dlt_otc_t_th_pos_pre(vTTransfHist, f_inicio, fecha_proceso)
    print(etq_sql(VSQLdelete))
    hive_hwc.executeUpdate(VSQLdelete)
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTTransfHist)))
    VSQLinsrt=qry_insrt_otc_t_th_pos_pre(vTTransfHist, vTTrOutBI, fecha_movimientos_cp)
    hive_hwc.executeUpdate(VSQLinsrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))
    

print(lne_dvs())
vStp='Paso [3.05]: Se inserta los cambios de plan del mes de la tabla [{}] en la tabla [{}] '.format(vTCPBI, vTCPHist)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("Se eliminan los cambios de plan preexistentes de la tabla[{}]".format(vTCPHist)))
    VSQLdelete=qry_dlt_otc_t_cph(vTCPHist, f_inicio, fecha_proceso)
    print(etq_sql(VSQLdelete))
    hive_hwc.executeUpdate(VSQLdelete)
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTCPHist)))
    VSQLinsrt=qry_insrt_otc_t_cph(vTCPHist, vTCPBI, fecha_movimientos_cp)
    hive_hwc.executeUpdate(VSQLinsrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))
    

print(lne_dvs())
VStp='Paso [3.06]: Se obtiene el ultimo evento del alta en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP06)))
    vSQL=qry_otc_t_alta_hist_unic(vTAltBajHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df06=spark.sql(vSQL)
    if df06.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df06'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP06)))
            df06.repartition(1).write.mode('overwrite').saveAsTable(vTMP06)
            df06.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP06,str(df06.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP06,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP06,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df06')))
    del df06
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.07]: Se obtiene el ultimo evento de las bajas en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP07)))
    vSQL=qry_otc_t_baja_hist_unic(vTAltBajHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df07=spark.sql(vSQL)
    if df07.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df07'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP07)))
            df07.repartition(1).write.mode('overwrite').saveAsTable(vTMP07)
            df07.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP07,str(df07.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP07,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP07,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df07')))
    del df07
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.08]: Se obtiene el ultimo evento de las transferencias out en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP08)))
    vSQL=qry_otc_t_pos_pre_hist_unic(vTTransfHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df08=spark.sql(vSQL)
    if df08.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df08'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP08)))
            df08.repartition(1).write.mode('overwrite').saveAsTable(vTMP08)
            df08.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP08,str(df08.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP08,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP08,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df08')))
    del df08
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.09]: Se obtiene el ultimo evento de las transferencias in  en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP09)))
    vSQL=qry_otc_t_pre_pos_hist_unic(vTTransfHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df09=spark.sql(vSQL)
    if df09.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df09'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP09)))
            df09.repartition(1).write.mode('overwrite').saveAsTable(vTMP09)
            df09.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP09,str(df09.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP09,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP09,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df09')))
    del df09
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.10]: Se obtiene el ultimo evento de los cambios de plan en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP10)))
    vSQL=qry_otc_t_cambio_plan_hist_unic(vTCPHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df10=spark.sql(vSQL)
    if df10.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df10'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP10)))
            df10.repartition(1).write.mode('overwrite').saveAsTable(vTMP10)
            df10.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP10,str(df10.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP10,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP10,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df06')))
    del df10
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.11]: Se realiza el cruce con cada tabla usando [{}] (tabla resultante de pivot_parque) y agregando los campos de cada tabla renombrandolos de acuerdo al movimiento que corresponda'.format(vTPivotParq)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP11)))
    vSQL=qry_otc_t_360_parque_1_tmp_t_mov(vTPivotParq, vTMP06, vTMP08, vTMP09, vTMP10)
    print(etq_sql(vSQL))
    df11=spark.sql(vSQL)
    if df11.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df11'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP11)))
            df11.repartition(1).write.mode('overwrite').saveAsTable(vTMP11)
            df11.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP11,str(df11.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP11,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP11,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df11')))
    del df11
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.12]: Se crea la tabla temp union para obtener ultimo movimiento del mes por num_telefono'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP12)))
    vSQL=qry_otc_t_360_parque_1_mov_mes_tmp(f_inicio, fecha_proceso, vTMP06, vTMP07, vTMP08, vTMP09, vTMP10)
    print(etq_sql(vSQL))
    df12=spark.sql(vSQL)
    if df12.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df12'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP12)))
            df12.repartition(1).write.mode('overwrite').saveAsTable(vTMP12)
            df12.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP12,str(df12.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP12,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP12,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df12')))
    del df12
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.13]: Se crea la tabla para segmentos'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP13)))
    vSQL=qry_otc_t_360_parque_1_mov_seg_tmp(vTMP06, vTMP08, vTMP09)
    print(etq_sql(vSQL))
    df13=spark.sql(vSQL)
    if df13.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df13'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP13)))
            df13.repartition(1).write.mode('overwrite').saveAsTable(vTMP13)
            df13.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP13,str(df13.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP13,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP13,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df13')))
    del df13
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.14]: Se crea la ultima tabla del proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP14)))
    vSQL=qry_otc_t_360_parque_1_tmp_t_mov_mes(vTPivotParq, vTMP12)
    print(etq_sql(vSQL))
    df14=spark.sql(vSQL)
    if df14.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df14'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP14)))
            df14.repartition(1).write.mode('overwrite').saveAsTable(vTMP14)
            df14.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP14,str(df14.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP14,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP14,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df14')))
    del df14
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    

print(lne_dvs())
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())