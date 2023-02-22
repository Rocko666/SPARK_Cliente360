from datetime import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType, DoubleType, DateType, StringType, FloatType, TimestampType, StructField, IntegerType, BooleanType
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import concat
from pyspark.sql.functions import to_date
from pyspark_llap.sql.session import HiveWarehouseSession
from pyspark import SQLContext
import argparse
import time
import sys
import os
# General cliente 360
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Configuraciones')
from otc_t_360_movimientos_parque_config import *
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Querys')
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
    #hive_hws = HiveWarehouseSession.session(spark).build()
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
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_06(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_07(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_08(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_09(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_10(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_11(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_12(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_movimientos_parque_14(vSchTmp)))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_06(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_07(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_08(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_09(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_10(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_11(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_12(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp))))
    print(etq_info(str(nme_tbl_otc_t_360_movimientos_parque_14(vSchTmp))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4]: Generando logica de negocio '
print(etq_info(VStp))
print(lne_dvs())
VStp='Paso [4.1]: Se inserta las altas del mes de la tabla {} en la tabla {} '.format(vTAltBI, vTAltBajHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_select_query(vTAltBajHist)))
    print(etq_sql(qry_ori_otc_t_alta_baja_hist(vTAltBajHist)))
    df_altas_hist_original=spark.sql(qry_ori_otc_t_alta_baja_hist(vTAltBajHist))
    df_altas_hist_original.printSchema()
    rows_to_delete=df_altas_hist_original.filter((F.upper(col('TIPO'))=='ALTA') & (col('FECHA').between(f_inicio,fecha_proceso)))
    df_altas_hist_original=df_altas_hist_original.join(rows_to_delete, on=['TIPO','FECHA'], how='left_anti')
    print(etq_info(msg_i_insert_hive(vTAltBajHist)))
    print(etq_sql(qry_insrt_otc_t_alta_baja_hist_alta(vTAltBI, fecha_movimientos_cp)))
    df01Insert=spark.sql(qry_insrt_otc_t_alta_baja_hist_alta(vTAltBI, fecha_movimientos_cp))
    df01Insert.printSchema()
    df_altas_hist=df_altas_hist_original.union(df01Insert)
    if df_altas_hist.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_altas_hist'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_create_hive_tmp(vTAltBajHist)))
            df_altas_hist.repartition(1).write.mode('overwrite').saveAsTable(vTAltBajHist)
            df_altas_hist.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTAltBajHist,str(df_altas_hist.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTAltBajHist,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTAltBajHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [4.2]: Se inserta las bajas del mes de la tabla {} en la tabla {} '.format(vTBajBI, vTAltBajHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_select_query(vTAltBajHist)))
    print(etq_sql(qry_ori_otc_t_alta_baja_hist(vTAltBajHist)))
    df_bajas_hist_original=spark.sql(qry_ori_otc_t_alta_baja_hist(vTAltBajHist))
    df_bajas_hist_original.printSchema()
    rows_to_delete=df_bajas_hist_original.filter((F.upper(col('TIPO'))=='BAJA') & (col('FECHA').between(f_inicio, fecha_proceso)))
    df_bajas_hist_original=df_bajas_hist_original.join(rows_to_delete, on=['TIPO','FECHA'], how='left_anti')
    print(etq_info(msg_i_insert_hive(vTAltBajHist)))
    print(etq_sql(qry_insrt_otc_t_alta_baja_hist_baja(vTBajBI, fecha_movimientos_cp)))
    df02Insert=spark.sql(qry_insrt_otc_t_alta_baja_hist_baja(vTBajBI, fecha_movimientos_cp))
    df02Insert.printSchema()
    df_bajas_hist=df_bajas_hist_original.union(df02Insert)
    if df_bajas_hist.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_bajas_hist'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_create_hive_tmp(vTAltBajHist)))
            df_bajas_hist.repartition(1).write.mode('overwrite').saveAsTable(vTAltBajHist)
            df_bajas_hist.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTAltBajHist,str(df_bajas_hist.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTAltBajHist,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTAltBajHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [4.3]: Se inserta los transfer_in del mes de la tabla {} en la tabla {} '.format(vTTrInBI, vTTransfHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_select_query(vTTransfHist)))
    print(etq_sql(qry_ori_otc_t_transfer_hist(vTTransfHist)))
    df_ti_hist_original=spark.sql(qry_ori_otc_t_transfer_hist(vTTransfHist))
    df_ti_hist_original.printSchema()
    rows_to_delete=df_ti_hist_original.filter((F.upper(col('TIPO'))=='PRE_POS') & (col('FECHA').between(f_inicio, fecha_proceso)))
    df_ti_hist_original=df_ti_hist_original.join(rows_to_delete, on=['TIPO','FECHA'], how='left_anti')
    print(etq_info(msg_i_insert_hive(vTTransfHist)))
    print(etq_sql(qry_insrt_otc_t_transfer_hist_pre_pos(vTTrInBI, fecha_movimientos_cp)))
    df03Insert=spark.sql(qry_insrt_otc_t_transfer_hist_pre_pos(vTTrInBI, fecha_movimientos_cp))
    df03Insert.printSchema()
    df_ti_hist=df_ti_hist_original.union(df03Insert)
    if df_ti_hist.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_ti_hist'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_create_hive_tmp(vTTransfHist)))
            df_ti_hist.repartition(1).write.mode('overwrite').saveAsTable(vTTransfHist)
            df_ti_hist.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTTransfHist,str(df_ti_hist.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTTransfHist,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTTransfHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [4.4]: Se inserta los transfer_out del mes de la tabla {} en la tabla {} '.format(vTTrOutBI, vTTransfHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_select_query(vTTransfHist)))
    print(etq_sql(qry_ori_otc_t_transfer_hist(vTTransfHist)))
    df_to_hist_original=spark.sql(qry_ori_otc_t_transfer_hist(vTTransfHist))
    df_to_hist_original.printSchema()
    rows_to_delete=df_to_hist_original.filter((F.upper(col('TIPO'))=='POS_PRE') & (col('FECHA').between(f_inicio, fecha_proceso)))
    df_to_hist_original=df_to_hist_original.join(rows_to_delete, on=['TIPO','FECHA'], how='left_anti')
    print(etq_info(msg_i_insert_hive(vTTransfHist)))
    print(etq_sql(qry_insrt_otc_t_transfer_hist_pos_pre(vTTrOutBI, fecha_movimientos_cp)))
    df04Insert=spark.sql(qry_insrt_otc_t_transfer_hist_pos_pre(vTTrOutBI, fecha_movimientos_cp))
    df04Insert.printSchema()
    df_to_hist=df_to_hist_original.union(df04Insert)
    if df_to_hist.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_to_hist'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_create_hive_tmp(vTTransfHist)))
            df_to_hist.repartition(1).write.mode('overwrite').saveAsTable(vTTransfHist)
            df_to_hist.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTTransfHist,str(df_to_hist.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTTransfHist,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTTransfHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [4.5]: Se inserta los cambios de plan del mes de la tabla {} en la tabla {} '.format(vTCPBI, vTCPHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_select_query(vTCPHist)))
    print(etq_sql(qry_ori_otc_t_cambio_plan_hist(vTCPHist)))
    df_cp_hist_original=spark.sql(qry_ori_otc_t_cambio_plan_hist(vTCPHist))
    df_cp_hist_original.printSchema()
    rows_to_delete=df_cp_hist_original.filter(col('FECHA').between(f_inicio, fecha_proceso))
    df_cp_hist_original=df_cp_hist_original.join(rows_to_delete, on=['FECHA'], how='left_anti')
    print(etq_info(msg_i_insert_hive(vTCPHist)))
    print(etq_sql(qry_insrt_otc_t_cambio_plan_hist(vTCPBI, fecha_movimientos_cp)))
    df05Insert=spark.sql(qry_insrt_otc_t_cambio_plan_hist(vTCPBI, fecha_movimientos_cp))
    df05Insert.printSchema()
    df_cp_hist=df_cp_hist_original.union(df05Insert)
    if df_cp_hist.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_cp_hist'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_create_hive_tmp(vTCPHist)))
            df_cp_hist.repartition(1).write.mode('overwrite').saveAsTable(vTCPHist)
            df_cp_hist.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTCPHist,str(df_cp_hist.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTCPHist,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTCPHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [4.06]: Se obtiene el ultimo evento del alta en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_movimientos_parque_06(vSchRep)))))
    print(etq_sql(qry_otc_t_alta_hist_unic(vTAltBajHist, fecha_movimientos)))
    df06=spark.sql(qry_otc_t_alta_hist_unic(vTAltBajHist, fecha_movimientos))
    if df06.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df06'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_06(vSchRep)))))
            df06.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_movimientos_parque_06(vSchRep)))
            df06.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_movimientos_parque_06(vSchRep)),str(df06.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_movimientos_parque_06(vSchRep)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_06(vSchRep)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [4.07]: Se obtiene el ultimo evento de las bajas en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_movimientos_parque_07(vSchRep)))))
    print(etq_sql(qry_otc_t_baja_hist_unic(vTAltBajHist, fecha_movimientos)))
    df07=spark.sql(qry_otc_t_baja_hist_unic(vTAltBajHist, fecha_movimientos))
    if df07.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df07'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_07(vSchRep)))))
            df07.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_movimientos_parque_07(vSchRep)))
            df07.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_movimientos_parque_07(vSchRep)),str(df07.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_movimientos_parque_07(vSchRep)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_07(vSchRep)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [4.08]: Se obtiene el ultimo evento de las transferencias out en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_movimientos_parque_08(vSchRep)))))
    print(etq_sql(qry_otc_t_pos_pre_hist_unic(vTTransfHist, fecha_movimientos)))
    df08=spark.sql(qry_otc_t_pos_pre_hist_unic(vTTransfHist, fecha_movimientos))
    if df08.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df08'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_08(vSchRep)))))
            df08.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_movimientos_parque_08(vSchRep)))
            df08.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_movimientos_parque_08(vSchRep)),str(df08.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_movimientos_parque_08(vSchRep)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_08(vSchRep)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [4.09]: Se obtiene el ultimo evento de las transferencias in  en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_movimientos_parque_09(vSchRep)))))
    print(etq_sql(qry_otc_t_pre_pos_hist_unic(vTTransfHist, fecha_movimientos)))
    df09=spark.sql(qry_otc_t_pre_pos_hist_unic(vTTransfHist, fecha_movimientos))
    if df09.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df09'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_09(vSchRep)))))
            df09.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_movimientos_parque_09(vSchRep)))
            df09.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_movimientos_parque_09(vSchRep)),str(df09.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_movimientos_parque_09(vSchRep)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_09(vSchRep)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [4.10]: Se obtiene el ultimo evento de los cambios de plan en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_movimientos_parque_10(vSchRep)))))
    print(etq_sql(qry_otc_t_cambio_plan_hist_unic(vTCPHist, fecha_movimientos)))
    df10=spark.sql(qry_otc_t_cambio_plan_hist_unic(vTCPHist, fecha_movimientos))
    if df10.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df10'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_10(vSchRep)))))
            df10.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_movimientos_parque_10(vSchRep)))
            df10.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_movimientos_parque_10(vSchRep)),str(df10.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_movimientos_parque_10(vSchRep)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_10(vSchRep)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [4.11]: Se realiza el cruce con cada tabla usando {} (tabla resultante de pivot_parque) y agregando los campos de cada tabla renombrandolos de acuerdo al movimiento que corresponda'.format(vTPivotParq)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_movimientos_parque_11(vSchTmp)))))
    print(etq_sql(qry_otc_t_360_parque_1_tmp_t_mov(vTPivotParq, str(nme_tbl_otc_t_360_movimientos_parque_06(vSchRep)), str(nme_tbl_otc_t_360_movimientos_parque_08(vSchRep)), str(nme_tbl_otc_t_360_movimientos_parque_09(vSchRep)), str(nme_tbl_otc_t_360_movimientos_parque_10(vSchRep)))))
    df11=spark.sql(qry_otc_t_360_parque_1_tmp_t_mov(vTPivotParq, str(nme_tbl_otc_t_360_movimientos_parque_06(vSchRep)), str(nme_tbl_otc_t_360_movimientos_parque_08(vSchRep)), str(nme_tbl_otc_t_360_movimientos_parque_09(vSchRep)), str(nme_tbl_otc_t_360_movimientos_parque_10(vSchRep))))
    if df11.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df11'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_11(vSchTmp)))))
            df11.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_movimientos_parque_11(vSchTmp)))
            df11.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_movimientos_parque_11(vSchTmp)),str(df11.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_movimientos_parque_11(vSchTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_11(vSchTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [4.12]: Se crea la tabla temp union para obtener ultimo movimiento del mes por num_telefono'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_movimientos_parque_12(vSchTmp)))))
    print(etq_sql(qry_otc_t_360_parque_1_mov_mes_tmp(f_inicio, fecha_proceso, vTMP06, vTMP07, vTMP08, vTMP09, vTMP10)))
    df12=spark.sql(qry_otc_t_360_parque_1_mov_mes_tmp(f_inicio, fecha_proceso, vTMP06, vTMP07, vTMP08, vTMP09, vTMP10))
    if df12.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df12'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_12(vSchTmp)))))
            df12.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_movimientos_parque_12(vSchTmp)))
            df12.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_movimientos_parque_12(vSchTmp)),str(df12.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_movimientos_parque_12(vSchTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_12(vSchTmp)),str(e))))
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