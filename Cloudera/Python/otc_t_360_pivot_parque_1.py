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
    parser.add_argument('--vTBajasBi', required=True, type=str)
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
    vTBajasBi=parametros.vTBajasBi
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
    print(etq_info(log_p_parametros("vTBajasBi",vTBajasBi)))
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
        .config("spark.yarn.queue", "capa_semantica") \
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

VStp='Paso [3]: Cargando configuracion y nombre de tablas:'

try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())  
    print(etq_info("Inicio del proceso en PySpark...")) 
    print(lne_dvs())
    print(etq_info("Importando librerias personalizadas..."))
    sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Configuraciones')
    from otc_t_360_pivot_parque_config import *
    sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Querys')
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
    vTPP08=str(nme_tbl_tmp_otc_t_360_pivot_parque_08(vSSchHiveTmp, vAbrev))
    vTPP09=str(nme_tbl_tmp_otc_t_360_pivot_parque_09(vSSchHiveTmp, vAbrev))
    vTPP10=str(nme_tbl_tmp_otc_t_360_pivot_parque_10(vSSchHiveTmp, vAbrev))
    vTPP11=str(nme_tbl_tmp_otc_t_360_pivot_parque_11(vSSchHiveTmp, vAbrev))
    vTPP12=str(nme_tbl_tmp_otc_t_360_pivot_parque_12(vSSchHiveTmp, vAbrev))
    vTPP13=str(nme_tbl_tmp_otc_t_360_pivot_parque_13(vSSchHiveTmp, vAbrev))
    vTPP14=str(nme_tbl_tmp_otc_t_360_pivot_parque_14(vSSchHiveTmp, vAbrev))
    vTPP15=str(nme_tbl_tmp_otc_t_360_pivot_parque_15(vSSchHiveTmp, vAbrev))
    vTPP16=str(nme_tbl_tmp_otc_t_360_pivot_parque_16(vSSchHiveTmp, vAbrev))
    vTPP17=str(nme_tbl_tmp_otc_t_360_pivot_parque_17(vSSchHiveTmp, vAbrev))
    vTPP18=str(nme_tbl_tmp_otc_t_360_pivot_parque_18(vSSchHiveTmp, vAbrev))
    vTPP19=str(nme_tbl_tmp_otc_t_360_pivot_parque_19(vSSchHiveTmp, vAbrev))
    print(etq_info(log_p_parametros('vTPP01', vTPP01)))
    print(etq_info(log_p_parametros('vTPP02', vTPP02)))
    print(etq_info(log_p_parametros('vTPP03', vTPP03)))
    print(etq_info(log_p_parametros('vTPP04', vTPP04)))
    print(etq_info(log_p_parametros('vTPP05', vTPP05)))
    print(etq_info(log_p_parametros('vTPP06', vTPP06)))
    print(etq_info(log_p_parametros('vTPP07', vTPP07)))
    print(etq_info(log_p_parametros('vTPP08', vTPP08)))
    print(etq_info(log_p_parametros('vTPP09', vTPP09)))
    print(etq_info(log_p_parametros('vTPP10', vTPP10)))
    print(etq_info(log_p_parametros('vTPP11', vTPP11)))
    print(etq_info(log_p_parametros('vTPP12', vTPP12)))
    print(etq_info(log_p_parametros('vTPP13', vTPP13)))
    print(etq_info(log_p_parametros('vTPP14', vTPP14)))
    print(etq_info(log_p_parametros('vTPP15', vTPP15)))
    print(etq_info(log_p_parametros('vTPP16', vTPP16)))
    print(etq_info(log_p_parametros('vTPP17', vTPP17)))
    print(etq_info(log_p_parametros('vTPP18', vTPP18)))
    print(etq_info(log_p_parametros('vTPP19', vTPP19)))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4]: Generando logica de negocio '
print(etq_info(VStp))
print(lne_dvs())
VStp='Paso [4.1]: Se obtienen las altas desde el inicio del mes hasta la fecha de proceso de la tabla {} '.format(vTAltasBi)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTPP01)))
    print(etq_sql(qyr_tmp_360_alta_tmp(vTAltasBi,fec_proc)))
    df01=spark.sql(qyr_tmp_360_alta_tmp(vTAltasBi,fec_proc))
    if df01.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df01'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP01)))
            df01.write.mode('overwrite').saveAsTable(vTPP01)
            df01.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP01,str(df01.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP01,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP01,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.2]: Se obtienen las transferencias pos a pre desde el inicio del mes hasta la fecha de proceso de la tabla {} '.format(vTTransferOutBi)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTPP02)))
    print(etq_sql(qyr_tmp_360_transfer_in_pp_tmp(vTTransferOutBi,fec_proc)))
    df02=spark.sql(qyr_tmp_360_transfer_in_pp_tmp(vTTransferOutBi,fec_proc))
    if df02.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df02'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP02)))
            df02.write.mode('overwrite').saveAsTable(vTPP02)
            df02.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP02,str(df02.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP02,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP02,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.3]: Se obtienen las transferencias pre a pos desde el inicio del mes hasta la fecha de proceso de la tabla {} '.format(vTTransferInBi)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTPP03)))
    print(etq_sql(qyr_tmp_360_transfer_in_pos_tmp(vTTransferInBi,fec_proc)))
    df03=spark.sql(qyr_tmp_360_transfer_in_pos_tmp(vTTransferInBi,fec_proc))
    if df03.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df03'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP03)))
            df03.write.mode('overwrite').saveAsTable(vTPP03)
            df03.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP03,str(df03.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP03,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP03,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.4]: Se obtienen los cambios de plan de tipo upsell de la tabla {} '.format(vTCPBi)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTPP04)))
    print(etq_sql(qyr_tmp_360_upsell_tmp(vTCPBi,fec_proc)))
    df04=spark.sql(qyr_tmp_360_upsell_tmp(vTCPBi,fec_proc))
    if df04.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df04'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP04)))
            df04.write.mode('overwrite').saveAsTable(vTPP04)
            df04.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP04,str(df04.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP04,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP04,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.5]: Se obtienen los cambios de plan de tipo downsell de la tabla {} '.format(vTCPBi)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTPP05)))
    print(etq_sql(qyr_tmp_360_downsell_tmp(vTCPBi,fec_proc)))
    df05=spark.sql(qyr_tmp_360_downsell_tmp(vTCPBi,fec_proc))
    if df05.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df05'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP05)))
            df05.write.mode('overwrite').saveAsTable(vTPP05)
            df05.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP05,str(df05.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP05,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP05,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.6]: Se obtienen los cambios de plan de tipo crossell de la tabla {} '.format(vTCPBi)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTPP06)))
    print(etq_sql(qyr_tmp_360_misma_tarifa_tmp(vTCPBi,fec_proc)))
    df06=spark.sql(qyr_tmp_360_misma_tarifa_tmp(vTCPBi,fec_proc))
    if df06.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df06'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP06)))
            df06.write.mode('overwrite').saveAsTable(vTPP06)
            df06.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP06,str(df06.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP06,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP06,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.7]: Se obtienen las bajas involuntarias, en el periodo del mes de la tabla {} '.format(vTBajasInv)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTPP07)))
    print(etq_sql(qyr_tmp_360_bajas_invo(vTBajasInv, fec_ini_mes, vIFechaProceso)))
    df07=spark.sql(qyr_tmp_360_bajas_invo(vTBajasInv, fec_ini_mes, vIFechaProceso))
    if df07.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df07'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP07)))
            df07.write.mode('overwrite').saveAsTable(vTPP07)
            df07.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP07,str(df07.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP07,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP07,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.8]: Se obtienen el parque prepago, de acuerdo a la minima fecha de churn menor a la fecha de ejecucion de la tabla {} '.format(vTChurnSP2)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTPP08)))
    print(etq_sql(qyr_tmp_360_otc_t_360_churn90_ori(vTChurnSP2, fec_menos_5, fec_mas_1)))
    df08=spark.sql(qyr_tmp_360_otc_t_360_churn90_ori(vTChurnSP2, fec_menos_5, fec_mas_1))
    if df08.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df08'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP08)))
            df08.write.mode('overwrite').saveAsTable(vTPP08)
            df08.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP08,str(df08.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP08,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP08,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.9]: Se obtiene por cuenta de facturacion en banco atado '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTPP09)))
    print(etq_sql(qyr_tmp_360_otc_t_temp_banco_cliente360_tmp(vTCFact, vTPRMANDATE, fechaeje1)))
    df09=spark.sql(qyr_tmp_360_otc_t_temp_banco_cliente360_tmp(vTCFact, vTPRMANDATE, fechaeje1))
    if df09.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df09'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP09)))
            df09.write.mode('overwrite').saveAsTable(vTPP09)
            df09.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP09,str(df09.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP09,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP09,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.10]: Se obtienen las bajas desde el inicio del mes hasta la fecha de proceso de la tabla {} '.format(vTBajasBi)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTPP10)))
    print(etq_sql(qyr_tmp_360_baja_tmp(vTBajasBi, fec_proc)))
    df10=spark.sql(qyr_tmp_360_baja_tmp(vTBajasBi, fec_proc))
    if df10.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df10'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP10)))
            df10.write.mode('overwrite').saveAsTable(vTPP10)
            df10.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP10,str(df10.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP10,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP10,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.11]: Se unen los telefonos del parque inactivo '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTPP11)))
    print(etq_sql(qyr_tmp_360_parque_inactivo(vTPP10, vTPP02, vTPP03)))
    df11=spark.sql(qyr_tmp_360_parque_inactivo(vTPP10, vTPP02, vTPP03))
    if df11.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df11'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP11)))
            df11.write.mode('overwrite').saveAsTable(vTPP11)
            df11.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP11,str(df10.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP11,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP11,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.12]: Se obtienen datos de la tabla {} '.format(vTChurnSP2)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTPP12)))
    print(etq_sql(qyr_tmp_360_otc_t_360_churn90_tmp1(vTChurnSP2, fec_inac_1)))
    df12=spark.sql(qyr_tmp_360_otc_t_360_churn90_tmp1(vTChurnSP2, fec_inac_1))
    if df12.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df12'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTPP12)))
            df12.write.mode('overwrite').saveAsTable(vTPP12)
            df12.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTPP12,str(df12.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTPP12,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTPP12,str(e))))
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
    
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStpFin,str(e))))

print(lne_dvs())

spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())

