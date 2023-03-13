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

# General cliente 360
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Configuraciones')
from otc_t_rtd_oferta_sugerida_config import *
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Querys')
from otc_t_rtd_oferta_sugerida_query import *

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
    parser.add_argument('--vTPivotante', required=True, type=str)
    parser.add_argument('--vSSchHiveMain', required=True, type=str)
    parser.add_argument('--vSTblHiveMain', required=True, type=str)
    parser.add_argument('--vIFechaProceso', required=True, type=int)
    parser.add_argument('--vIFechaEje_1', required=True, type=int)
    parser.add_argument('--vIFechaEje_2', required=True, type=int)
    parser.add_argument('--vIPtMes', required=True, type=int)
    parser.add_argument('--vSComboDefecto', required=True, type=str)
    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vSSchHiveTmp=parametros.vSSchHiveTmp
    vTPivotante=parametros.vTPivotante
    vSSchHiveMain=parametros.vSSchHiveMain
    vSTblHiveMain=parametros.vSTblHiveMain
    vIFechaProceso=parametros.vIFechaProceso
    vIFechaEje_1=parametros.vIFechaEje_1
    vIFechaEje_2=parametros.vIFechaEje_2
    vIPtMes=parametros.vIPtMes
    vSComboDefecto=parametros.vSComboDefecto
    print(etq_info(log_p_parametros("vSEntidad",vSEntidad)))
    print(etq_info(log_p_parametros("vSSchHiveTmp",vSSchHiveTmp)))
    print(etq_info(log_p_parametros("vTPivotante",vTPivotante)))
    print(etq_info(log_p_parametros("vSSchHiveMain",vSSchHiveMain)))
    print(etq_info(log_p_parametros("vSTblHiveMain",vSTblHiveMain)))
    print(etq_info(log_p_parametros("vIFechaProceso",vIFechaProceso)))
    print(etq_info(log_p_parametros("vIFechaEje_1",vIFechaEje_1)))
    print(etq_info(log_p_parametros("vIFechaEje_2",vIFechaEje_2)))
    print(etq_info(log_p_parametros("vIPtMes",vIPtMes)))
    print(etq_info(log_p_parametros("vSComboDefecto",vSComboDefecto)))
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
        .config("spark.yarn.queue", "default")\
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
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_rtd_01(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_rtd_02(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_rtd_03(vSSchHiveTmp)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_rtd_04(vSSchHiveTmp)))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_rtd_01(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_rtd_02(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_rtd_03(vSSchHiveTmp))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_rtd_04(vSSchHiveTmp)))) 
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4]: Generando logica de negocio '
print(etq_info(VStp))
print(lne_dvs())
VStp='Paso [4.1]: Extraer mines de la tabla pivontante {} '.format(vTPivotante)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_rtd_01(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_otc_t_rtd_new_pvt(vTPivotante,vIFechaEje_1,vIFechaEje_2)))
    df01=spark.sql(qry_tmp_otc_t_rtd_new_pvt(vTPivotante,vIFechaEje_1,vIFechaEje_2))
    if df01.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df01'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_rtd_01(vSSchHiveTmp)))))
            df01.write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_rtd_01(vSSchHiveTmp)))
            df01.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_rtd_01(vSSchHiveTmp)),str(df01.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_rtd_01(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_rtd_01(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.2]: Extraer mines de db_reportes.otc_t_rtd_categoria_ultimo_mes '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_rtd_02(vSSchHiveTmp)))))
    print(etq_sql(qyr_tmp_otc_t_rtd_categoria_ultimo_mes(vIPtMes)))
    df02=spark.sql(qyr_tmp_otc_t_rtd_categoria_ultimo_mes(vIPtMes))
    if df02.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df02'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_rtd_02(vSSchHiveTmp)))))
            df02.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_rtd_02(vSSchHiveTmp)))
            df02.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_rtd_02(vSSchHiveTmp)),str(df02.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_rtd_02(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_rtd_02(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.3]: Extraer mines de db_altamira.otc_t_saldos '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_rtd_03(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_saldos_diario(vIFechaEje_2)))
    df03=spark.sql(qry_tmp_saldos_diario(vIFechaEje_2))
    if df03.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df03'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_rtd_03(vSSchHiveTmp)))))
            df03.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_rtd_03(vSSchHiveTmp)))
            df03.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_rtd_03(vSSchHiveTmp)),str(df03.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_rtd_03(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_rtd_03(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.4]: Generando reglas para oferta sugerida '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_rtd_04(vSSchHiveTmp)))))
    print(etq_sql(qry_tmp_otc_rtd_oferta_sugerida_prv(str(nme_tbl_tmp_otc_t_360_rtd_01(vSSchHiveTmp)),str(nme_tbl_tmp_otc_t_360_rtd_02(vSSchHiveTmp)),str(nme_tbl_tmp_otc_t_360_rtd_03(vSSchHiveTmp)))))
    df04=spark.sql(qry_tmp_otc_rtd_oferta_sugerida_prv(str(nme_tbl_tmp_otc_t_360_rtd_01(vSSchHiveTmp)),str(nme_tbl_tmp_otc_t_360_rtd_02(vSSchHiveTmp)),str(nme_tbl_tmp_otc_t_360_rtd_03(vSSchHiveTmp))))
    if df04.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df04'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_rtd_04(vSSchHiveTmp)))))
            df04.write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_rtd_04(vSSchHiveTmp)))
            df04.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_rtd_04(vSSchHiveTmp)),str(df04.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_rtd_04(vSSchHiveTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_rtd_04(vSSchHiveTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
otc_t_rtd_oferta_sugerida=vSSchHiveMain+"."+vSTblHiveMain
VStp='Paso [5]: Insertar registros del negocio en la tabla {}'.format(otc_t_rtd_oferta_sugerida)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())    
    print(etq_sql(qry_tmp_otc_rtd_oferta_sugerida(vSComboDefecto,nme_tbl_tmp_otc_t_360_rtd_04(vSSchHiveTmp),vIFechaProceso)))
    df05 = spark.sql(qry_tmp_otc_rtd_oferta_sugerida(vSComboDefecto,nme_tbl_tmp_otc_t_360_rtd_04(vSSchHiveTmp),vIFechaProceso))
    if df05.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df05'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(otc_t_rtd_oferta_sugerida)))
            query_truncate = "ALTER TABLE "+otc_t_rtd_oferta_sugerida+" DROP IF EXISTS PARTITION (fecha_proceso = "+str(vIFechaProceso)+") purge"
            print(etq_info(query_truncate))
            hc=HiveContext(spark)
            hc.sql(query_truncate)
            df05.write.mode('append').insertInto(otc_t_rtd_oferta_sugerida)
            df05.printSchema()            
            print(etq_info(msg_t_total_registros_hive(otc_t_rtd_oferta_sugerida,str(df05.count())))) #BORRAR
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(otc_t_rtd_oferta_sugerida,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(otc_t_rtd_oferta_sugerida,str(e))))
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