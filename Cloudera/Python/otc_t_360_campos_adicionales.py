# -- coding: utf-8 --
import sys
reload(sys)
sys.setdefaultencoding('utf8')
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
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Querys')
from otc_t_360_campos_adicionales_query import *
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Configuraciones')
from otc_t_360_campos_adicionales_config import *
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
    parser.add_argument('--vFECHAEJE', required=True, type=str)
    parser.add_argument('--vfechamas1_2', required=True, type=str)
    parser.add_argument('--vfechamas1', required=True, type=str)


    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vTempSchema=parametros.vSSchHiveTmp
    FECHAEJE=parametros.vFECHAEJE
    fechamas1_2=parametros.vfechamas1_2
    fechamas1=parametros.vfechamas1

    print(etq_info(log_p_parametros("Entidad",vSEntidad)))
    print(etq_info(log_p_parametros("Esquema Temporal",vTempSchema)))
    print(etq_info(log_p_parametros("FECHAEJE",FECHAEJE)))
    print(etq_info(log_p_parametros("fechamas1_2",fechamas1_2)))
    print(etq_info(log_p_parametros("fechamas1",fechamas1)))


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

VStp='Paso [3]: Eliminando tablas temporales'

try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_01(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_02(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_03(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_04(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_05(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_06(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_07(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_08(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_09(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_10(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_11(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_12(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_13(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_14(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_15(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_16(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_17(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_18(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_19(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_20(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_21(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_22(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_23(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_24(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_25(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_campos_adicionales_26(vTempSchema)))

    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_01(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_02(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_03(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_04(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_05(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_06(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_07(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_08(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_09(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_10(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_11(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_12(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_13(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_14(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_15(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_16(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_17(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_18(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_19(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_20(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_21(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_22(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_23(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_24(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_25(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_campos_adicionales_26(vTempSchema))))

    te_step = datetime.now()

    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4]: Generando logica de negocio '
print(etq_info(VStp))
print(lne_dvs())

VStp='Paso [4_01]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_01(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_01(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_01(fechamas1_2)))
    df_01=spark.sql(qry_otc_t_360_campos_adicionales_01(fechamas1_2))
    if df_01.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_01'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_01(vTempSchema)))))
            df_01.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_01(vTempSchema)))
            df_01.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_01(vTempSchema)),str(df_01.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_01(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_01(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_02]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_02(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_02(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_02()))
    df_02=spark.sql(qry_otc_t_360_campos_adicionales_02())
    if df_02.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_02'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_02(vTempSchema)))))
            df_02.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_02(vTempSchema)))
            df_02.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_02(vTempSchema)),str(df_02.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_02(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_02(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_03]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_03(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_03(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_03()))
    df_03=spark.sql(qry_otc_t_360_campos_adicionales_03())
    if df_03.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_03'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_03(vTempSchema)))))
            df_03.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_03(vTempSchema)))
            df_03.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_03(vTempSchema)),str(df_03.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_03(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_03(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_04]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_04(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_04(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_04()))
    df_04=spark.sql(qry_otc_t_360_campos_adicionales_04())
    if df_04.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_04'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_04(vTempSchema)))))
            df_04.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_04(vTempSchema)))
            df_04.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_04(vTempSchema)),str(df_04.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_04(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_04(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())

VStp='Paso [4_05]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_05(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_05(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_05()))
    df_05=spark.sql(qry_otc_t_360_campos_adicionales_05())
    if df_05.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_05'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_05(vTempSchema)))))
            df_05.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_05(vTempSchema)))
            df_05.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_05(vTempSchema)),str(df_05.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_05(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_05(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_06]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_06(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_06(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_06(FECHAEJE)))
    df_06=spark.sql(qry_otc_t_360_campos_adicionales_06(FECHAEJE))
    if df_06.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_06'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_06(vTempSchema)))))
            df_06.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_06(vTempSchema)))
            df_06.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_06(vTempSchema)),str(df_06.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_06(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_06(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())

VStp='Paso [4_07]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_07(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_07(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_07()))
    df_07=spark.sql(qry_otc_t_360_campos_adicionales_07())
    if df_07.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_07'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_07(vTempSchema)))))
            df_07.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_07(vTempSchema)))
            df_07.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_07(vTempSchema)),str(df_07.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_07(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_07(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_08]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_08(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_08(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_08()))
    df_08=spark.sql(qry_otc_t_360_campos_adicionales_08())
    if df_08.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_08'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_08(vTempSchema)))))
            df_08.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_08(vTempSchema)))
            df_08.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_08(vTempSchema)),str(df_08.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_08(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_08(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_09]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_09(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_09(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_09()))
    df_09=spark.sql(qry_otc_t_360_campos_adicionales_09())
    if df_09.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_09'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_09(vTempSchema)))))
            df_09.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_09(vTempSchema)))
            df_09.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_09(vTempSchema)),str(df_09.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_09(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_09(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_10]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_10(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_10(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_10()))
    df_10=spark.sql(qry_otc_t_360_campos_adicionales_10())
    if df_10.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_10'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_10(vTempSchema)))))
            df_10.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_10(vTempSchema)))
            df_10.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_10(vTempSchema)),str(df_10.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_10(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_10(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_11]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_11(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_11(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_11(fechamas1_2)))
    df_11=spark.sql(qry_otc_t_360_campos_adicionales_11(fechamas1_2))
    if df_11.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_11'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_11(vTempSchema)))))
            df_11.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_11(vTempSchema)))
            df_11.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_11(vTempSchema)),str(df_11.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_11(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_11(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())

VStp='Paso [4_12]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_12(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_12(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_12()))
    df_12=spark.sql(qry_otc_t_360_campos_adicionales_12())
    if df_12.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_12'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_12(vTempSchema)))))
            df_12.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_12(vTempSchema)))
            df_12.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_12(vTempSchema)),str(df_12.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_12(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_12(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_13]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_13(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_13(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_13()))
    df_13=spark.sql(qry_otc_t_360_campos_adicionales_13())
    if df_13.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_13'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_13(vTempSchema)))))
            df_13.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_13(vTempSchema)))
            df_13.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_13(vTempSchema)),str(df_13.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_13(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_13(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_14]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_14(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_14(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_14()))
    df_14=spark.sql(qry_otc_t_360_campos_adicionales_14())
    if df_14.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_14'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_14(vTempSchema)))))
            df_14.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_14(vTempSchema)))
            df_14.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_14(vTempSchema)),str(df_14.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_14(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_14(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())

VStp='Paso [4_15]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_15(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_15(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_15()))
    df_15=spark.sql(qry_otc_t_360_campos_adicionales_15())
    if df_15.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_15'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_15(vTempSchema)))))
            df_15.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_15(vTempSchema)))
            df_15.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_15(vTempSchema)),str(df_15.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_15(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_15(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())

VStp='Paso [4_16]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_16(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_16(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_16()))
    df_16=spark.sql(qry_otc_t_360_campos_adicionales_16())
    if df_16.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_16'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_16(vTempSchema)))))
            df_16.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_16(vTempSchema)))
            df_16.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_16(vTempSchema)),str(df_16.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_16(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_16(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_17]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_17(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_17(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_17()))
    df_17=spark.sql(qry_otc_t_360_campos_adicionales_17())
    if df_17.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_17'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_17(vTempSchema)))))
            df_17.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_17(vTempSchema)))
            df_17.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_17(vTempSchema)),str(df_17.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_17(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_17(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_18]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_18(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_18(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_18(fechamas1_2)))
    df_18=spark.sql(qry_otc_t_360_campos_adicionales_18(fechamas1_2))
    if df_18.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_18'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_18(vTempSchema)))))
            df_18.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_18(vTempSchema)))
            df_18.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_18(vTempSchema)),str(df_18.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_18(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_18(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_19]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_19(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_19(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_19()))
    df_19=spark.sql(qry_otc_t_360_campos_adicionales_19())
    if df_19.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_19'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_19(vTempSchema)))))
            df_19.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_19(vTempSchema)))
            df_19.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_19(vTempSchema)),str(df_19.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_19(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_19(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())

VStp='Paso [4_20]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_20(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_20(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_20(FECHAEJE)))
    df_20=spark.sql(qry_otc_t_360_campos_adicionales_20(FECHAEJE))
    if df_20.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_20'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_20(vTempSchema)))))
            df_20.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_20(vTempSchema)))
            df_20.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_20(vTempSchema)),str(df_20.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_20(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_20(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())

VStp='Paso [4_21]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_21(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_21(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_21()))
    df_21=spark.sql(qry_otc_t_360_campos_adicionales_21())
    if df_21.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_21'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_21(vTempSchema)))))
            df_21.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_21(vTempSchema)))
            df_21.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_21(vTempSchema)),str(df_21.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_21(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_21(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_22]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_22(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_22(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_22()))
    df_22=spark.sql(qry_otc_t_360_campos_adicionales_22())
    if df_22.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_22'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_22(vTempSchema)))))
            df_22.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_22(vTempSchema)))
            df_22.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_22(vTempSchema)),str(df_22.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_22(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_22(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())

VStp='Paso [4_23]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_23(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_23(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_23()))
    df_23=spark.sql(qry_otc_t_360_campos_adicionales_23())
    if df_23.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_23'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_23(vTempSchema)))))
            df_23.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_23(vTempSchema)))
            df_23.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_23(vTempSchema)),str(df_23.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_23(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_23(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_24]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_24(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_24(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_24()))
    df_24=spark.sql(qry_otc_t_360_campos_adicionales_24())
    if df_24.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_24'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_24(vTempSchema)))))
            df_24.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_24(vTempSchema)))
            df_24.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_24(vTempSchema)),str(df_24.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_24(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_24(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())

VStp='Paso [4_25]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_25(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_25(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_25()))
    df_25=spark.sql(qry_otc_t_360_campos_adicionales_25())
    if df_25.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_25'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_25(vTempSchema)))))
            df_25.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_25(vTempSchema)))
            df_25.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_25(vTempSchema)),str(df_25.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_25(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_25(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4_26]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_campos_adicionales_26(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_campos_adicionales_26(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_campos_adicionales_26(fechamas1)))
    df_26=spark.sql(qry_otc_t_360_campos_adicionales_26(fechamas1))
    if df_26.rdd.isEmpty():
        print(etq_nodata(msg_e_df_nodata(str('df_26'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_26(vTempSchema)))))
            df_26.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_campos_adicionales_26(vTempSchema)))
            df_26.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_26(vTempSchema)),str(df_26.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_26(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_campos_adicionales_26(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())



vStpFin='Paso [Final]: Eliminando dataframes ..'
print(lne_dvs())

try:
    ts_step = datetime.now()
    del df_01
    del df_02
    del df_03
    del df_04
    del df_05
    del df_06
    del df_07
    del df_08
    del df_09
    del df_10
    del df_11
    del df_12
    del df_13
    del df_14
    del df_15
    del df_16
    del df_17
    del df_18
    del df_19
    del df_20
    del df_21
    del df_22
    del df_23
    del df_24
    del df_25
    del df_26
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStpFin,str(e))))

print(lne_dvs())

print('OK - PROCESO PYSPARK TERMINADO')

spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())